// Package network implements interface, route and listening-socket discovery.
package network

import (
	"context"
	"net"
	"os"
	"path/filepath"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// Link is the runtime-neutral view of one network interface.
type Link struct {
	Index        int
	Name         string
	MTU          int
	Flags        net.Flags
	OperState    string
	HardwareAddr string
	Type         string
	MasterIndex  int
	ParentIndex  int
	Statistics   *LinkStats
}

// LinkStats mirrors IFLA_STATS64.
type LinkStats struct {
	RxBytes   uint64
	TxBytes   uint64
	RxPackets uint64
	TxPackets uint64
	RxErrors  uint64
	TxErrors  uint64
	RxDropped uint64
	TxDropped uint64
}

// Addr is one interface address.
type Addr struct {
	IP        net.IP
	PrefixLen int
	Scope     int
}

// Route is one unicast route.
type Route struct {
	Family          int
	Dst             *net.IPNet
	Gw              net.IP
	LinkIndex       int
	Table           int
	Metric          int
	Protocol        int
	Scope           int
	PreferredSource net.IP
	Type            int
}

// backend abstracts netlink so the module can be tested without a host.
type backend interface {
	Links() ([]Link, error)
	Addrs(linkIndex int) ([]Addr, error)
	Routes(family int) ([]Route, error)
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}

// newBackend is replaced by tests. It selects the host network namespace
// when the agent itself runs in a container with the host /proc mounted.
var newBackend = func(procfsRoot string) backend {
	hostNS, _ := os.Readlink(filepath.Join(procfsRoot, "1", "ns", "net"))
	selfNS, _ := os.Readlink(filepath.Join(procfsRoot, "self", "ns", "net"))
	if hostNS != "" && selfNS != "" && hostNS != selfNS {
		return netlinkBackend{nsPath: filepath.Join(procfsRoot, "1", "ns", "net")}
	}
	return netlinkBackend{}
}

type netlinkBackend struct {
	nsPath string
}

// open returns a handle bound to the host namespace, or nil for the current
// namespace. The caller must call close when done.
func (b netlinkBackend) open() (*netlink.Handle, func(), error) {
	if b.nsPath == "" {
		return nil, func() {}, nil
	}
	ns, err := netns.GetFromPath(b.nsPath)
	if err != nil {
		return nil, nil, err
	}
	h, err := netlink.NewHandleAt(ns)
	ns.Close()
	if err != nil {
		return nil, nil, err
	}
	return h, h.Close, nil
}

func (b netlinkBackend) Links() ([]Link, error) {
	h, closeFn, err := b.open()
	if err != nil {
		return nil, err
	}
	defer closeFn()
	var native []netlink.Link
	if h != nil {
		native, err = h.LinkList()
	} else {
		native, err = netlink.LinkList()
	}
	if err != nil {
		return nil, err
	}
	out := make([]Link, 0, len(native))
	for _, l := range native {
		a := l.Attrs()
		link := Link{
			Index:       a.Index,
			Name:        a.Name,
			MTU:         a.MTU,
			Flags:       a.Flags,
			OperState:   a.OperState.String(),
			Type:        l.Type(),
			MasterIndex: a.MasterIndex,
			ParentIndex: a.ParentIndex,
		}
		if len(a.HardwareAddr) > 0 {
			link.HardwareAddr = a.HardwareAddr.String()
		}
		if a.Statistics != nil {
			link.Statistics = &LinkStats{
				RxBytes:   a.Statistics.RxBytes,
				TxBytes:   a.Statistics.TxBytes,
				RxPackets: a.Statistics.RxPackets,
				TxPackets: a.Statistics.TxPackets,
				RxErrors:  a.Statistics.RxErrors,
				TxErrors:  a.Statistics.TxErrors,
				RxDropped: a.Statistics.RxDropped,
				TxDropped: a.Statistics.TxDropped,
			}
		}
		out = append(out, link)
	}
	return out, nil
}

func (b netlinkBackend) Addrs(linkIndex int) ([]Addr, error) {
	h, closeFn, err := b.open()
	if err != nil {
		return nil, err
	}
	defer closeFn()
	var link netlink.Link
	if h != nil {
		link, err = h.LinkByIndex(linkIndex)
	} else {
		link, err = netlink.LinkByIndex(linkIndex)
	}
	if err != nil {
		return nil, err
	}
	var native []netlink.Addr
	if h != nil {
		native, err = h.AddrList(link, netlink.FAMILY_ALL)
	} else {
		native, err = netlink.AddrList(link, netlink.FAMILY_ALL)
	}
	if err != nil {
		return nil, err
	}
	out := make([]Addr, 0, len(native))
	for _, a := range native {
		if a.IPNet == nil {
			continue
		}
		ones, _ := a.IPNet.Mask.Size()
		out = append(out, Addr{IP: a.IPNet.IP, PrefixLen: ones, Scope: a.Scope})
	}
	return out, nil
}

func (b netlinkBackend) Routes(family int) ([]Route, error) {
	h, closeFn, err := b.open()
	if err != nil {
		return nil, err
	}
	defer closeFn()
	var native []netlink.Route
	if h != nil {
		native, err = h.RouteListFiltered(family, &netlink.Route{Table: unix.RT_TABLE_MAIN}, netlink.RT_FILTER_TABLE)
	} else {
		native, err = netlink.RouteListFiltered(family, &netlink.Route{Table: unix.RT_TABLE_MAIN}, netlink.RT_FILTER_TABLE)
	}
	if err != nil {
		return nil, err
	}
	out := make([]Route, 0, len(native))
	for _, r := range native {
		out = append(out, Route{
			Family:          family,
			Dst:             r.Dst,
			Gw:              r.Gw,
			LinkIndex:       r.LinkIndex,
			Table:           r.Table,
			Metric:          r.Priority,
			Protocol:        int(r.Protocol),
			Scope:           int(r.Scope),
			PreferredSource: r.Src,
			Type:            r.Type,
		})
	}
	return out, nil
}

func (b netlinkBackend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	updates := make(chan struct{}, 1)
	linkCh := make(chan netlink.LinkUpdate)
	addrCh := make(chan netlink.AddrUpdate)
	routeCh := make(chan netlink.RouteUpdate)
	done := make(chan struct{})
	var ns netns.NsHandle
	subscribeLink := func() error { return netlink.LinkSubscribe(linkCh, done) }
	subscribeAddr := func() error { return netlink.AddrSubscribe(addrCh, done) }
	subscribeRoute := func() error { return netlink.RouteSubscribe(routeCh, done) }
	if b.nsPath != "" {
		var err error
		ns, err = netns.GetFromPath(b.nsPath)
		if err != nil {
			return nil, err
		}
		subscribeLink = func() error { return netlink.LinkSubscribeAt(ns, linkCh, done) }
		subscribeAddr = func() error { return netlink.AddrSubscribeAt(ns, addrCh, done) }
		subscribeRoute = func() error { return netlink.RouteSubscribeAt(ns, routeCh, done) }
	}
	if err := subscribeLink(); err != nil {
		if ns != 0 {
			ns.Close()
		}
		return nil, err
	}
	if err := subscribeAddr(); err != nil {
		close(done)
		if ns != 0 {
			ns.Close()
		}
		return nil, err
	}
	if err := subscribeRoute(); err != nil {
		close(done)
		if ns != 0 {
			ns.Close()
		}
		return nil, err
	}
	go func() {
		defer close(updates)
		if ns != 0 {
			defer ns.Close()
		}
		for {
			select {
			case <-ctx.Done():
				close(done)
				return
			case <-linkCh:
				notify(updates)
			case <-addrCh:
				notify(updates)
			case <-routeCh:
				notify(updates)
			}
		}
	}()
	return updates, nil
}

func notify(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
