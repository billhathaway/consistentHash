// Package consistentHash implements a consistent hashing algorithm
package consistentHash

import (
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"sort"
	"strconv"
	"sync"
)

var (
	// ErrNoMembers occurs when trying to hash before any members are added
	ErrNoMembers = errors.New("no members added")
	// ErrNotEnoughMembers occurs when more members are asked for than are available
	ErrNotEnoughMembers = errors.New("not enough members")
	// ErrNotAvailableOnceMembersAdded occurs if any attempt is made to modify the vnode account once members are added
	ErrNotAvailableOnceMembersAdded = errors.New("not available once members are added")
	// ErrInvalidVnodeCount occurs if the vnode count is set to 0 or lower
	ErrInvalidVnodeCount = errors.New("vnodeCount must be > 0")
)

const (
	// DefaultVnodeCount is a tradeoff of memory and ~ log(N) speed versus how well the hash spreads
	DefaultVnodeCount = 200
)

type vnode struct {
	token   uint64
	address string
}

type vnodes []vnode

// ConsistentHash holds the internal data structures for the hashing
type ConsistentHash struct {
	vnodes     vnodes
	nodes      map[string]bool
	vnodeCount int
	mutex      sync.Mutex
}

// New creates a new consistentHash pointer and initializes all the necessary fields
func New() *ConsistentHash {
	ch := new(ConsistentHash)
	ch.nodes = make(map[string]bool)
	ch.vnodes = make(vnodes, 0)
	ch.vnodeCount = DefaultVnodeCount
	return ch
}

// dumpVnodes prints the vnode slice to stdout, only useful for debugging
func (ch *ConsistentHash) dumpVnodes() {
	for _, vn := range ch.vnodes {
		fmt.Printf("%v\n", vn)
	}
}

// addressToKey converts an address and an integer to a []byte that we are sure won't be duplicated with a later valid IP
// or hostname
func addressToKey(address string, increment int) []byte {
	return []byte(strconv.Itoa(increment) + "=" + address)
}

// SetVnodeCount sets the number of vnodes that will be added for every server
// This must be called before any Add() calls
func (ch *ConsistentHash) SetVnodeCount(count int) error {
	if len(ch.nodes) > 0 {
		return ErrNotAvailableOnceMembersAdded
	}
	if count < 1 {
		return ErrInvalidVnodeCount
	}
	ch.vnodeCount = count
	return nil
}

// Add adds a server to the consistentHash
func (ch *ConsistentHash) Add(address string) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	// if the address has already been added, there is no work to do
	if _, found := ch.nodes[address]; found {
		return
	}
	ch.nodes[address] = true
	for i := 0; i < ch.vnodeCount; i++ {
		token := murmur3.Sum64(addressToKey(address, i))
		newVnode := vnode{token, address}
		ch.insertVnode(newVnode)
	}
}

// Remove removes a server from the consistentHash
func (ch *ConsistentHash) Remove(address string) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	if _, found := ch.nodes[address]; !found {
		return
	}
	for i := 0; i < ch.vnodeCount; i++ {
		token := murmur3.Sum64(addressToKey(address, i))
		ch.removeVnode(token)
	}
	delete(ch.nodes, address)
}

func (v *vnode) String() string {
	return fmt.Sprintf("token=%d address=%s", v.token, v.address)
}

// Get finds the closest member for a given key
func (ch *ConsistentHash) Get(key []byte) (string, error) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	if len(ch.vnodes) == 0 {
		return "", ErrNoMembers
	}
	token := murmur3.Sum64(key)
	return ch.vnodes[ch.closest(token)].address, nil
}

// Get2 finds the closest 2 members for a given key and is just a helper function
// calling into GetN
func (ch *ConsistentHash) Get2(key []byte) (string, string, error) {
	// don't use the mutex since GetN will use it
	servers, err := ch.GetN(key, 2)
	if err != nil {
		return "", "", err
	}
	return servers[0], servers[1], nil

}

// GetN finds the closest N members for a given key
func (ch *ConsistentHash) GetN(key []byte, count int) ([]string, error) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	if len(ch.nodes) < count {
		return nil, ErrNotEnoughMembers
	}
	token := murmur3.Sum64(key)
	addressMap := make(map[string]bool)
	addresses := make([]string, count)
	index := ch.closest(token)
	found := 0
	for found < count {
		if exists := addressMap[ch.vnodes[index].address]; !exists {
			addressMap[ch.vnodes[index].address] = true
			addresses[found] = ch.vnodes[index].address
			found++
		}
		index++
		if index == len(ch.vnodes) {
			index = 0
		}
	}
	return addresses, nil

}

// removeVnode removes a vnode from the ring
func (ch *ConsistentHash) removeVnode(token uint64) {
	index := ch.index(token)
	if index == len(ch.vnodes) {
		ch.vnodes = ch.vnodes[:index-1]
		return
	}
	ch.vnodes = append(ch.vnodes[:index], ch.vnodes[index+1:]...)
}

// insertVnode adds a vnode into the appropriate location of the ring
func (ch *ConsistentHash) insertVnode(vn vnode) {
	index := ch.index(vn.token)
	ch.vnodes = append(ch.vnodes[:index], append(vnodes{vn}, ch.vnodes[index:]...)...)
}

// index returns the position where we should insert a new vnode
// differs from closest in that if the new token is bigger than the current highest token
// the index returned should be the end
func (ch *ConsistentHash) index(token uint64) int {
	index := sort.Search(len(ch.vnodes), func(i int) bool {
		return ch.vnodes[i].token >= token
	})
	return index
}

// closest returns the index of the vnode greater than or equal to the token
func (ch *ConsistentHash) closest(token uint64) int {
	index := sort.Search(len(ch.vnodes), func(i int) bool {
		return ch.vnodes[i].token >= token
	})
	if index == len(ch.vnodes) {
		index = 0
	}
	return index
}
