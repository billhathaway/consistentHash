// test_consistentHash
package consistentHash

import (
	"crypto/rand"
	"fmt"
	"github.com/GaryBoone/GoStats/stats"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

var (
	keys [][]byte
)

func init() {
	keys = make([][]byte, 10000)
	for i := 0; i < len(keys); i++ {
		keys[i] = rand_bytes(10)
	}

}

func rand_bytes(size int) []byte {
	var bytes = make([]byte, size)
	rand.Read(bytes)
	return bytes
}

// Test_vnodeAdd verifies that the correct number of vnodes are added after an Add() call
func Test_vnodeAdd(t *testing.T) {
	c := New()
	c.Add("localhost")
	assert.Equal(t, c.vnodeCount, len(c.vnodes))

}

// Test_Distribution tests how well keys are distributed across servers
// and how many keys are remapped after a node is removed
// This is informational, not a pass/fail test
func Test_Distribution(t *testing.T) {
	c := New()
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	distribution := make(map[string]int)
	keymapping := make(map[string]string)
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		server, _ := c.Get(key)
		keymapping[string(key)] = server
		distribution[server]++
	}
	stat := stats.Stats{}
	for key, count := range distribution {
		stat.Update(float64(count))
		delete(distribution, key)
	}
	fmt.Printf("Stddev for %d keys mapped across %d servers = %.2f\n", len(keys), serverCount, stat.PopulationStandardDeviation())

	c.Remove("server" + strconv.Itoa(serverCount/2))
	stat = stats.Stats{}
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		server, _ := c.Get(key)
		if keymapping[string(key)] == server {
			delete(keymapping, string(key))
		}
		distribution[server]++
	}
	for key, count := range distribution {
		stat.Update(float64(count))
		delete(distribution, key)
	}
	fmt.Printf("Stddev for %d keys mapped across %d servers after one server removed = %.2f\n", len(keys), serverCount, stat.PopulationStandardDeviation())
	fmt.Printf("Number of keys out of %d remapped after removing 1 out of %d servers = %d\n", len(keys), serverCount, len(keymapping))
}

// Benchmark_DefaultLookup tests how fast lookups are if each node has the default number of vnodes
func Benchmark_DefaultLookup(b *testing.B) {
	c := New()
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i%len(keys)])
	}
}

// Benchmark_SingleVnodeLookup tests how fast lookups are if each node has 1 vnode
// Note that this would have very poor distribution
func Benchmark_SingleVnodeLookup(b *testing.B) {
	c := New()
	c.SetVnodeCount(1)
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i%len(keys)])
	}
}

// Benchmark_1000VnodeLookup tests how fast lookups are if each node has 1000 vnodes
func Benchmark_1000VnodeLookup(b *testing.B) {
	c := New()
	c.SetVnodeCount(1000)
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i%len(keys)])
	}
}

// Test_insertVnode verifies that vnodes are correctly inserted in the proper order
func Test_insertVnode(t *testing.T) {
	ch := New()
	v1 := vnode{100, "a"}
	v2 := vnode{50, "b"}
	v3 := vnode{1001, "c"}
	v4 := vnode{1000, "d"}
	ch.insertVnode(v1)
	ch.insertVnode(v2)
	ch.insertVnode(v3)
	ch.insertVnode(v4)
	assert.Equal(t, 4, len(ch.vnodes))
	assert.Equal(t, v2, ch.vnodes[0])
	assert.Equal(t, v1, ch.vnodes[1])
	assert.Equal(t, v3, ch.vnodes[3])
	assert.Equal(t, v4, ch.vnodes[2])

}

func Test_Get2(t *testing.T) {
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	server1, server2, err := ch.Get2([]byte("testKey"))
	assert.Nil(t, err)
	assert.True(t, (server1 == "server1" && server2 == "server2") || (server1 == "server2" && server2 == "server1"))
}

func Test_GetN(t *testing.T) {
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	_, err := ch.GetN([]byte("testKey"), 3)
	assert.Equal(t, err, notEnoughMembersError)
	ch.Add("server3")
	servers, err := ch.GetN([]byte("testKey"), 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(servers))
}

// Test_removeVnode verifies that vnodes are correctly removed
func Test_removeVnode(t *testing.T) {
	ch := New()
	v1 := vnode{100, "a"}
	v2 := vnode{50, "b"}
	v3 := vnode{1001, "c"}
	v4 := vnode{1000, "d"}
	ch.insertVnode(v1)
	ch.insertVnode(v2)
	ch.insertVnode(v3)
	ch.insertVnode(v4)
	ch.removeVnode(50)
	assert.Equal(t, 3, len(ch.vnodes))
	ch.removeVnode(1001)
	ch.removeVnode(100)
	ch.removeVnode(1000)
	assert.Empty(t, ch.vnodes)

}

func Example_basic() {
	// Output: key=A server=server3
	//key=B server=server3
	//key=C server=server1
	//key=D server=server3
	//key=E server=server2
	//key=F server=server2
	//key=G server=server1
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	ch.Add("server3")
	keys := []string{"A", "B", "C", "D", "E", "F", "G"}
	for _, key := range keys {
		server, err := ch.Get([]byte(key))
		if err != nil {
			panic(err)
		}
		fmt.Printf("key=%s server=%s\n", key, server)
	}
}

func Example_remove() {
	//Output: 3 servers
	//key=A server=server3
	//key=B server=server3
	//key=C server=server1
	//key=D server=server3
	//key=E server=server2
	//key=F server=server2
	//key=G server=server1
	//Removing server3
	//key=A server=server1
	//key=B server=server2
	//key=C server=server1
	//key=D server=server1
	//key=E server=server2
	//key=F server=server2
	//key=G server=server1
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	ch.Add("server3")
	keys := []string{"A", "B", "C", "D", "E", "F", "G"}
	fmt.Println("3 servers")
	for _, key := range keys {
		server, _ := ch.Get([]byte(key))
		fmt.Printf("key=%s server=%s\n", key, server)
	}
	fmt.Println("Removing server3")
	ch.Remove("server3")
	for _, key := range keys {
		server, _ := ch.Get([]byte(key))
		fmt.Printf("key=%s server=%s\n", key, server)
	}
}
