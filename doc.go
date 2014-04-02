package consistentHash

/*
Package consistentHash provides a consistent hashing implementation using murmur3 with a 64bit ring space.
Virtual nodes are used to provide a good distribution.

This package has an almost identical API to StatHat's consistent package at https://github.com/stathat/consistent,
although that packages uses a crc32 hash and a smaller number of vnodes by default.

See:
 http://en.wikipedia.org/wiki/Consistent_hashing
 http://en.wikipedia.org/wiki/MurmurHash


The only time an error will be returned from a Get(), Get2(), or GetN() call is if there are not enough members added

Basic Example:
  ch := consistentHash.New()
  ch.Add("server1")
  ch.Add("server2")
  ch.Add("server3")
  for _,key := range []string{"A","B","C","D","E","F","G"} {
	   server,err := ch.Get([]byte(key)
	   if err != nil {
		  panic(err)
	   }
	   fmt.Println("key=%s server=%s\n",key,server)
  }
  Outputs:
	key=A server=server3
	key=B server=server3
	key=C server=server1
	key=D server=server3
	key=E server=server2
	key=F server=server2
	key=G server=server1

  Example with 3 servers and then removing a member:
    ch := consistentHash.New()
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
	Output:
	 Output:
	3 servers
	key=A server=server3
	key=B server=server3
	key=C server=server1
	key=D server=server3
	key=E server=server2
	key=F server=server2
	key=G server=server1
	Removing server3
	key=A server=server1  // remapped from 3->1
	key=B server=server2  // remapped from 3->2
	key=C server=server1  // stayed in same location
	key=D server=server1  // remapped from 3->1
	key=E server=server2  // stayed in same location
	key=F server=server2  // stayed in same location
	key=G server=server1  // stayed in same location

*/
