package servicediscovery

import (
	"fmt"
	"testing"
)

func TestServiceDiscovery(t *testing.T) {
	// 服务器地址列表
	servers := []string{"192.168.0.101:2118", "192.168.0.102:2118", "192.168.0.103:2118"}
	client, err := NewZKClient(servers, "/api", 10)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	node1 := &ServiceNode{"user", "127.0.0.1", 4000}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002}
	if err := client.Register(node1); err != nil {
		panic(err)
	}
	if err := client.Register(node2); err != nil {
		panic(err)
	}
	if err := client.Register(node3); err != nil {
		panic(err)
	}
	nodes, err := client.GetNodes("user")
	if err != nil {
		panic(err)
	}
	for _, node := range nodes {
		fmt.Println(node.Host, node.Port)
	}
}
