package servicediscovery

import (
	"testing"
)

func TestServiceDiscovery(t *testing.T) {
	// 服务器地址列表
	servers := []string{"192.168.0.101:2118", "192.168.0.102:2118", "192.168.0.103:2118"}
	client, err := NewDSClient(servers, "/api", 10)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if topic, err := client.Register("service1", ""); err == nil {
		print(topic)
	}
	if topic, err := client.Register("service1", ""); err == nil {
		print(topic)
	}
	if topic, err := client.Register("service1", ""); err == nil {
		print(topic)
	}
}
