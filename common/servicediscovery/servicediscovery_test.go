package servicediscovery

import (
	"fmt"
	"github.com/finogeeks/ligase/skunkworks/log"
	"testing"
	"time"
)

func TestServiceDiscovery(t *testing.T) {
	servers := []string{"127.0.0.1:2181"}
	client, err := NewDSClient(servers, "/servicediscovery", 10, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	err = client.Watch("service1", func(endpoints []*Endpoint) {
		log.Infof("change watched for service1 [%+v]", endpoints)
	})
	if err != nil {
		log.Errorf("err[%+v]", err)
	}

	if topic, err := client.Register("service1", ""); err == nil {
		log.Infof(fmt.Sprintf("topic[%s]", topic))
	}
	if topic, err := client.Register("service1", ""); err == nil {
		log.Infof(fmt.Sprintf("topic[%s]", topic))
	}
	if topic, err := client.Register("service1", ""); err == nil {
		log.Infof(fmt.Sprintf("topic[%s]", topic))
	}

	for {
		time.Sleep(time.Second)
		log.Infof("tick")
	}

}
