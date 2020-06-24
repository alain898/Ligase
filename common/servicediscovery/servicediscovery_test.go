package servicediscovery

import (
	"fmt"
	"github.com/finogeeks/ligase/skunkworks/log"
	"sync"
	"testing"
	"time"
)

type LocalLocker struct {
	locker sync.RWMutex
}

func (wl *LocalLocker) Lock() {
	wl.locker.Lock()
}

func (wl *LocalLocker) UnLock() {
	wl.locker.Unlock()
}

func NewLocalLocker() Locker {
	return new(LocalLocker)
}

var lock = NewLocalLocker()

func doTest() {
	servers := []string{"127.0.0.1:2181"}
	client, err := NewDSClient(servers, "/servicediscovery", 10, lock)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	if topic, err := client.Register("service1", ""); err == nil {
		log.Infof(fmt.Sprintf("topic[%s]", topic))
	}

	for {
		time.Sleep(time.Second)
		log.Infof("tick")
	}
}

func TestServiceDiscovery(t *testing.T) {
	servers := []string{"127.0.0.1:2181"}
	client, err := NewDSClient(servers, "/servicediscovery", 10, lock)
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

	instances := 10
	for i := 0; i < instances; i++ {
		go doTest()
	}

	for {
		time.Sleep(time.Second)
		log.Infof("tick")
	}

}
