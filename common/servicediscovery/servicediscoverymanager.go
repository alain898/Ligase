package servicediscovery

import (
	"github.com/finogeeks/ligase/common/config"
	"github.com/finogeeks/ligase/skunkworks/log"
	"sync"
)

const (
	RoleRegister = "register"
	RoleWatcher  = "watcher"
)

type SDManager struct {
	Locker            sync.RWMutex
	RegisterEndpoints sync.Map // concurrent map<service string, topic string>
	SDClient          *SDClient
	Role              string
	WatcherEndpoints  sync.Map // concurrent map<service string, list<topic string>>
}

func NewSDManager() *SDManager {
	return &SDManager{SDClient: nil}
}

func (sdm *SDManager) SetRole(role string) {
	if role != RoleRegister && role != RoleWatcher {
		log.Panicf("invalid role")
	}
	sdm.Role = role
}

func (sdm *SDManager) PrepareSDClient(cfg *config.Dendrite) *SDClient {
	if sdm.SDClient != nil {
		return sdm.SDClient
	}
	sdm.Locker.Lock()
	defer sdm.Locker.Unlock()
	if sdm.SDClient != nil {
		return sdm.SDClient
	}
	sdClient, err := NewDSClient(cfg.ServiceDiscovery.ZkServers,
		cfg.ServiceDiscovery.ZkRoot, cfg.ServiceDiscovery.TimeoutSeconds, nil)
	if err != nil {
		log.Panicf("failed to NewDSClient, ZkServers[%s], ZkRoot[%s], err:%v",
			cfg.ServiceDiscovery.ZkServers, cfg.ServiceDiscovery.ZkRoot, err)
	}
	sdm.SDClient = sdClient
	log.Infof("succeed to NewDSClient, ZkServers[%s], ZkRoot[%s]",
		cfg.ServiceDiscovery.ZkServers, cfg.ServiceDiscovery.ZkRoot)
	return sdm.SDClient
}

var SDM = NewSDManager()
