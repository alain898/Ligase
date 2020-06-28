package servicediscovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/finogeeks/ligase/skunkworks/log"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	splitSign = "___"
)

type Locker interface {
	Lock() error
	UnLock() error
}

// SDClient is not concurrency safe, so please use a extra distribute lock to ensure concurrency safe when it's necessary.
type SDClient struct {
	zkServers []string // zookeeper servers
	zkRoot    string   // root path for store the meta data for service discovery
	conn      *zk.Conn // zookeeper client connect
	locker    wrapLocker
}

type wrapLocker struct {
	locker Locker
}

func (wl *wrapLocker) Lock() error {
	if wl.locker != nil {
		return wl.locker.Lock()
	}
	return nil
}

func (wl *wrapLocker) UnLock() error {
	if wl.locker != nil {
		return wl.locker.UnLock()
	}
	return nil
}

type zkLocker struct {
	locker *zk.Lock
}

func (zl *zkLocker) Lock() error {
	return zl.locker.Lock()
}

func (zl *zkLocker) UnLock() error {
	return zl.locker.Unlock()
}

func NewDSClient(zkServers []string, zkRoot string, timeoutSeconds int64, locker Locker) (*SDClient, error) {
	client := new(SDClient)
	client.zkServers = zkServers
	client.zkRoot = zkRoot
	conn, _, err := zk.Connect(zkServers, time.Duration(timeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	client.conn = conn
	if err := client.createRootIfNotExist(); err != nil {
		client.Close()
		return nil, err
	}
	if locker != nil {
		client.locker = wrapLocker{locker: locker}
	} else {
		zl := &zkLocker{locker: zk.NewLock(conn, fmt.Sprintf("%s___lock", zkRoot), zk.WorldACL(zk.PermAll))}
		client.locker = wrapLocker{locker: zl}
	}
	return client, nil
}

func (s *SDClient) Close() {
	s.conn.Close()
}

func (s *SDClient) createRootIfNotExist() error {
	exists, _, err := s.conn.Exists(s.zkRoot)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Create(s.zkRoot, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func (s *SDClient) createServiceIfNotExist(service string) error {
	path := s.zkRoot + "/" + service
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

type Endpoint struct {
	Service string `json:"service"`
	Topic   string `json:"topic"`
}

func (s *SDClient) contains(list []int64, e int64) bool {
	for _, a := range list {
		if a == e {
			return true
		}
	}
	return false
}

func (s *SDClient) getEndpointIndex(endpoints []string) ([]int64, error) {
	if endpoints == nil {
		return nil, errors.New("endpoints is nil")
	}
	var indexList []int64
	for _, ep := range endpoints {
		splits := strings.Split(ep, splitSign)
		if len(splits) != 2 {
			err := fmt.Sprintf("invalid topic[%s]", ep)
			log.Errorf(err)
			return nil, errors.New(err)
		}
		index, err := strconv.ParseInt(splits[1], 10, 0)
		if err != nil {
			log.Errorf("failed to parse index[%s]", splits[1])
			return nil, err
		}
		indexList = append(indexList, index)
	}
	sort.Slice(indexList, func(i, j int) bool { return indexList[i] < indexList[j] })
	return indexList, nil
}

func (s *SDClient) genIndex(indexList []int64) int64 {
	if indexList == nil {
		return 0
	}
	for i := int64(0); ; i++ {
		if !s.contains(indexList, i) {
			return i
		}
	}
}

func (s *SDClient) Register(service string, topicPrefix string, specifiedTopic string) (string, error) {
	err := s.locker.Lock()
	if err != nil {
		log.Panicf("failed to lock, service[%s], topicPrefix[%s]", service, topicPrefix)
		return "", err
	}
	defer func() {
		err := s.locker.UnLock()
		if err != nil {
			log.Panicf("failed to unlock, service[%s], topicPrefix[%s]", service, topicPrefix)
		}
	}()
	if topicPrefix == "" {
		topicPrefix = service
	}
	if err := s.createServiceIfNotExist(service); err != nil {
		log.Errorf("failed to create node, service[%s], topicPrefix[%s]", service, topicPrefix)
		return "", err
	}
	topic := fmt.Sprintf("%s%s%d", topicPrefix, splitSign, 0)
	if specifiedTopic == "" {
		endpoints, err := s.listEndpoints(service)
		if err != nil {
			log.Errorf("failed to list endpoint, service[%s]", service)
			return "", err
		}

		if len(endpoints) != 0 {
			indexList, err := s.getEndpointIndex(endpoints)
			if err != nil {
				log.Errorf("failed to parse index for endpoints[%+v]", endpoints)
				return "", err
			}
			topicIndex := s.genIndex(indexList)
			topic = fmt.Sprintf("%s%s%d", topicPrefix, splitSign, topicIndex)
		}
	} else {
		topic = specifiedTopic
	}

	path := fmt.Sprintf("%s/%s/%s", s.zkRoot, service, topic)
	endpoint := Endpoint{Service: service, Topic: topic}
	data, err := json.Marshal(endpoint)
	if err != nil {
		log.Errorf("failed to marshal endpoint, service[%s], topic[%s]", service, topic)
		return "", err
	}
	_, err = s.conn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Errorf("failed to create endpoint, service[%s], topic[%s]", service, topic)
		return "", err
	}

	// recreate service topic when 1) error occurred or 2) topic found not exist
	watchPath := fmt.Sprintf("%s/%s", s.zkRoot, service)
	childrenRes, errorsRes := s.watchPath(watchPath)
	go func() {
		for {
			select {
			case children := <-childrenRes:
				log.Infof("watch changed children[%+v], service[%s]", children, service)
			case err := <-errorsRes:
				log.Errorf("watch err[%+v], service[%s]", err, service)
			}
			func() {
				err := s.locker.Lock()
				if err != nil {
					log.Panicf("failed to lock, service[%s]", service)
				}
				defer func() {
					err := s.locker.UnLock()
					if err != nil {
						log.Panicf("failed to unlock, service[%s]", service)
					}
				}()
				exists, _, err := s.conn.Exists(path)
				if err != nil {
					log.Panicf("failed to check exists path[%s]", path)
					return
				}
				if exists {
					log.Infof("exists path[%s]", path)
					return
				}
				_, err = s.conn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
				if err != nil {
					log.Panicf("failed to create endpoint, service[%s], topic[%s]", service, topic)
					return
				}
			}()
		}
	}()
	return topic, nil
}

func (s *SDClient) Deregister(service string, topic string) error {
	err := s.locker.Lock()
	if err != nil {
		log.Panicf("failed to lock, service[%s]", service)
		return err
	}
	defer func() {
		err := s.locker.UnLock()
		if err != nil {
			log.Panicf("failed to unlock, service[%s]", service)
		}
	}()
	path := fmt.Sprintf("%s/%s/%s", s.zkRoot, service, topic)
	_, stat, err := s.conn.Get(path)
	if err != nil {
		log.Errorf("failed to get stat, service[%s], topic[%s]", service, topic)
		return err
	}
	err = s.conn.Delete(path, stat.Aversion)
	if err != nil {
		log.Errorf("failed to delete path[%s]", path)
		return err
	}
	log.Infof("succeed to deregister service[%s], topic[%s]", service, topic)
	return nil
}

func (s *SDClient) listEndpoints(service string) ([]string, error) {
	if service == "" {
		return nil, errors.New("service is empty")
	}
	path := fmt.Sprintf("%s/%s", s.zkRoot, service)
	children, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return []string{}, nil
		}
		return nil, err
	}
	var nodes []string
	for _, child := range children {
		fullPath := path + "/" + child
		data, _, err := s.conn.Get(fullPath)
		if err != nil {
			if err == zk.ErrNoNode {
				continue
			}
			return nil, err
		}
		node := new(Endpoint)
		err = json.Unmarshal(data, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node.Topic)
	}
	sort.Strings(nodes)
	return nodes, nil
}

func (s *SDClient) ListEndpoints(service string) ([]string, error) {
	err := s.locker.Lock()
	if err != nil {
		log.Panicf("failed to lock, service[%s]", service)
		return nil, err
	}
	defer func() {
		err := s.locker.UnLock()
		if err != nil {
			log.Panicf("failed to unlock, service[%s]", service)
		}
	}()
	if service == "" {
		return nil, errors.New("service is empty")
	}
	return s.listEndpoints(service)
}

func (s *SDClient) watchPath(path string) (chan zk.Event, chan error) {
	childrenRes := make(chan zk.Event)
	errorsRes := make(chan error)

	go func() {
		for {
			children, _, childEvent, err := s.conn.ChildrenW(path)
			if err != nil {
				log.Errorf("error watched, err[%+v]", err)
				errorsRes <- err
			} else {
				event := <-childEvent
				log.Errorf("event watched, children[%+v], event[%+v]", children, event)
				childrenRes <- event
			}
		}
	}()
	return childrenRes, errorsRes
}

type WatchHandler func(service string, endpoints []string)

func (s *SDClient) Watch(service string, handler WatchHandler) error {
	err := s.locker.Lock()
	if err != nil {
		log.Panicf("failed to lock, service[%s]", service)
		return err
	}
	defer func() {
		err := s.locker.UnLock()
		if err != nil {
			log.Panicf("failed to unlock, service[%s]", service)
		}
	}()
	if service == "" {
		return errors.New("service is empty")
	}
	path := fmt.Sprintf("%s/%s", s.zkRoot, service)
	childrenRes, errorsRes := s.watchPath(path)
	go func() {
		for {
			select {
			case children := <-childrenRes:
				log.Infof("watch changed children[%+v], service[%s]", children, service)
			case err := <-errorsRes:
				log.Errorf("watch err[%+v], service[%s]", err, service)
			}
			func() {
				err := s.locker.Lock()
				if err != nil {
					log.Panicf("failed to lock, service[%s]", service)
				}
				defer func() {
					err := s.locker.UnLock()
					if err != nil {
						log.Panicf("failed to unlock, service[%s]", service)
					}
				}()
				endpoints, err := s.listEndpoints(service)
				if err != nil {
					log.Panicf("failed to list endpoint, service[%s]", service)
				} else {
					log.Infof("handler endpoints[%+v]", endpoints)
					handler(service, endpoints)
				}
			}()
		}
	}()
	return nil
}
