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
	Lock()
	UnLock()
}

// DSClient is not concurrency safe, so please use a extra distribute lock to ensure concurrency safe when it's necessary.
type DSClient struct {
	zkServers []string // zookeeper servers
	zkRoot    string   // root path for store the meta data for service discovery
	conn      *zk.Conn // zookeeper client connect
	locker    wrapLocker
}

type wrapLocker struct {
	locker Locker
}

func (wl *wrapLocker) Lock() {
	if wl.locker != nil {
		wl.locker.Lock()
	}
}

func (wl *wrapLocker) UnLock() {
	if wl.locker != nil {
		wl.locker.UnLock()
	}
}

func NewDSClient(zkServers []string, zkRoot string, timeoutSeconds int, locker Locker) (*DSClient, error) {
	client := new(DSClient)
	client.zkServers = zkServers
	client.zkRoot = zkRoot
	client.locker = wrapLocker{locker: locker}
	conn, _, err := zk.Connect(zkServers, time.Duration(timeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	client.conn = conn
	if err := client.createRootIfNotExist(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

func (s *DSClient) Close() {
	s.conn.Close()
}

func (s *DSClient) createRootIfNotExist() error {
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

func (s *DSClient) createNodeIfNotExist(name string) error {
	path := s.zkRoot + "/" + name
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

func (s *DSClient) contains(list []int64, e int64) bool {
	for _, a := range list {
		if a == e {
			return true
		}
	}
	return false
}

func (s *DSClient) getEndpointIndex(endpoints []*Endpoint) ([]int64, error) {
	if endpoints == nil {
		return nil, errors.New("endpoints is nil")
	}
	var indexList []int64
	for _, ep := range endpoints {
		splits := strings.Split(ep.Topic, splitSign)
		if len(splits) != 2 {
			err := fmt.Sprintf("invalid topic[%s], service[%s]", ep.Topic, ep.Service)
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

func (s *DSClient) genIndex(indexList []int64) int64 {
	if indexList == nil {
		return 0
	}
	for i := int64(0); ; i++ {
		if !s.contains(indexList, i) {
			return i
		}
	}
}

func (s *DSClient) Register(service string, topicPrefix string) (string, error) {
	s.locker.Lock()
	defer s.locker.UnLock()
	if topicPrefix == "" {
		topicPrefix = service
	}
	if err := s.createNodeIfNotExist(service); err != nil {
		log.Errorf("failed to create node, service[%s], topicPrefix[%s]", service, topicPrefix)
		return "", err
	}
	endpoints, err := s.listEndpoint(service)
	if err != nil {
		log.Errorf("failed to list endpoint, service[%s]", service)
		return "", err
	}
	topic := fmt.Sprintf("%s%s%d", topicPrefix, splitSign, 0)
	if len(endpoints) != 0 {
		indexList, err := s.getEndpointIndex(endpoints)
		if err != nil {
			log.Errorf("failed to parse index for endpoints[%+v]", endpoints)
			return "", err
		}
		topicIndex := s.genIndex(indexList)
		topic = fmt.Sprintf("%s%s%d", topicPrefix, splitSign, topicIndex)
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
	return topic, nil
}

func (s *DSClient) Deregister(service string, topic string) error {
	s.locker.Lock()
	defer s.locker.UnLock()
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

func (s *DSClient) listEndpoint(service string) ([]*Endpoint, error) {
	if service == "" {
		return nil, errors.New("service is empty")
	}
	path := fmt.Sprintf("%s/%s", s.zkRoot, service)
	children, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return []*Endpoint{}, nil
		}
		return nil, err
	}
	var nodes []*Endpoint
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
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (s *DSClient) watchPath(path string) (chan []string, chan error) {
	childrenRes := make(chan []string)
	errorsRes := make(chan error)

	go func() {
		for {
			children, _, events, err := s.conn.ChildrenW(path)
			if err != nil {
				errorsRes <- err
				return
			}
			childrenRes <- children
			evt := <-events
			if evt.Err != nil {
				errorsRes <- evt.Err
				return
			}
		}
	}()
	return childrenRes, errorsRes
}

type WatchHandler func(endpoints []*Endpoint)

func (s *DSClient) Watch(service string, handler WatchHandler) error {
	s.locker.Lock()
	defer s.locker.UnLock()
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
				func() {
					s.locker.Lock()
					defer s.locker.UnLock()
					endpoints, err := s.listEndpoint(service)
					if err != nil {
						log.Errorf("failed to list endpoint, service[%s]", service)
					} else {
						log.Infof("handler endpoints[%+v]", endpoints)
						handler(endpoints)
					}
				}()
			case err := <-errorsRes:
				log.Infof("watch err[%+v], service[%s]", err, service)
			}
		}
	}()
	return nil
}
