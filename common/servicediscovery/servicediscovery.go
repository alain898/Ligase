package servicediscovery

import (
	"encoding/json"
	"github.com/finogeeks/ligase/skunkworks/log"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"time"
)

// DSClient is not concurrency safe, so please use a extra dist lock to ensure concurrency safe when it's necessary.
type DSClient struct {
	zkServers []string // zookeeper servers
	zkRoot    string   // root path for store the meta data for service discovery
	conn      *zk.Conn // zookeeper client connect
}

func NewDSClient(zkServers []string, zkRoot string, timeoutSeconds int) (*DSClient, error) {
	client := new(DSClient)
	client.zkServers = zkServers
	client.zkRoot = zkRoot
	conn, _, err := zk.Connect(zkServers, time.Duration(timeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	client.conn = conn
	// 创建服务根节点
	if err := client.createRootIfNotExist(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

// 关闭连接，释放临时节点
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
	Service string `json:"service"` // 服务名称，这里是user
	Topic   string `json:"topic"`
}

func (s *DSClient) Register(service string, topicPrefix string) (string, error) {
	if topicPrefix == "" {
		topicPrefix = service
	}
	if err := s.createNodeIfNotExist(service); err != nil {
		log.Errorf("failed to create node, service[%s], topicPrefix[%s]", service, topicPrefix)
		return "", err
	}
	topic := fmt.Sprint("%s___$d", topicPrefix, 0) // todo: gen topic by existed topics
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

func (s *DSClient) GetNodes(name string) ([]*Endpoint, error) {
	path := s.zkRoot + "/" + name
	// 获取字节点名称
	children, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return []*Endpoint{}, nil
		}
		return nil, err
	}
	nodes := make([]*Endpoint, len(children))
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
