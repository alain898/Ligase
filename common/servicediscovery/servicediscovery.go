package servicediscovery

import (
	"encoding/json"
	"github.com/finogeeks/ligase/skunkworks/log"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"time"
)

// @NotThreadSafe
type ZKClient struct {
	zkServers []string // 多个节点地址
	zkRoot    string   // 服务根节点，这里是/api
	conn      *zk.Conn // zk的客户端连接
}

func NewZKClient(zkServers []string, zkRoot string, timeoutSeconds int) (*ZKClient, error) {
	client := new(ZKClient)
	client.zkServers = zkServers
	client.zkRoot = zkRoot
	// 连接服务器
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
func (s *ZKClient) Close() {
	s.conn.Close()
}

func (s *ZKClient) createRootIfNotExist() error {
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

func (s *ZKClient) createNodeIfNotExist(name string) error {
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

func (s *ZKClient) Register(service string, topicPrefix string) (string, error) {
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

func (s *ZKClient) Deregister(service string, topic string) error {
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

func (s *ZKClient) GetNodes(name string) ([]*Endpoint, error) {
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
