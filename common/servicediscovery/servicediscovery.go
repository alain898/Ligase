package servicediscovery

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ServiceNode struct {
	Name string `json:"name"` // 服务名称，这里是user
	Host string `json:"host"`
	Port int    `json:"port"`
}

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

func (s *ZKClient) Register(node *ServiceNode) error {
	if err := s.createNodeIfNotExist(node.Name); err != nil {
		return err
	}
	path := s.zkRoot + "/" + node.Name + "/n"
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	_, err = s.conn.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

func (s *ZKClient) GetNodes(name string) ([]*ServiceNode, error) {
	path := s.zkRoot + "/" + name
	// 获取字节点名称
	children, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return []*ServiceNode{}, nil
		}
		return nil, err
	}
	nodes := make([]*ServiceNode, len(children))
	for _, child := range children {
		fullPath := path + "/" + child
		data, _, err := s.conn.Get(fullPath)
		if err != nil {
			if err == zk.ErrNoNode {
				continue
			}
			return nil, err
		}
		node := new(ServiceNode)
		err = json.Unmarshal(data, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}
