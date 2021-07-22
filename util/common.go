package util

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/smallnest/rpcx/log"

	"github.com/henrylee2cn/goutil"
)

var (
	// serviceNamespace the service prefix of ETCD key
	serviceNamespace = "RPCX-SRV@"
)

// ServiceNamespace returns the service prefix of ETCD key.
func ServiceNamespace() string {
	return serviceNamespace
}

// SetServiceNamespace sets the service prefix of ETCD key.
// Note: It should be called the first time after importing this package.
func SetServiceNamespace(prefix string) {
	serviceNamespace = prefix
}

// ServiceInfo serivce info
type ServiceInfo struct {
	UriPaths []string `json:"uri_paths"`
	mu       sync.RWMutex
}

// String returns the JSON string.
func (s *ServiceInfo) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, _ := json.Marshal(s)
	return goutil.BytesToString(b)
}

// Append appends uri path
func (s *ServiceInfo) Append(uriPath ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.UriPaths = append(s.UriPaths, uriPath...)
}

func CreateServiceKey(addr string) string {
	return serviceNamespace + addr
}

func GetHostport(serviceKey string) string {
	return strings.TrimPrefix(serviceKey, serviceNamespace)
}

func GetServiceInfo(value []byte) *ServiceInfo {
	info := &ServiceInfo{}
	err := json.Unmarshal(value, info)
	if err != nil {
		log.Errorf("%s", err.Error())
	}
	return info
}
