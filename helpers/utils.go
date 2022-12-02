package helpers

import (
	"fmt"
	"net"
	"os"
	"sync"
)

type SyncMap struct {
	m sync.Map
}

func (s *SyncMap) Set(key interface{}, val interface{}) {
	s.m.Store(key, val)
}

func (s *SyncMap) Get(key interface{}) (interface{}, bool) {
	return s.m.Load(key)
}

func (s *SyncMap) Del(key interface{}) {
	s.m.Delete(key)
}

func CreateUnixListenter(addr string) (net.Listener, error) {
	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("unexpected error when trying to remove unix socket file %q: %w", addr, err)
	}
	ln, err := net.Listen("unix", addr)
	if err != nil {
		return nil, err
	}

	mode := os.FileMode(0777)
	if err = os.Chmod(addr, mode); err != nil {
		return nil, fmt.Errorf("cannot chmod %#o for %q: %w", mode, addr, err)
	}
	return ln, nil
}
