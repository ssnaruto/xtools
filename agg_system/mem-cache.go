package agg_system

import (
	"math"
	"strconv"
	"sync"

	cmap "github.com/orcaman/concurrent-map/v2"
)

type CachingStorage interface {
	Set(key string, value MetricsData)
	Get(key string) (MetricsData, bool)
	Items() map[string]MetricsData
	Lock()
	Unlock()
}

type MemCache struct {
	Workers []CachingStorage

	PartitionItemLimit float64
	MaxItems           float64
	NumberOfPartition  float64
}

func (m *MemCache) GetCachePartition(partitionId string) CachingStorage {
	id, _ := strconv.ParseFloat(partitionId, 64)
	if id <= 0 {
		return m.Workers[0]
	}

	partition := math.Floor(id / m.MaxItems * m.NumberOfPartition)
	if partition > m.NumberOfPartition || partition < 0 {
		partition = m.NumberOfPartition
	}
	return m.Workers[int(partition)]
}

func NewMemCache(maxItems float64) MemCache {
	result := MemCache{
		Workers:            []CachingStorage{},
		PartitionItemLimit: 5000,
		MaxItems:           maxItems,
	}

	result.NumberOfPartition = math.Floor(maxItems / result.PartitionItemLimit)

	for i := 0; i < int(result.NumberOfPartition); i++ {
		result.Workers = append(result.Workers, NewMapCache())
	}

	return result
}

func NewMapCache() CachingStorage {
	// return &MapCache{
	// 	Records: make(map[string]MetricsData),
	// }
	
	return &CMapCache{
		Records: cmap.New[MetricsData](),
	}
}

type MapCache struct {
	Records map[string]MetricsData
	sync.Mutex
}

func (m *MapCache) Set(key string, value MetricsData) {
	m.Records[key] = value
}

func (m *MapCache) Get(key string) (MetricsData, bool) {
	if result, ok := m.Records[key]; ok {
		return result, true
	}

	return MetricsData{}, false
}

func (m *MapCache) Items() map[string]MetricsData {
	return m.Records
}

type CMapCache struct {
	Records cmap.ConcurrentMap[string, MetricsData]
	sync.Mutex
}
func (m *CMapCache) Set(key string, value MetricsData) {
	m.Records.Set(key, value)
}
func (m *CMapCache) Items() map[string]MetricsData {
	return m.Records.Items()
}
func (m *CMapCache) Get(key string) (MetricsData, bool) {
	return m.Records.Get(key)
}

type MetricsData struct {
	Dimesions map[string]string
	Metrics   map[string]float64
}
