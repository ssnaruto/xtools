package agg_system

import (
	"math"
	"strconv"
	"sync"
)

type MemCache struct {
	Workers []*MapCache

	PartitionItemLimit float64
	MaxItems           float64
	NumberOfPartition  float64
}

func (m *MemCache) GetCachePartition(partitionId string) *MapCache {
	id, _ := strconv.ParseFloat(partitionId, 64)
	partition := math.Floor(id / m.MaxItems * m.NumberOfPartition)
	if partition > m.NumberOfPartition || partition < 0 {
		partition = m.NumberOfPartition
	}

	return m.Workers[int(partition)]
}

func NewMemCache(maxItems float64) MemCache {
	result := MemCache{
		Workers:            []*MapCache{},
		PartitionItemLimit: 5000,
		MaxItems:           maxItems,
	}

	result.NumberOfPartition = math.Floor(maxItems / result.PartitionItemLimit)

	for i := 0; i < int(result.NumberOfPartition); i++ {
		result.Workers = append(result.Workers, NewMapCache())
	}

	return result
}

func NewMapCache() *MapCache {
	return &MapCache{
		Records: make(map[string]MetricsData),
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

type MetricsData struct {
	Dimesions map[string]string
	Metrics   map[string]float64
}
