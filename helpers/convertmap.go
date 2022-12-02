package helpers

import (
	"fmt"

	aer "github.com/aerospike/aerospike-client-go"
)

func cleanupInterfaceArray(in []interface{}) []interface{} {
	res := make([]interface{}, len(in))
	for i, v := range in {
		res[i] = CleanupMapValue(v)
	}
	return res
}

func cleanupInterfaceMap(in map[interface{}]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range in {
		res[fmt.Sprintf("%v", k)] = CleanupMapValue(v)
	}
	return res
}

func cleanupInterfaceBinMap(in aer.BinMap) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range in {
		res[fmt.Sprintf("%v", k)] = CleanupMapValue(v)
	}
	return res
}

func CleanupMapValue(v interface{}) interface{} {
	switch vl := v.(type) {
	case []interface{}:
		return cleanupInterfaceArray(vl)
	case map[interface{}]interface{}:
		return cleanupInterfaceMap(vl)
	case aer.BinMap:
		return cleanupInterfaceBinMap(vl)
	default:
		return vl
	}
}
