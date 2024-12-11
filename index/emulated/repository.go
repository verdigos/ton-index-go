package emulated

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"strings"
)

type EmulatedTracesRepository struct {
	rdb *redis.Client
}

func NewRepository() *EmulatedTracesRepository {
	return &EmulatedTracesRepository{rdb: redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Username: "app",
		Password: "111111",
	})}
}

func (receiver *EmulatedTracesRepository) LoadRawTraces(trace_ids []string) (map[string]map[string][]interface{}, error) {
	result := make(map[string]map[string][]interface{})
	for _, trace_id := range trace_ids {
		trace_query := receiver.rdb.HGetAll(context.Background(), trace_id)
		trace_query_result, err := trace_query.Result()
		if err != nil {
			return nil, err
		}
		trace_tx_map := make(map[string][]interface{})
		for key, value := range trace_query_result {
			decoder := msgpack.NewDecoder(strings.NewReader(value))
			decoder.UseLooseInterfaceDecoding(true)
			slice, err := decoder.DecodeSlice()
			if err != nil {
				return nil, err
			}
			trace_tx_map[key] = slice
		}
		result[trace_id] = trace_tx_map
	}
	return result, nil
}

func (receiver *EmulatedTracesRepository) GetTraceIdsByAccount(account string) ([]string, error) {
	// read hset from redis into keys array
	scores := receiver.rdb.ZRevRangeWithScores(context.Background(), account, 0, -1)
	result, err := scores.Result()
	var trace_ids []string
	if err != nil {
		return nil, err
	}
	for _, z := range result {
		key := z.Member.(string)
		// split key by :
		split := strings.Split(key, ":")
		if len(split) != 2 {
			return nil, errors.New("Invalid key format")
		}
		trace_id := split[0]
		keys_query := receiver.rdb.Keys(context.Background(), trace_id)
		key_query_result, err := keys_query.Result()
		if err != nil {
			return nil, err
		}
		if len(key_query_result) == 0 {
			return nil, nil
		}
		trace_ids = append(trace_ids, trace_id)
	}
	return trace_ids, nil
}
