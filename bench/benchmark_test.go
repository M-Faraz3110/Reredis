package store__test

import (
	"fmt"
	"log"
	"reredis/pkg/resp"
	"reredis/pkg/store"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkSet(b *testing.B) {
	s := store.NewStore()
	b.ResetTimer()
	const keyspace = 10000
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%keyspace)
		keyVal := resp.Value{Bulk: &key}
		val := "value"
		valVal := resp.Value{Bulk: &val}
		args := []resp.Value{keyVal, valVal}
		result := s.Set(args)
		require.Equal(b, "string", result.Type)
		require.Equal(b, "OK", *result.String)
	}
	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.Logf("SET ops/sec: %.2f", ops)
}

func BenchmarkGet(b *testing.B) {
	s := store.NewStore()
	const keyspace = 10000
	// Pre-populate keys
	for i := 0; i < keyspace; i++ {
		key := fmt.Sprintf("key%d", i)
		keyVal := resp.Value{Bulk: &key}
		val := "value"
		valVal := resp.Value{Bulk: &val}
		args := []resp.Value{keyVal, valVal}
		s.Set(args)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%keyspace)
		keyVal := resp.Value{Bulk: &key}
		args := []resp.Value{keyVal}
		result := s.Get(args)
		require.Equal(b, "bulk", result.Type)
		require.Equal(b, "value", *result.Bulk)
	}
	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.Logf("GET ops/sec: %.2f", ops)
}

func BenchmarkHSet(b *testing.B) {
	s := store.NewStore()
	b.ResetTimer()
	const hashspace = 1000
	const fieldspace = 1000
	for i := 0; i < b.N; i++ {
		hash := fmt.Sprintf("hash%d", i%hashspace)
		field := fmt.Sprintf("field%d", i%fieldspace)
		hashVal := resp.Value{Bulk: &hash}
		fieldVal := resp.Value{Bulk: &field}
		val := "value"
		valVal := resp.Value{Bulk: &val}
		args := []resp.Value{hashVal, fieldVal, valVal}
		result := s.HSet(args)
		require.Equal(b, "string", result.Type)
	}
	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.Logf("HSET ops/sec: %.2f", ops)
}

// func BenchmarkLpush(b *testing.B) {
// 	s := store.NewStore()
// 	const listspace = 1000
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		list := fmt.Sprintf("list%d", i%listspace)
// 		listVal := resp.Value{Bulk: &list}
// 		val := "value"
// 		valVal := resp.Value{Bulk: &val}
// 		args := []resp.Value{listVal, valVal}
// 		result := s.LPush(args)
// 		require.Equal(b, "integer", result.Type)
// 	}
// 	b.StopTimer()
// 	ops := float64(b.N) / b.Elapsed().Seconds()
// 	b.Logf("LPUSH ops/sec: %.2f", ops)
// }

// // }

// func BenchmarkLpop(b *testing.B) {
// 	s := store.NewStore()
// 	const listspace = 1000
// 	list := "benchlist"
// 	listVal := resp.Value{Bulk: &list}
// 	val := "value"
// 	valVal := resp.Value{Bulk: &val}
// 	argsPush := []resp.Value{listVal, valVal}
// 	// Pre-populate the list with listspace elements
// 	for i := 0; i < listspace; i++ {
// 		s.LPush(argsPush)
// 	}
// 	b.ResetTimer()
// 	argsPop := []resp.Value{listVal}
// 	for i := 0; i < b.N; i++ {
// 		result := s.LPop(argsPop)
// 		// Accept both string and nil (empty list) results

func BenchmarkHGet(b *testing.B) {
	s := store.NewStore()
	const hashspace = 100
	const fieldspace = 100
	// Pre-populate hashes and fields
	for i := 0; i < hashspace; i++ {
		hash := fmt.Sprintf("hash%d", i)
		for j := 0; j < fieldspace; j++ {
			field := fmt.Sprintf("field%d", j)
			val := "value"
			hashVal := resp.Value{Bulk: &hash}
			fieldVal := resp.Value{Bulk: &field}
			valVal := resp.Value{Bulk: &val}
			args := []resp.Value{hashVal, fieldVal, valVal}
			s.HSet(args)
		}
	}
	log.Println("Finished HSET")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash := fmt.Sprintf("hash%d", i%hashspace)
		field := fmt.Sprintf("field%d", i%fieldspace)
		hashVal := resp.Value{Bulk: &hash}
		fieldVal := resp.Value{Bulk: &field}
		args := []resp.Value{hashVal, fieldVal}
		result := s.HGet(args)
		require.Equal(b, "bulk", result.Type, "HGet returned wrong type for hash=%s field=%s", hash, field)
		require.NotNil(b, result.Bulk, "HGet returned nil Bulk for hash=%s field=%s", hash, field)
		require.Equal(b, "value", *result.Bulk, "HGet returned wrong value for hash=%s field=%s", hash, field)
	}
	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.Logf("HGET ops/sec: %.2f", ops)
}

// 		require.True(b, result.Type == "string" || result.Type == "nil")
// 	}
// 	b.StopTimer()
// 	ops := float64(b.N) / b.Elapsed().Seconds()
// 	b.Logf("LPOP ops/sec: %.2f", ops)
// }

// }

func BenchmarkParallelSet(b *testing.B) {
	s := store.NewStore()
	const keyspace = 10000
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			key := fmt.Sprintf("pkey%d", i%keyspace)
			keyVal := resp.Value{Bulk: &key}
			val := "value"
			valVal := resp.Value{Bulk: &val}
			args := []resp.Value{keyVal, valVal}
			result := s.Set(args)
			require.Equal(b, "string", result.Type)
			require.Equal(b, "OK", *result.String)
		}
	})
	b.StopTimer()
	ops := float64(b.N) / b.Elapsed().Seconds()
	b.Logf("Parallel SET ops/sec: %.2f", ops)
}

// }
