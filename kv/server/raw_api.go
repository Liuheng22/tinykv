package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	val, _ := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	resp := &kvrpcpb.RawGetResponse{Value: val}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}})
	err := server.storage.Write(nil, batch)
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Delete{Cf: req.GetCf(), Key: req.GetKey()}})
	err := server.storage.Write(nil, batch)
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	var resp kvrpcpb.RawScanResponse
	for i := 0; i < int(req.GetLimit()) && iter.Valid(); i++ {
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: key, Value: val})
		iter.Next()
	}
	iter.Close()
	return &resp, nil
}
