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
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Delete{
				Cf:    req.Cf,
				Key:   req.Key,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	rawScanResponse := kvrpcpb.RawScanResponse{}
	it := reader.IterCF(req.Cf)
	it.Seek(req.StartKey)
	var count uint32
	count = 0
	for it.Valid() && count < req.Limit {
		value, err := it.Item().Value()
		if err != nil {
			return nil, err
		}
		rawScanResponse.Kvs = append(rawScanResponse.Kvs, &kvrpcpb.KvPair{
			Key:it.Item().Key(),
			Value: value,
		})
		it.Next()
		count ++
	}
	return &rawScanResponse, nil
}
