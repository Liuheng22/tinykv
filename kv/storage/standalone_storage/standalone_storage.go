package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	engines *engine_util.Engines

}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	newDB := engine_util.CreateDB(conf.DBPath, conf.Raft)
	return &StandAloneStorage{
		db: newDB,
		engines: engine_util.NewEngines(newDB, nil, conf.DBPath, ""),
	}

}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}


func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, modify := range batch  {
		writeBatch.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	err := writeBatch.WriteToDB(s.db)
	return err
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(s.db, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	txn := s.db.NewTransaction(false)
	//defer txn.Discard()
	return engine_util.NewCFIterator(cf, txn)
}

func (s *StandAloneStorage) Close() {
}
