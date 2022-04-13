package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type standaloneReader struct {
	txn *badger.Txn
}

func NewstandaloneReader(txn *badger.Txn) *standaloneReader {
	return &standaloneReader{
		txn: txn,
	}
}

func (sr *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (sr *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *standaloneReader) Close() {
	sr.txn.Discard()
}
