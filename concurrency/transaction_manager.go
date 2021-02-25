package concurrency

import (
	"errors"
	"github.com/xiaobogaga/fakedb/buffer_logging"
	"github.com/xiaobogaga/fakedb/util"
	"sync"
)

var transManagerLog = util.GetLog("transactionManager")

type TransactionManager struct {
	Lock        sync.Mutex
	TransId     uint64
	LockManager *LockManager
	Buf         *buffer_logging.BufferManager
	Log         *buffer_logging.LogManager
	Trans       map[uint64]*Transaction
}

func NewTransactionManager(startTransactionId uint64, bufManager *buffer_logging.BufferManager,
	logManager *buffer_logging.LogManager) *TransactionManager {
	transManager := &TransactionManager{
		Lock:        sync.Mutex{},
		TransId:     startTransactionId,
		LockManager: NewLockManager(),
		Buf:         bufManager,
		Log:         logManager,
		Trans:       map[uint64]*Transaction{},
	}
	return transManager
}

type Transaction struct {
	TransactionId  uint64
	LockManager    *LockManager
	Buf            *buffer_logging.BufferManager
	HoldingLocks   map[int32]bool
	Lock           sync.Mutex
	Log            *buffer_logging.LogManager
	WriteSet       map[int32]*WriteSet // page id -> WriteSet
	PrevLsn        int64
	TransLockState TransLockState
}

type TransactionState byte

type WriteSet struct {
	BeforeValue *buffer_logging.Pair
	AfterValue  *buffer_logging.Pair
}

func (trans *TransactionManager) NewTransaction() *Transaction {
	trans.Lock.Lock()
	defer trans.Lock.Unlock()
	ret := &Transaction{
		TransactionId:  trans.TransId,
		LockManager:    trans.LockManager,
		Buf:            trans.Buf,
		HoldingLocks:   map[int32]bool{},
		Log:            trans.Log,
		PrevLsn:        buffer_logging.InvalidLsn,
		TransLockState: TransGrowing,
		WriteSet:       map[int32]*WriteSet{},
	}
	trans.Trans[ret.TransactionId] = ret
	transManagerLog.InfoF("create new transaction, txnId: %d", ret.TransactionId)
	trans.TransId++
	return ret
}

var txnLog = util.GetLog("transaction")

func (txn *Transaction) Begin() {
	txnLog.InfoF("begin, txnId: %d", txn.TransactionId)
	log := &buffer_logging.LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   buffer_logging.InvalidLsn,
		TransactionId: txn.TransactionId,
		TP:            buffer_logging.TransBeginLogType,
	}
	txn.Log.Append(log) // Begin Log
	txn.PrevLsn = log.LSN
}

func (txn *Transaction) Commit() {
	txnLog.InfoF("commit, txnId: %d", txn.TransactionId)
	log := &buffer_logging.LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   buffer_logging.InvalidLsn,
		TransactionId: txn.TransactionId,
		TP:            buffer_logging.TransCommitLogType,
		Force:         true,
	}
	txn.Log.Append(log) // Begin Log
	txn.PrevLsn = log.LSN
	txn.TransLockState = TransCommit
	txn.Log.WaitFlush(log)
	txn.ReleaseLocks()
}

func (txn *Transaction) Rollback() error {
	txnLog.InfoF("rollback, txnId: %d", txn.TransactionId)
	log := &buffer_logging.LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   buffer_logging.InvalidLsn,
		TransactionId: txn.TransactionId,
		TP:            buffer_logging.TransAbortLogType,
	}
	txn.Log.Append(log) // Begin Log
	txn.PrevLsn = log.LSN
	err := txn.Undo(log.LSN)
	if err != nil {
		return err
	}
	txn.WriteSet = map[int32]*WriteSet{}
	txn.TransLockState = TransAbort
	txn.ReleaseLocks()
	return nil
}

func (txn *Transaction) Undo(lsn int64) error {
	for pageId, writeSet := range txn.WriteSet {
		err := txn.Buf.Set(pageId, writeSet.BeforeValue.Key, writeSet.BeforeValue.Value, lsn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (txn *Transaction) ReleaseLocks() {
	for lockId := range txn.HoldingLocks {
		txn.LockManager.Unlock(lockId, txn)
		txn.RemoveLock(lockId)
	}
}

var txnLockConflict = errors.New("cannot acquire lock, now transaction aborted")

// Return whether found the key, the value of the key if found. error. Also return the index of the key: 0(not found), 1, 2
func (txn *Transaction) Get(key []byte) (int32, bool, []byte, error) {
	txnLog.InfoF("get, txnId: %d, key: %s", txn.TransactionId, string(key))
	ok := txn.LockManager.LockShared(1, txn)
	if !ok {
		txn.Rollback()
		return 0, false, nil, txnLockConflict
	}
	found, value, err := txn.Buf.Get(1, key)
	if err != nil {
		return 0, false, nil, err
	}
	if found {
		return 1, true, value, nil
	}
	ok = txn.LockManager.LockShared(2, txn)
	if !ok {
		txn.Rollback()
		return 0, false, nil, txnLockConflict
	}
	found, value, err = txn.Buf.Get(2, key)
	if err != nil {
		return 0, false, nil, err
	}
	if found {
		return 2, true, value, nil
	}
	return 0, false, nil, nil
}

func checkKeyValueSizeLimit(key []byte, value []byte) bool {
	if 8+4+len(key)+4+len(value) > buffer_logging.PageSize {
		return false
	}
	return true
}

var KeyValueSizeTooLargeError = errors.New("key value is too large")

func (txn *Transaction) Set(key, value []byte) error {
	txnLog.InfoF("set, txnId: %d, key: %s, value: %s", txn.TransactionId, string(key), string(value))
	if !checkKeyValueSizeLimit(key, value) {
		return KeyValueSizeTooLargeError
	}
	id, found, v, err := txn.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return txn.Add(key, value)
	}
	ok := txn.LockManager.LockUpgrade(id, txn)
	if !ok {
		txn.Rollback()
		return txnLockConflict
	}
	// Todo:
	// * support lock on txn.
	// * support before and after value.
	beforeValue := &buffer_logging.Pair{Key: key, Value: v}
	afterValue := &buffer_logging.Pair{Key: key, Value: value}
	log := &buffer_logging.LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   buffer_logging.InvalidLsn,
		TransactionId: txn.TransactionId,
		PageId:        int32(id),
		TP:            buffer_logging.SetLogType,
		ActionTP:      buffer_logging.SetAction,
		BeforeValue:   beforeValue.Serialize(),
		AfterValue:    afterValue.Serialize(),
	}
	log = txn.Log.Append(log)
	txn.PrevLsn = log.LSN
	err = txn.Buf.Set(id, key, value, log.LSN)
	if err != nil {
		return err
	}
	txn.AddSetWrite(id, afterValue, beforeValue)
	return nil
}

func (txn *Transaction) Add(key, value []byte) error {
	if txn.Buf.Size() >= 2 {
		return errors.New("full")
	}
	if !checkKeyValueSizeLimit(key, value) {
		return KeyValueSizeTooLargeError
	}
	txnLog.InfoF("add, txnId: %d, key: %s, value: %s", txn.TransactionId, string(key), string(value))
	pair := &buffer_logging.Pair{Key: key, Value: value}
	emptyPair := &buffer_logging.Pair{}
	if txn.Buf.IsEmpty(1) {
		ok := txn.LockManager.LockUpgrade(1, txn)
		if !ok {
			txn.Rollback()
			return txnLockConflict
		}
		log := &buffer_logging.LogRecord{
			PrevLsn:       txn.PrevLsn,
			UndoNextLsn:   buffer_logging.InvalidLsn,
			TransactionId: txn.TransactionId,
			PageId:        1,
			TP:            buffer_logging.SetLogType,
			ActionTP:      buffer_logging.AddAction,
			BeforeValue:   emptyPair.Serialize(),
			AfterValue:    pair.Serialize(),
		}
		txn.Log.Append(log)
		txn.PrevLsn = log.LSN
		txn.Buf.Set(1, key, value, log.LSN)
		txn.AddAddWrite(1, pair, emptyPair)
		return nil
	}
	ok := txn.LockManager.LockUpgrade(2, txn)
	if !ok {
		txn.Rollback()
		return txnLockConflict
	}
	log := &buffer_logging.LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   buffer_logging.InvalidLsn,
		TransactionId: txn.TransactionId,
		PageId:        2,
		TP:            buffer_logging.SetLogType,
		ActionTP:      buffer_logging.AddAction,
		BeforeValue:   emptyPair.Serialize(),
		AfterValue:    pair.Serialize(),
	}
	log = txn.Log.Append(log)
	txn.PrevLsn = log.LSN
	err := txn.Buf.Set(2, key, value, log.LSN)
	if err != nil {
		return err
	}
	txn.AddAddWrite(2, pair, emptyPair)
	return nil
}

var ErrKeyNotFound = errors.New("key not found")

func (txn *Transaction) Del(key []byte) error {
	txnLog.InfoF("del, txnId: %d, key: %s", txn.TransactionId, string(key))
	id, found, value, err := txn.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return ErrKeyNotFound
	}
	pair := &buffer_logging.Pair{Key: key, Value: value}
	ok := txn.LockManager.LockExclusive(id, txn)
	if !ok {
		txn.Rollback()
		return txnLockConflict
	}
	emptyPair := &buffer_logging.Pair{}
	log := &buffer_logging.LogRecord{
		PrevLsn:       txn.PrevLsn,
		UndoNextLsn:   buffer_logging.InvalidLsn,
		TransactionId: txn.TransactionId,
		PageId:        int32(id),
		TP:            buffer_logging.SetLogType,
		ActionTP:      buffer_logging.DelAction,
		BeforeValue:   pair.Serialize(),
		AfterValue:    emptyPair.Serialize(),
	}
	log = txn.Log.Append(log)
	txn.PrevLsn = log.LSN
	err = txn.Buf.Del(id, log.LSN)
	if err != nil {
		return err
	}
	txn.AddDelWrite(id, emptyPair, pair)
	return nil
}

func (txn *Transaction) AddLock(id int32) {
	txn.HoldingLocks[id] = true
}

func (txn *Transaction) RemoveLock(id int32) {
	delete(txn.HoldingLocks, id)
}

func (txn *Transaction) AddSetWrite(id int32, new, orig *buffer_logging.Pair) {
	old, ok := txn.WriteSet[id]
	if !ok {
		txn.WriteSet[id] = &WriteSet{BeforeValue: orig, AfterValue: new}
		return
	}
	old.AfterValue = new
}

func (txn *Transaction) AddDelWrite(id int32, new, orig *buffer_logging.Pair) {
	old, ok := txn.WriteSet[id]
	if !ok {
		txn.WriteSet[id] = &WriteSet{
			BeforeValue: orig,
			AfterValue:  new,
		}
		return
	}
	old.AfterValue = new
}

func (txn *Transaction) AddAddWrite(id int32, new, orig *buffer_logging.Pair) {
	old, ok := txn.WriteSet[id]
	if !ok {
		txn.WriteSet[id] = &WriteSet{
			BeforeValue: orig,
			AfterValue:  new,
		}
		return
	}
	old.AfterValue = new
}

func (txn *Transaction) SetTransactionLockState(state TransLockState) {
	txn.TransLockState = state
}

func (txn *Transaction) GetTransactionLockState() TransLockState {
	return txn.TransLockState
}
