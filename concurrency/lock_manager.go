package concurrency

import (
	"github.com/xiaobogaga/fakedb/util"
	"sync"
)

var lockLog = util.GetLog("lock")

// LockManager based on 2PL.

type LockManager struct {
	Latch   sync.Mutex
	LockMap map[int32]*LockInfo
}

func NewLockManager() *LockManager {
	return &LockManager{LockMap: map[int32]*LockInfo{}}
}

type LockMode byte

const (
	LockShared LockMode = iota
	LockExclusive
)

type TransLockState byte

const (
	TransGrowing TransLockState = iota
	// TransShrinking
	TransAbort
	TransCommit
)

func (locks *LockManager) LockShared(lockId int32, transaction *Transaction) bool {
	if transaction.GetTransactionLockState() != TransGrowing {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	locks.Latch.Lock()
	lockInfo := locks.LockMap[lockId]
	if lockInfo == nil {
		lockInfo = &LockInfo{
			Holding: map[uint64]*LockItem{},
			Lock:    &sync.Mutex{},
		}
		lockInfo.Con = sync.NewCond(lockInfo.Lock)
		locks.LockMap[lockId] = lockInfo
	}
	locks.Latch.Unlock()
	ret := lockInfo.AddWaitingTxn(transaction, LockShared)
	if ret {
		transaction.AddLock(lockId)
	}
	return ret
}

func (locks *LockManager) LockExclusive(lockId int32, transaction *Transaction) bool {
	if transaction.GetTransactionLockState() != TransGrowing {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	locks.Latch.Lock()
	lockInfo := locks.LockMap[lockId]
	if lockInfo == nil {
		lockInfo = &LockInfo{
			Holding: map[uint64]*LockItem{},
			Lock:    &sync.Mutex{},
		}
		lockInfo.Con = sync.NewCond(lockInfo.Lock)
		locks.LockMap[lockId] = lockInfo
	}
	locks.Latch.Unlock()
	ret := lockInfo.AddWaitingTxn(transaction, LockExclusive)
	if ret {
		transaction.AddLock(lockId)
	}
	return ret
}

func (locks *LockManager) LockUpgrade(lockId int32, transaction *Transaction) bool {
	if transaction.GetTransactionLockState() != TransGrowing {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	locks.Latch.Lock()
	lockInfo := locks.LockMap[lockId]
	locks.Latch.Unlock()
	if lockInfo == nil {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	item := lockInfo.GetLockItem(transaction.TransactionId)
	if item.Txn == nil {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	lockInfo.RemoveLock(transaction)
	transaction.RemoveLock(lockId)
	ret := lockInfo.AddWaitingTxn(transaction, LockExclusive)
	if ret {
		transaction.AddLock(lockId)
	}
	return ret
}

func (locks *LockManager) Unlock(lockId int32, transaction *Transaction) bool {
	if transaction.GetTransactionLockState() != TransAbort && transaction.GetTransactionLockState() != TransCommit {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	locks.Latch.Lock()
	lockInfo := locks.LockMap[lockId]
	locks.Latch.Unlock()
	lockItem := lockInfo.GetLockItem(transaction.TransactionId)
	if lockItem.Txn == nil {
		transaction.SetTransactionLockState(TransAbort)
		return false
	}
	lockInfo.RemoveLock(transaction)
	transaction.RemoveLock(lockId)
	return true
}

type LockItem struct {
	Txn  *Transaction
	Mode LockMode
}

type LockInfo struct {
	Lock    *sync.Mutex
	Holding map[uint64]*LockItem
	Con     *sync.Cond
}

func (lockInfo *LockInfo) AddWaitingTxn(txn *Transaction, mode LockMode) bool {
	lockInfo.Con.L.Lock()
	defer lockInfo.Con.L.Unlock()
	if lockInfo.AlreadyHold(txn, mode) {
		lockItem := lockInfo.Holding[txn.TransactionId]
		if mode == LockExclusive && lockItem.Mode != mode {
			lockItem.Mode = LockExclusive
		}
		return true
	}
	for {
		if lockInfo.canGrant(txn, mode) {
			break
		} else if lockInfo.WaitDie(txn, mode) {
			txn.SetTransactionLockState(TransAbort)
			return false
		} else {
			lockInfo.Con.Wait()
		}
	}
	lockInfo.Holding[txn.TransactionId] = &LockItem{Txn: txn, Mode: mode}
	return true
}

func (lockInfo *LockInfo) canGrant(txn *Transaction, mode LockMode) bool {
	if len(lockInfo.Holding) == 0 {
		return true
	}
	if mode == LockExclusive {
		if len(lockInfo.Holding) == 1 {
			for _, item := range lockInfo.Holding {
				return item.Txn.TransactionId == txn.TransactionId
			}
		}
		return len(lockInfo.Holding) == 0
	}
	for _, item := range lockInfo.Holding {
		if item.Mode == LockShared {
			return true
		}
	}
	return false
}

func (lockInfo *LockInfo) AlreadyHold(txn *Transaction, mode LockMode) bool {
	for _, item := range lockInfo.Holding {
		if item.Txn.TransactionId == txn.TransactionId && item.Mode == mode {
			return true
		}
		if item.Txn.TransactionId == txn.TransactionId && item.Mode == LockExclusive {
			return true
		}
		if item.Txn.TransactionId == txn.TransactionId && len(lockInfo.Holding) == 1 {
			return true
		}
	}
	return false
}

func (lockInfo *LockInfo) GetLockItem(txnId uint64) *LockItem {
	lockInfo.Lock.Lock()
	defer lockInfo.Lock.Unlock()
	return lockInfo.Holding[txnId]
}

func (lockInfo *LockInfo) RemoveLock(txn *Transaction) {
	lockInfo.Lock.Lock()
	defer lockInfo.Lock.Unlock()
	delete(lockInfo.Holding, txn.TransactionId)
	lockInfo.Con.Signal()
}

func (lockInfo *LockInfo) WaitDie(txn *Transaction, mode LockMode) bool {
	var maxTxn *Transaction
	for _, item := range lockInfo.Holding {
		if maxTxn == nil {
			maxTxn = item.Txn
			continue
		}
		if item.Txn.TransactionId >= maxTxn.TransactionId {
			maxTxn = item.Txn
		}
	}
	return maxTxn.TransactionId < txn.TransactionId
}
