package concurrency

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLockManager_LockShared(t *testing.T) {
	lockManager := NewLockManager()
	txn1 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  1,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	txn2 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  2,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	assert.True(t, lockManager.LockShared(1, txn1))
	assert.True(t, lockManager.LockShared(2, txn2))
	assert.True(t, lockManager.LockShared(1, txn2))
	assert.True(t, lockManager.LockShared(2, txn1))
}

func TestLockManager_LockExclusive(t *testing.T) {
	lockManager := NewLockManager()
	txn1 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  1,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	txn2 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  2,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	assert.True(t, lockManager.LockShared(1, txn1))
	assert.True(t, lockManager.LockExclusive(2, txn2))
	assert.False(t, lockManager.LockExclusive(1, txn2))
	txn2.ReleaseLocks()
	assert.True(t, lockManager.LockExclusive(2, txn1))
}

func TestLockManager_LockUpgrade(t *testing.T) {
	lockManager := NewLockManager()
	txn1 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  1,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	txn2 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  2,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	assert.True(t, lockManager.LockShared(1, txn1))
	assert.True(t, lockManager.LockUpgrade(1, txn1))
	assert.True(t, lockManager.LockExclusive(2, txn2))
	assert.True(t, lockManager.LockUpgrade(2, txn2))
}

func TestLockManager_Unlock(t *testing.T) {
	lockManager := NewLockManager()
	txn1 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  1,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	txn2 := &Transaction{
		TransLockState: TransGrowing,
		TransactionId:  2,
		HoldingLocks:   map[int32]bool{},
		LockManager:    lockManager,
	}
	//txn3 := &Transaction{
	//	TransLockState: TransGrowing,
	//	TransactionId:  3,
	//	HoldingLocks:   map[int32]bool{},
	//	LockManager:    lockManager,
	//}
	assert.True(t, lockManager.LockExclusive(1, txn2))
	done := make(chan struct{})
	go func() {
		lockManager.LockShared(1, txn1)
		close(done)
	}()
	time.Sleep(time.Second * 1)
	txn2.TransLockState = TransCommit
	lockManager.Unlock(1, txn2)
	<-done
}
