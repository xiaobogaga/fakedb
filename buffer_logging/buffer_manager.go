package buffer_logging

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xiaobogaga/fakedb/util"
	"sync"
	"time"
)

var bufManagerLog = util.GetLog("bufManager")

type BufferManager struct {
	Data           map[int32]*Page
	Lock           sync.Mutex
	Disk           *DiskManager
	DirtyPageTable map[int32]*Page
	Ctx            context.Context
	FlushDuration  time.Duration
}

func NewBufferManager(ctx context.Context, dataFile string, flushDuration time.Duration) (*BufferManager, error) {
	diskManager, err := NewDiskManager(dataFile)
	if err != nil {
		return nil, err
	}
	return &BufferManager{
		Data:           map[int32]*Page{},
		Lock:           sync.Mutex{},
		Disk:           diskManager,
		DirtyPageTable: map[int32]*Page{},
		Ctx:            ctx,
		FlushDuration:  flushDuration,
	}, nil
}

type Page struct {
	Dirty   bool
	Key     []byte
	Value   []byte
	LSN     int64
	RecvLsn int64 // recovery lsn. won't save to Disk.
}

// +----------+------------+
// + page len + page bytes +
// +----------+------------+
//            + lsn + key len + key bytes + value len + value bytes
func (page *Page) Serialize() []byte {
	ret := make([]byte, page.Len())
	binary.BigEndian.PutUint64(ret, uint64(page.LSN))
	binary.BigEndian.PutUint32(ret[8:], uint32(len(page.Key)))
	copy(ret[12:], page.Key)
	binary.BigEndian.PutUint32(ret[12+len(page.Key):], uint32(len(page.Value)))
	copy(ret[16+len(page.Key):], page.Value)
	return ret
}

var damagedPacket = errors.New("damaged packet")

func (page *Page) Deserialize(data []byte) error {
	len := len(data)
	if len < 16 {
		return damagedPacket
	}
	page.LSN = int64(binary.BigEndian.Uint64(data))
	keyLen := binary.BigEndian.Uint32(data[8:])
	if uint32(len) < 16+keyLen {
		return damagedPacket
	}
	page.Key = data[12 : keyLen+12]
	valueLen := binary.BigEndian.Uint32(data[12+keyLen:])
	if uint32(len) < 16+keyLen+valueLen {
		return damagedPacket
	}
	page.Value = data[16+keyLen : 16+keyLen+valueLen]
	return nil
}

func (page *Page) Len() uint32 {
	return uint32(8 + 4 + len(page.Key) + 4 + len(page.Value)) // page len + key len + key bytes + value len + value bytes + lsn len
}

// Return whether found the key, the value of the key if found. error.
func (buffer *BufferManager) Get(id int32, key []byte) (bool, []byte, error) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	_, ok := buffer.Data[id]
	if !ok {
		// Load Data from Disk
		page, err := buffer.Disk.Read(id)
		if err != nil {
			return false, nil, err
		}
		if page == nil {
			return false, nil, nil
		}
		page.RecvLsn = InvalidLsn
		buffer.Data[id] = page
	}
	v := buffer.Data[id]
	return bytes.Compare(v.Key, key) == 0, v.Value, nil
}

func (buffer *BufferManager) Set(id int32, key, value []byte, lsn int64) error {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, ok := buffer.Data[id]
	if !ok {
		page = &Page{Key: key, Value: value, Dirty: true, LSN: lsn, RecvLsn: lsn}
		buffer.Data[id] = page
		buffer.DirtyPageTable[id] = page
		return nil
	}
	if page.RecvLsn == InvalidLsn {
		page.RecvLsn = lsn
	}
	page.LSN = lsn
	page.Key = key
	page.Value = value
	buffer.DirtyPageTable[id] = page
	return nil
}

func (buffer *BufferManager) Del(id int32, lsn int64) (err error) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, ok := buffer.Data[id]
	if !ok {
		page, err = buffer.Disk.Read(id)
		if err != nil {
			return err
		}
		if page == nil {
			bufManagerLog.InfoF("cannot find such page to del")
			return nil
		}
		page.RecvLsn = lsn
		buffer.Data[id] = page
	}
	page.Dirty = true
	if page.RecvLsn == InvalidLsn {
		page.RecvLsn = lsn
	}
	page.LSN = lsn
	page.Key = nil
	page.Value = nil
	buffer.DirtyPageTable[id] = page
	return nil
}

func (buffer *BufferManager) GetPage(id int32) (*Page, error) {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, ok := buffer.Data[id]
	if ok {
		return page, nil
	}
	page, err := buffer.Disk.Read(id)
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, nil
	}
	buffer.Data[id] = page
	page.RecvLsn = InvalidLsn
	return page, nil
}

func (buffer *BufferManager) Size() int {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	ret := 0
	for _, data := range buffer.Data {
		if len(data.Key) == 0 && len(data.Value) == 0 {
			continue
		}
		ret++
	}
	return ret
}

func (buffer *BufferManager) IsEmpty(id int32) bool {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	page, ok := buffer.Data[id]
	return !ok || (len(page.Key) == 0 && len(page.Value) == 0)
}

func (buffer *BufferManager) FlushDirtyPagesRegularly(logManager *LogManager) {
	bufManagerLog.InfoF("start flush dirty page goroutine")
	defer bufManagerLog.InfoF("end flush dirty page goroutine")
	for {
		select {
		case <-time.After(buffer.FlushDuration):
		case <-buffer.Ctx.Done():
			return
		}
		buffer.Lock.Lock()
		for id, page := range buffer.DirtyPageTable {
			if page.LSN <= logManager.GetFlushedLsn() {
				bufManagerLog.InfoF("write dirty page: lsn: %d, key: %s, value: %s", page.LSN, string(page.Key), string(page.Value))
				err := buffer.Disk.Write(id, page)
				if err != nil {
					panic(err)
				}
				delete(buffer.DirtyPageTable, id)
			}
		}
		buffer.Lock.Unlock()
	}
}

type DirtyPageRecord struct {
	PageId int32
	RevLSN int64
}

func (record *DirtyPageRecord) Serialize() []byte {
	ret := make([]byte, 12)
	binary.BigEndian.PutUint32(ret, uint32(record.PageId))
	binary.BigEndian.PutUint64(ret[4:], uint64(record.RevLSN))
	return ret
}

func (record *DirtyPageRecord) Deserialize(data []byte) error {
	if len(data) < 12 {
		return damagedPacket
	}
	record.PageId = int32(binary.BigEndian.Uint32(data))
	record.RevLSN = int64(binary.BigEndian.Uint64(data[4:]))
	return nil
}

func (record *DirtyPageRecord) Len() int {
	return 12
}

func (record *DirtyPageRecord) String() string {
	return fmt.Sprintf("[pageId: %d, recLSN: %d]", record.PageId, record.RevLSN)
}

func (buffer *BufferManager) DirtyPageRecordTable() []*DirtyPageRecord {
	buffer.Lock.Lock()
	defer buffer.Lock.Unlock()
	var ret []*DirtyPageRecord
	for id, page := range buffer.DirtyPageTable {
		if page.Dirty {
			ret = append(ret, &DirtyPageRecord{PageId: id, RevLSN: page.RecvLsn})
		}
	}
	return ret
}

func (buffer *BufferManager) FlushDirtyPage(logManager *LogManager) {
	buffer.Lock.Lock()
	for id, page := range buffer.DirtyPageTable {
		if page.LSN <= logManager.GetFlushedLsn() {
			bufManagerLog.InfoF("write dirty page: lsn: %d, key: %s, value: %s", page.LSN, string(page.Key), string(page.Value))
			err := buffer.Disk.Write(id, page)
			if err != nil {
				panic(err)
			}
			delete(buffer.DirtyPageTable, id)
		}
	}
	buffer.Lock.Unlock()
}
