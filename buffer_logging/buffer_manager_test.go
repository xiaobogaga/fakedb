package buffer_logging

import (
	"context"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestDirtyPageRecord_Serialize(t *testing.T) {
	dirtyRecord := &DirtyPageRecord{PageId: 1, RevLSN: 1}
	data := dirtyRecord.Serialize()
	assert.Equal(t, dirtyRecord.Len(), len(data))
	another := &DirtyPageRecord{}
	err := another.Deserialize(data)
	assert.Nil(t, err)
	assert.Equal(t, dirtyRecord, another)
	assert.NotNil(t, another.Deserialize(data[1:10]))
}

func TestPage_Deserialize(t *testing.T) {
	page := &Page{
		Key:   []byte{1},
		Value: []byte{2},
		LSN:   int64(1),
	}
	data := page.Serialize()
	assert.Equal(t, int(page.Len()), len(data))
	another := &Page{}
	err := another.Deserialize(data)
	assert.Nil(t, err)
	assert.Equal(t, page, another)
	assert.NotNil(t, another.Deserialize(data[1:2]))
}

func TestBufferManager_Get(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "fakedb.")
	assert.Nil(t, err)
	bufManager, err := NewBufferManager(context.Background(), f.Name(), time.Second*10)
	assert.Nil(t, err)
	err = bufManager.Set(1, []byte{1}, []byte{1}, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, bufManager.Size())
	assert.False(t, bufManager.IsEmpty(1))
	found, value, err := bufManager.Get(1, []byte{1})
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte{1}, value)
	bufManager.Del(1, 1)
	assert.Equal(t, 0, bufManager.Size())
	found, _, err = bufManager.Get(1, []byte{1})
	assert.Nil(t, err)
	assert.False(t, found)
	err = os.Remove(f.Name())
	assert.Nil(t, err)
}

func TestBufferManager_FlushDirtyPagesRegularly(t *testing.T) {
	// Todo
}
