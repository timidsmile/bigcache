package bigcache

import (
	"sync"
)

type iteratorError string

func (e iteratorError) Error() string {
	return string(e)
}

// ErrInvalidIteratorState is reported when iterator is in invalid state
const ErrInvalidIteratorState = iteratorError("Iterator is in invalid state. Use SetNext() to move to next position")

// ErrCannotRetrieveEntry is reported when entry cannot be retrieved from underlying
const ErrCannotRetrieveEntry = iteratorError("Could not retrieve entry from cache")

var emptyEntryInfo = EntryInfo{}

// EntryInfo holds informations about entry in the cache
// 就是我们缓存中的key
type EntryInfo struct {
	// 存储该元素时的时间
	timestamp uint64
	// 该元素的key使用 hash 算法后得到的 uint64
	hash uint64
	// 原始的key 字符串
	key string
	// key 对应的元素值
	value []byte
	// 所有过程中可能产生的error
	err error
}

// Key returns entry's underlying key
func (e EntryInfo) Key() string {
	return e.key
}

// Hash returns entry's hash value
func (e EntryInfo) Hash() uint64 {
	return e.hash
}

// Timestamp returns entry's timestamp (time of insertion)
func (e EntryInfo) Timestamp() uint64 {
	return e.timestamp
}

// Value returns entry's underlying value
func (e EntryInfo) Value() []byte {
	return e.value
}

// EntryInfoIterator allows to iterate over entries in the cache
type EntryInfoIterator struct {
	mutex        sync.Mutex
	cache        *BigCache
	currentShard int
	// 迭代器
	currentIndex     int
	currentEntryInfo EntryInfo
	elements         []uint64
	elementsCount    int
	valid            bool
}

// SetNext moves to next element and returns true if it exists.
func (it *EntryInfoIterator) SetNext() bool {
	it.mutex.Lock()

	it.valid = false
	it.currentIndex++

	if it.elementsCount > it.currentIndex {
		it.valid = true

		empty := it.setCurrentEntry()
		it.mutex.Unlock()

		if empty {
			return it.SetNext()
		}
		return true
	}

	for i := it.currentShard + 1; i < it.cache.config.Shards; i++ {
		it.elements, it.elementsCount = it.cache.shards[i].copyHashedKeys()

		// Non empty shard - stick with it
		if it.elementsCount > 0 {
			it.currentIndex = 0
			it.currentShard = i
			it.valid = true

			empty := it.setCurrentEntry()
			it.mutex.Unlock()

			if empty {
				return it.SetNext()
			}
			return true
		}
	}
	it.mutex.Unlock()
	return false
}

func (it *EntryInfoIterator) setCurrentEntry() bool {
	var entryNotFound = false
	entry, err := it.cache.shards[it.currentShard].getEntry(it.elements[it.currentIndex])

	if err == ErrEntryNotFound {
		it.currentEntryInfo = emptyEntryInfo
		entryNotFound = true
	} else if err != nil {
		it.currentEntryInfo = EntryInfo{
			err: err,
		}
	} else {
		it.currentEntryInfo = EntryInfo{
			timestamp: readTimestampFromEntry(entry),
			hash:      readHashFromEntry(entry),
			key:       readKeyFromEntry(entry),
			value:     readEntry(entry),
			err:       err,
		}
	}

	return entryNotFound
}

func newIterator(cache *BigCache) *EntryInfoIterator {
	elements, count := cache.shards[0].copyHashedKeys()

	return &EntryInfoIterator{
		cache:         cache,
		currentShard:  0,
		currentIndex:  -1,
		elements:      elements,
		elementsCount: count,
	}
}

// Value returns current value from the iterator
func (it *EntryInfoIterator) Value() (EntryInfo, error) {
	if !it.valid {
		return emptyEntryInfo, ErrInvalidIteratorState
	}

	return it.currentEntryInfo, it.currentEntryInfo.err
}
