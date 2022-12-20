package bigcache

import (
	"encoding/binary"
)

/*
| | | | | | | | | | | | | | | | | |
|<-timestamp->|   <-hash key->|<-原始keySize=5|
| | | | |
原始key->| value |
 */
const (
	// 前8位存储该元素对应的时间戳 对应的bytes
	timestampSizeInBytes = 8                                                       // Number of bytes used for timestamp
	// hash值，就是string转uint64的那个hash。header中的这8个字节存储的就是key
	hashSizeInBytes      = 8                                                       // Number of bytes used for hash
	// key 的长度
	keySizeInBytes       = 2                                                       // Number of bytes used for size of entry key
	headersSizeInBytes   = timestampSizeInBytes + hashSizeInBytes + keySizeInBytes // Number of bytes used for all headers
)

func wrapEntry(timestamp uint64, hash uint64, key string, entry []byte, buffer *[]byte) []byte {
	// 原始的字符串key
	keyLength := len(key)
	// 块大小。=固定header+value的真实大小+一个可变的原始key的长度
	blobLength := len(entry) + headersSizeInBytes + keyLength

	// 如果所需的存储空间大于 buffer 大小，需要重新分配大小
	if blobLength > len(*buffer) {
		*buffer = make([]byte, blobLength)
	}
	blob := *buffer

	// 时间戳放在第一个8字节，就是前64位
	binary.LittleEndian.PutUint64(blob, timestamp)
	// hash 值是固定的长度，也占8个字节，放在第二个64位
	binary.LittleEndian.PutUint64(blob[timestampSizeInBytes:], hash)
	// 第三个用来存储可变的原始字符串的长度
	binary.LittleEndian.PutUint16(blob[timestampSizeInBytes+hashSizeInBytes:], uint16(keyLength))
	// 把原始的key copy到header后
	copy(blob[headersSizeInBytes:], key)
	// 原始的value copy 到key之后
	copy(blob[headersSizeInBytes+keyLength:], entry)

	// 返回的是封装后的块内容
	return blob[:blobLength]
}

func appendToWrappedEntry(timestamp uint64, wrappedEntry []byte, entry []byte, buffer *[]byte) []byte {
	blobLength := len(wrappedEntry) + len(entry)
	if blobLength > len(*buffer) {
		*buffer = make([]byte, blobLength)
	}

	blob := *buffer

	binary.LittleEndian.PutUint64(blob, timestamp)
	copy(blob[timestampSizeInBytes:], wrappedEntry[timestampSizeInBytes:])
	copy(blob[len(wrappedEntry):], entry)

	return blob[:blobLength]
}

func readEntry(data []byte) []byte {
	// key 占用的位数
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	// copy on read
	// header后存储的是key，key后存储的就是原始value
	// 而 key 的长度就是第17 18位表示的值
	dst := make([]byte, len(data)-int(headersSizeInBytes+length))
	copy(dst, data[headersSizeInBytes+length:])

	return dst
}

// 前8位是时间戳 Uint64 就正好是处理一个字节
func readTimestampFromEntry(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func readKeyFromEntry(data []byte) string {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	// copy on read
	dst := make([]byte, length)
	copy(dst, data[headersSizeInBytes:headersSizeInBytes+length])

	return bytesToString(dst)
}

func compareKeyFromEntry(data []byte, key string) bool {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	return bytesToString(data[headersSizeInBytes:headersSizeInBytes+length]) == key
}

func readHashFromEntry(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[timestampSizeInBytes:])
}

// 删除元素仅仅是将[]byte 中存放元素的block 的header的前8个字节标记为0了
func resetKeyFromEntry(data []byte) {
	binary.LittleEndian.PutUint64(data[timestampSizeInBytes:], 0)
}
