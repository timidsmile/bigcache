package bigcache

import "time"

// Config for BigCache
type Config struct {
	// Number of cache shards, value must be a power of two
	Shards int
	// Time after which entry can be evicted
	// 删除 key 的时间（删除key并不意味着清楚了这个key对应的内存）
	LifeWindow time.Duration
	// Interval between removing expired entries (clean up).
	// If set to <= 0 then no action is performed. Setting to < 1 second is counterproductive — bigcache has a one second resolution.
	// clean up的时间（删除value 实际对应的内存的方案）如果<=0，那么久不会异步的开启clean线程
	CleanWindow time.Duration
	// Max number of entries in life window. Used only to calculate initial size for cache shards.
	// When proper value is set then additional memory allocation does not occur.
	// 支持存储的key的最大数目。这个值仅在初始化cache shards时用到。指定这个值可以避免内存的浪费。
	MaxEntriesInWindow int
	// Max size of entry in bytes. Used only to calculate initial size for cache shards.
	// 支持存储的key的最大内存大小。
	MaxEntrySize int
	// StatsEnabled if true calculate the number of times a cached resource was requested.
	// 开启后，可以用来统计请求命中情况
	StatsEnabled bool
	// Verbose mode prints information about new memory allocation
	Verbose bool
	// Hasher used to map between string keys and unsigned 64bit integers, by default fnv64 hashing is used.
	// key 由string映射到uint64的hash方式，默认使用 fnv ，也可以由业务自己指定
	Hasher Hasher
	// HardMaxCacheSize is a limit for BytesQueue size in MB.
	// It can protect application from consuming all available memory on machine, therefore from running OOM Killer.
	// Default value is 0 which means unlimited size. When the limit is higher than 0 and reached then
	// the oldest entries are overridden for the new ones. The max memory consumption will be bigger than
	// HardMaxCacheSize due to Shards' s additional memory. Every Shard consumes additional memory for map of keys
	// and statistics (map[uint64]uint32) the size of this map is equal to number of entries in
	// cache ~ 2×(64+32)×n bits + overhead or map itself.
	// HardMaxCacheSize 是对 BytesQueue 的限制，这样可以避免总内存使用过大而导致程序oom的问题。
	// 默认值是0,0表示无限制。当该值>0并且内存使用量达到该值时，新的key会覆盖老的key。要注意，最大的内存消耗会大于该值！
	// 因为shard会有一些额外的内存开销(统计数据之类的、key之类的)
	HardMaxCacheSize int
	// OnRemove is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// ignored if OnRemoveWithMetadata is specified.
	// OnRemove是一个回调，当最旧的条目被删除（因为过期或没有空间留给新条目，或因为删除被调用），回调被触发。
	// 如果 OnRemoveWithMetadata被设置了，该值会被忽略。
	OnRemove func(key string, entry []byte)
	// OnRemoveWithMetadata is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A structure representing details about that specific entry.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// 同上
	OnRemoveWithMetadata func(key string, entry []byte, keyMetadata Metadata)
	// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A constant representing the reason will be passed through.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// Ignored if OnRemove is specified.
	OnRemoveWithReason func(key string, entry []byte, reason RemoveReason)

	onRemoveFilter int

	// Logger is a logging interface and used in combination with `Verbose`
	// Defaults to `DefaultLogger()`
	Logger Logger
}

// DefaultConfig initializes config with default values.
// When load for BigCache can be predicted in advance then it is better to use custom config.
func DefaultConfig(eviction time.Duration) Config {
	return Config{
		Shards:             1024,            // 分片设置为1024个
		LifeWindow:         eviction,        // key指定的过期时间
		CleanWindow:        1 * time.Second, // 删除key的时间间隔（todo 这个是不是太小了？？）
		MaxEntriesInWindow: 1000 * 10 * 60,  // 最大key数目为 600K
		MaxEntrySize:       500,             // key最大的size
		StatsEnabled:       false,
		Verbose:            true,
		Hasher:             newDefaultHasher(),
		HardMaxCacheSize:   0,
		Logger:             DefaultLogger(),
	}
}

// initialShardSize computes initial shard size
func (c Config) initialShardSize() int {
	// 每个hard 存储的key的数目
	// 第一个值是 总的key的数目/shard的数目
	// 第二个是一个默认值
	return max(c.MaxEntriesInWindow/c.Shards, minimumEntriesInShard)
}

// maximumShardSizeInBytes computes maximum shard size in bytes
func (c Config) maximumShardSizeInBytes() int {
	maxShardSize := 0

	if c.HardMaxCacheSize > 0 {
		maxShardSize = convertMBToBytes(c.HardMaxCacheSize) / c.Shards
	}

	return maxShardSize
}

// OnRemoveFilterSet sets which remove reasons will trigger a call to OnRemoveWithReason.
// Filtering out reasons prevents bigcache from unwrapping them, which saves cpu.
func (c Config) OnRemoveFilterSet(reasons ...RemoveReason) Config {
	c.onRemoveFilter = 0
	for i := range reasons {
		c.onRemoveFilter |= 1 << uint(reasons[i])
	}

	return c
}
