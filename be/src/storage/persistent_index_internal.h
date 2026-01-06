// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains all internal implementation classes for the persistent index subsystem.
// Including: types, internal structures, mutable index, and immutable index classes.
//
// Main components:
// - Basic types and enums (IndexValue, KVRef, KeysInfo, etc.)
// - Internal structures (IndexHash, Page structures, etc.)
// - Mutable index classes (MutableIndex and implementations)
// - Immutable index classes (ImmutableIndex, ImmutableIndexShard, ImmutableIndexWriter)
//
// These are implementation details not meant to be used directly by external code.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/persistent_index.pb.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "storage/edit_version.h"
#include "util/fmt.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {

class PrimaryKeyDump;
class WritableFile;
class RandomAccessFile;

// ============================================================================
// SECTION 1: File Version and Enums
// ============================================================================

enum PersistentIndexFileVersion {
    PERSISTENT_INDEX_VERSION_UNKNOWN = 0,
    PERSISTENT_INDEX_VERSION_1,
    PERSISTENT_INDEX_VERSION_2,
    PERSISTENT_INDEX_VERSION_3,
    PERSISTENT_INDEX_VERSION_4,
    PERSISTENT_INDEX_VERSION_5,
    PERSISTENT_INDEX_VERSION_6,
    PERSISTENT_INDEX_VERSION_7
};

enum CommitType {
    kFlush = 0,
    kSnapshot = 1,
    kAppendWAL = 2,
};

// ============================================================================
// SECTION 2: Constants
// ============================================================================

static constexpr uint64_t NullIndexValue = -1;
static std::string MergeSuffix = ".merged";
static std::string BloomFilterSuffix = ".bf";

extern bool write_pindex_bf;

static constexpr size_t kIndexValueSize = 8;
constexpr static size_t kSliceMaxFixLength = 64;

// Page and bucket layout constants
constexpr size_t kDefaultUsagePercent = 85;
constexpr size_t kPageSize = 4096;
constexpr size_t kMaxPerPageSize = 1 << 16;
constexpr size_t kPageHeaderSize = 64;
constexpr size_t kBucketHeaderSize = 4;
constexpr size_t kBucketPerPage = 16;
constexpr size_t kRecordPerBucket = 8;
constexpr size_t kShardMax = 1 << 16;
constexpr uint64_t kPageMaxNum = 1ULL << 16;
constexpr size_t kPackSize = 16;
constexpr size_t kBucketSizeMax = 256;
constexpr size_t kFixedMaxKeySize = 128;
constexpr size_t kBatchBloomFilterReadSize = 4ULL << 20;
constexpr uint32_t kMutableIndexFormatVersion1 = 1;
constexpr uint32_t kMutableIndexFormatVersion2 = 2;
constexpr uint32_t kSnapshotMagicNum = 0xF2345678;

extern const char* const kIndexFileMagic;

// ============================================================================
// SECTION 3: Basic Type Definitions
// ============================================================================

// Use `uint8_t[8]` to store the value of a `uint64_t` to reduce memory cost in phmap
struct IndexValue {
    uint8_t v[8];
    IndexValue() = default;
    explicit IndexValue(const uint64_t val) { UNALIGNED_STORE64(v, val); }

    uint64_t get_value() const { return UNALIGNED_LOAD64(v); }
    uint32_t get_rssid() const { return (uint32_t)(get_value() >> 32); }
    uint32_t get_rowid() const { return (uint32_t)(get_value() & 0xFFFFFFFF); }
    bool operator==(const IndexValue& rhs) const { return memcmp(v, rhs.v, 8) == 0; }
    void operator=(uint64_t rhs) { return UNALIGNED_STORE64(v, rhs); }
};

static_assert(sizeof(IndexValue) == kIndexValueSize);

using IndexValueWithVer = std::pair<int64_t, IndexValue>;

// Hash function for index keys
uint64_t key_index_hash(const void* data, size_t len);

// Key information: (shard_id, hash)
using KeyInfo = std::pair<uint32_t, uint64_t>;

struct KeysInfo {
    std::vector<KeyInfo> key_infos;
    size_t size() const { return key_infos.size(); }
    void set_difference(KeysInfo& input);
};

// Reference to a key-value pair
struct KVRef {
    const uint8_t* kv_pos;
    uint64_t hash;
    uint16_t size;
    KVRef() = default;
    KVRef(const uint8_t* kv_pos, uint64_t hash, uint16_t size) : kv_pos(kv_pos), hash(hash), size(size) {}
};

// I/O statistics
struct IOStat {
    uint32_t read_iops = 0;
    uint32_t filtered_kv_cnt = 0;
    uint64_t get_in_shard_cost = 0;
    uint64_t read_io_bytes = 0;
    uint64_t l0_write_cost = 0;
    uint64_t l1_l2_read_cost = 0;
    uint64_t flush_or_wal_cost = 0;
    uint64_t compaction_cost = 0;
    uint64_t reload_meta_cost = 0;
    uint64_t total_file_size = 0;

    std::string print_str();
};

// Edit version with merge flag
struct EditVersionWithMerge {
    EditVersionWithMerge(const EditVersion& ver, bool m) : version(ver), merged(m) {}
    EditVersionWithMerge(int64_t major, int64_t minor, bool m) : version(major, minor), merged(m) {}
    bool operator<(const EditVersionWithMerge& rhs) {
        if (version == rhs.version) {
            if (!merged && rhs.merged) {
                return true;
            } else {
                return false;
            }
        } else {
            return version < rhs.version;
        }
    }
    EditVersion version;
    bool merged{false};
};

// ============================================================================
// SECTION 4: Utility Functions
// ============================================================================

// Utility template functions
template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

// Get L0 index file name
static std::string get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major_number(), version.minor_number());
}

// ============================================================================
// SECTION 5: Internal Index Structures
// ============================================================================

using KVPairPtr = const uint8_t*;

// Hash value decomposition for shard, page, bucket, and tag addressing
struct IndexHash {
    IndexHash() = default;
    IndexHash(uint64_t hash) : hash(hash) {}

    uint64_t shard(uint32_t n) const { return (hash >> (63 - n)) >> 1; }
    uint64_t page() const { return (hash >> 16) & 0xffffffff; }
    uint64_t bucket() const { return (hash >> 8) & (kBucketPerPage - 1); }
    uint64_t tag() const { return hash & 0xff; }

    uint64_t hash;
};

// Fixed-size key template
template <size_t KeySize>
struct FixedKey {
    uint8_t data[KeySize];
};

// Equality operator for FixedKey
template <size_t KeySize>
bool operator==(const FixedKey<KeySize>& lhs, const FixedKey<KeySize>& rhs) {
    return memcmp(lhs.data, rhs.data, KeySize) == 0;
}

// Hash functor for FixedKey
template <size_t KeySize>
struct FixedKeyHash {
    uint64_t operator()(const FixedKey<KeySize>& k) const;
};

// Bucket and page structures
struct alignas(4) BucketInfo {
    uint16_t pageid;
    uint8_t packid;
    uint8_t size;
};

struct alignas(kPageHeaderSize) PageHeader {
    BucketInfo buckets[kBucketPerPage];
};

struct alignas(kPageSize) IndexPage {
    uint8_t data[kPageSize];
    PageHeader& header() { return *reinterpret_cast<PageHeader*>(data); }
    uint8_t* pack(uint8_t packid) { return &data[packid * kPackSize]; }
};

struct alignas(kPageSize) LargeIndexPage {
    LargeIndexPage() = default;
    LargeIndexPage(uint32_t npage) : _pages(npage) {}

    void* data() { return _pages.data(); }
    PageHeader& header() { return *reinterpret_cast<PageHeader*>(_pages[0].data); }
    uint8_t* pack(uint8_t packid) {
        uint32_t pack_num = kPageSize / kPackSize;
        uint32_t real_pack_id = packid * _pages.size();
        uint32_t page_id = real_pack_id / pack_num;
        uint32_t packid_in_page = real_pack_id % pack_num;
        return &(_pages[page_id].data[packid_in_page * kPackSize]);
    }

    std::vector<IndexPage> _pages;
};

// Helper structures for bucket movement
struct BucketToMove {
    uint32_t npack = 0;
    uint32_t pageid = 0;
    uint32_t bucketid = 0;
    BucketToMove(uint32_t npack, uint32_t pageid, uint32_t bucketid)
            : npack(npack), pageid(pageid), bucketid(bucketid) {}
    bool operator<(const BucketToMove& rhs) const { return npack < rhs.npack; }
};

struct MoveDest {
    uint32_t npack = 0;
    uint32_t pageid = 0;
    MoveDest(uint32_t npack, uint32_t pageid) : npack(npack), pageid(pageid) {}
    bool operator<(const MoveDest& rhs) const { return npack < rhs.npack; }
};

struct BucketMovement {
    uint32_t src_pageid;
    uint32_t src_bucketid;
    uint32_t dest_pageid;
    BucketMovement(uint32_t src_pageid, uint32_t src_bucketid, uint32_t dest_pageid)
            : src_pageid(src_pageid), src_bucketid(src_bucketid), dest_pageid(dest_pageid) {}
};

// Utility functions for bucket operations
inline size_t num_pack_for_bucket(size_t kv_size, size_t num_kv) {
    return npad(num_kv, kPackSize) + npad(kv_size * num_kv, kPackSize);
}

std::vector<int8_t> get_move_buckets(size_t target, size_t nbucket, const uint8_t* bucket_packs_in_page);

Status find_buckets_to_move(uint32_t pageid, size_t nbucket, size_t min_pack_to_move,
                            const uint8_t* bucket_packs_in_page, std::vector<BucketToMove>* buckets_to_move);

void remove_packs_from_dests(std::vector<MoveDest>& dests, int idx, int npack);

StatusOr<std::vector<BucketMovement>> move_buckets(std::vector<BucketToMove>& buckets_to_move,
                                                   std::vector<MoveDest>& dests);

void copy_kv_to_page(size_t key_size, size_t num_kv, const KVPairPtr* kv_ptrs, const uint8_t* tags,
                     uint8_t* dest_pack, const uint16_t* kv_size);

bool load_bf_or_not();

// ============================================================================
// SECTION 6: ImmutableIndexShard
// ============================================================================

class ImmutableIndexShard {
public:
    ImmutableIndexShard(size_t npage, size_t page_size)
            : _page_size(page_size), _sub_page_num(page_size / kPageSize), _pages(npage * (page_size / kPageSize)) {}

    size_t npage() const { return _pages.size() / _sub_page_num; }
    IndexPage& page(uint32_t pageid) { return _pages[pageid * _sub_page_num]; }
    PageHeader& header(uint32_t pageid) { return _pages[pageid * _sub_page_num].header(); }
    BucketInfo& bucket(uint32_t pageid, uint32_t bucketid) {
        return _pages[pageid * _sub_page_num].header().buckets[bucketid];
    }
    uint8_t* pack_in_page(uint32_t pageid, uint32_t packid) {
        uint32_t pack_id = packid * (_page_size / kPageSize);
        uint32_t pack_num = kPageSize / kPackSize;
        uint32_t pageid_off = pack_id / pack_num;
        uint32_t packid_in_page = pack_id % pack_num;
        return _pages[pageid * _sub_page_num + pageid_off].pack(packid_in_page);
    }
    uint8_t* pack(uint32_t pageid, uint32_t bucketid) {
        auto& info = bucket(pageid, bucketid);
        return pack_in_page(pageid, info.packid);
    }
    void* data() { return _pages.data(); }

    Status write(WritableFile& wb) const;
    Status compress_and_write(const CompressionTypePB& compression_type, WritableFile& wb, size_t* uncompressed_size,
                              std::vector<int32_t>& compressed_pages_off) const;
    Status decompress_pages(const CompressionTypePB& compression_type, uint32_t npage, size_t uncompressed_size,
                            size_t compressed_size, const std::vector<int32_t>& pages_off);

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> try_create(size_t key_size, size_t npage, size_t page_size,
                                                                     size_t nbucket, const std::vector<KVRef>& kv_refs);
    static StatusOr<std::unique_ptr<ImmutableIndexShard>> create(size_t key_size, size_t npage, size_t page_size,
                                                                 size_t nbucket, const std::vector<KVRef>& kv_refs);

public:
    size_t num_entry_moved = 0;

private:
    uint64_t _page_size = 0;
    uint32_t _sub_page_num = 0;
    std::vector<IndexPage> _pages;
};

// ============================================================================
// SECTION 7: MutableIndex Classes
// ============================================================================

class MutableIndex {
public:
    MutableIndex();
    virtual ~MutableIndex();

    virtual Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
                       const std::vector<size_t>& idxes) const = 0;

    virtual Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                          size_t* num_found, const std::vector<size_t>& idxes) = 0;

    virtual Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                          const std::vector<size_t>& idxes) = 0;

    virtual Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) = 0;

    virtual Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                         const std::vector<size_t>& idxes) = 0;

    virtual Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) = 0;

    virtual Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                              std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) = 0;

    virtual Status load_wals(size_t n, const Slice* keys, const IndexValue* values) = 0;
    virtual Status load_snapshot(phmap::BinaryInputArchive& ar) = 0;
    virtual Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) = 0;
    virtual size_t dump_bound() = 0;
    virtual Status dump(phmap::BinaryOutputArchive& ar) = 0;

    virtual std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                                 bool with_null) const = 0;

    virtual Status flush_to_immutable_index(std::unique_ptr<class ImmutableIndexWriter>& writer, size_t nshard,
                                            size_t npage_hint, size_t page_size, size_t nbucket,
                                            bool with_null) const = 0;

    virtual size_t size() const = 0;
    virtual size_t usage() const = 0;
    virtual size_t capacity() = 0;
    virtual void reserve(size_t size) = 0;
    virtual void clear() = 0;
    virtual size_t memory_usage() = 0;
    virtual Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) = 0;
    virtual void set_mutable_index_format_version(uint32_t ver) = 0;
    virtual Status completeness_check(phmap::BinaryInputArchive& ar) = 0;

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size);
    static std::tuple<size_t, size_t, size_t> estimate_nshard_and_npage(const size_t total_kv_pairs_usage,
                                                                        const size_t total_kv_num);
    static size_t estimate_nbucket(size_t key_size, size_t size, size_t nshard, size_t npage);
};

class ShardByLengthMutableIndex {
public:
    ShardByLengthMutableIndex() = default;
    ShardByLengthMutableIndex(const size_t key_size, const std::string& path)
            : _fixed_key_size(key_size), _path(path) {}
    ~ShardByLengthMutableIndex();

    Status init();
    uint64_t file_size();
    Status get(size_t n, const Slice* keys, IndexValue* values, size_t* num_found,
               std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values, size_t* num_found,
                  std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, size_t* num_found,
                  std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);
    Status insert(size_t n, const Slice* keys, const IndexValue* values, std::set<size_t>& check_l1_key_sizes);
    Status erase(size_t n, const Slice* keys, IndexValue* old_values, size_t* num_found,
                 std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);
    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes);
    Status append_wal(size_t n, const Slice* keys, const IndexValue* values);
    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes);
    Status load_snapshot(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& dumped_shard_idxes);
    Status load(const MutableIndexMetaPB& meta);
    size_t dump_bound();
    Status dump(phmap::BinaryOutputArchive& ar, std::set<uint32_t>& dumped_shard_idxes);
    Status commit(MutableIndexMetaPB* meta, const EditVersion& version, const CommitType& type);
    std::vector<std::pair<uint32_t, std::vector<std::vector<KVRef>>>> get_kv_refs_by_shard(size_t num_entry,
                                                                                           bool with_null);
    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys, size_t idx_begin,
                                                         size_t idx_end);
    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys,
                                                         const std::vector<size_t>& idxes);
    Status flush_to_immutable_index(const std::string& dir, const EditVersion& version, bool write_tmp_l1,
                                    bool keep_delete);
    size_t size();
    size_t capacity();
    size_t memory_usage();
    void clear();
    Status create_index_file(std::string& path);
    static StatusOr<std::unique_ptr<ShardByLengthMutableIndex>> create(size_t key_size, const std::string& path);
    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb);
    Status check_snapshot_file(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& idxes);

private:
    friend class PersistentIndex;
    friend class starrocks::lake::LakeLocalPersistentIndex;

    template <int N>
    void _init_loop_helper();

private:
    uint32_t _fixed_key_size = -1;
    uint64_t _offset = 0;
    uint64_t _page_size = 0;
    uint32_t _checksum = 0;
    std::string _path;
    std::unique_ptr<WritableFile> _index_file;
    std::shared_ptr<FileSystem> _fs;
    std::vector<std::unique_ptr<MutableIndex>> _shards;
    std::map<uint32_t, std::pair<uint32_t, uint32_t>> _shard_info_by_key_size;
};

// ============================================================================
// SECTION 8: ImmutableIndex and ImmutableIndexWriter
// ============================================================================

class ImmutableIndex {
public:
    struct ShardInfo {
        size_t size = 0;
        size_t npage = 0;
        size_t nbucket = 0;
        uint64_t offset = 0;
        uint64_t bytes = 0;
        size_t data_size = 0;
        uint32_t key_size = 0;
        uint64_t bf_size = 0;
        uint32_t checksum = 0;
        CompressionTypePB compress_type = NO_COMPRESSION;
        std::vector<int32_t> pages_off;
        size_t uncompressed_size = 0;
        std::unique_ptr<BloomFilter> bf;
    };

    Status get(size_t n, const Slice* keys, KeysInfo& keys_info, IndexValue* values, KeysInfo* found_keys_info,
               size_t key_size, IOStat* stat = nullptr);
    Status check_not_exist(size_t n, const Slice* keys, size_t key_size);
    uint64_t file_size();
    void clear();
    void destroy();
    size_t total_usage();
    size_t total_size();
    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb);
    static StatusOr<std::unique_ptr<ImmutableIndex>> load(std::unique_ptr<RandomAccessFile>&& file);

private:
    friend class ImmutableIndexWriter;
    friend class PersistentIndex;

    Status _get_fixlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_off,
                                     const uint32_t shard_idx);
    Status _get_varlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_off,
                                     const uint32_t shard_idx);
    Status _get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_off, size_t shard_size,
                              size_t key_size);
    Status _get_in_fixlen_shard(size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values,
                                KeysInfo* found_keys_info, const ShardInfo& shard, std::unique_ptr<char[]>& page_buffer,
                                IOStat* stat);
    Status _get_in_varlen_shard(size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values,
                                KeysInfo* found_keys_info, const ShardInfo& shard, std::unique_ptr<char[]>& page_buffer,
                                IOStat* stat);
    Status _get_in_shard(size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values,
                         KeysInfo* found_keys_info, const ShardInfo& shard, IOStat* stat);
    Status _read_page(const ShardInfo& shard_info, RandomAccessFile* read_file, uint32_t pageid, size_t page_size,
                      char* page_buffer);
    void _split_keys_info_by_page(const ShardInfo& shard, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                  std::vector<KeysInfo>& keys_info_by_pages);
    Status _get_in_shard_by_page(size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values,
                                 KeysInfo* found_keys_info, const ShardInfo& shard, IOStat* stat);
    Status _prepare_bloom_filter(const ShardInfo& shard);
    bool _filter(const ShardInfo& shard_info, const Slice& key);
    bool _need_bloom_filter(const ShardInfo& shard_info);

private:
    EditVersion _version;
    std::unique_ptr<RandomAccessFile> _file;
    std::vector<ShardInfo> _shards;
};

class ImmutableIndexWriter {
public:
    ImmutableIndexWriter() = default;
    ~ImmutableIndexWriter();

    Status init(const string& idx_file_path, const EditVersion& version, bool sync_on_close);
    Status write_shard(size_t key_size, size_t npage, size_t nbucket, const std::vector<KVRef>& kvs);
    Status write_bf(std::unique_ptr<BloomFilter>& bf);
    Status finish();

    size_t total_kv_size() { return _total_kv_size; }
    uint64_t file_size() { return _total; }
    bool bf_flushed() { return _bf_flushed; }
    std::shared_ptr<FileSystem> fs() { return _fs; }
    std::vector<ImmutableIndex::ShardInfo>& shards() { return _shards; }

private:
    friend class ImmutableIndex;
    friend class PersistentIndex;

    EditVersion _version;
    bool _bf_flushed = false;
    std::string _idx_file_path;
    std::string _idx_file_path_tmp;
    std::string _bf_file_path;
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<WritableFile> _idx_wb;
    std::unique_ptr<WritableFile> _bf_wb;
    std::vector<ImmutableIndex::ShardInfo> _shards;
    uint64_t _total = 0;
    uint64_t _total_kv_size = 0;
    uint64_t _total_bf_size = 0;
};

} // namespace starrocks
