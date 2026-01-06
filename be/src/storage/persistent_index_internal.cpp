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

#include "storage/persistent_index_internal.h"

#include <cstring>
#include <numeric>
#include <utility>

#include "common/compiler_util.h"
#include "common/config.h"
#include "fs/fs.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/substitute.h"
#include "io/io_profiler.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/persistent_index_parallel_publish_context.h"
#include "storage/primary_key_dump.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "testutil/sync_point.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/compression/block_compression.h"
#include "util/crc32c.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/raw_container.h"
#include "util/stopwatch.hpp"
#include "util/xxh3.h"

namespace starrocks {
namespace starrocks {

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
// The introduction of this magic number serves two purposes:
// 1. To detect endianness mismatches in cross-platform scenarios
// 2. To identify the new snapshot encoding format
constexpr uint32_t kSnapshotMagicNum = 0xF2345678;

const char* const kIndexFileMagic = "IDX1";

bool write_pindex_bf = true;

using KVPairPtr = const uint8_t*;

template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

static std::string get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major_number(), version.minor_number());
}

struct IndexHash {
    IndexHash() = default;
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t shard(uint32_t n) const { return (hash >> (63 - n)) >> 1; }
    uint64_t page() const { return (hash >> 16) & 0xffffffff; }
    uint64_t bucket() const { return (hash >> 8) & (kBucketPerPage - 1); }
    uint64_t tag() const { return hash & 0xff; }

    uint64_t hash;
};

MutableIndex::MutableIndex() = default;

MutableIndex::~MutableIndex() = default;

template <size_t KeySize>
struct FixedKey {
    uint8_t data[KeySize];
};

template <size_t KeySize>
bool operator==(const FixedKey<KeySize>& lhs, const FixedKey<KeySize>& rhs) {
    return memcmp(lhs.data, rhs.data, KeySize) == 0;
}

template <size_t KeySize>
struct FixedKeyHash {
    uint64_t operator()(const FixedKey<KeySize>& k) const { return XXH3_64bits(k.data, KeySize); }
};

uint64_t key_index_hash(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}

// Page storage layout:
//   each page has 4096 / 16 = 256 packs, ie
//   |--------       4096 byte page             -------|
//   |16b pack0|16b pack0| ... |16b pack254|16b pack255|
//   | header  |       data for buckets                |
// Header layout
//   |BucketInfo0|BucketInfo1|...|BucketInfo14|BucketInfo15|
// Bucket data layout
//   | tags (16byte aligned) | kv0,kv1..,kvn (16 byte aligned) |
struct alignas(4) BucketInfo {
    uint16_t pageid;
    // bucket position as pack id
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

// the pageid in the following function are all logic pageid in shard
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

Status ImmutableIndexShard::write(WritableFile& wb) const {
    if (_pages.size() > 0) {
        return wb.append(Slice((uint8_t*)_pages.data(), kPageSize * _pages.size()));
    } else {
        return Status::OK();
    }
}

Status ImmutableIndexShard::compress_and_write(const CompressionTypePB& compression_type, WritableFile& wb,
                                               size_t* uncompressed_size,
                                               std::vector<int32_t>& compressed_pages_off) const {
    if (compression_type == CompressionTypePB::NO_COMPRESSION) {
        return write(wb);
    }

    if (npage() > 0) {
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
        int32_t offset = 0;
        faststring compressed_body;
        for (int32_t i = 0; i < npage(); i++) {
            compressed_body.resize(codec->max_compressed_len(_page_size));
            Slice input((uint8_t*)_pages.data() + i * _page_size, _page_size);
            *uncompressed_size += input.get_size();
            Slice compressed_slice(compressed_body);
            RETURN_IF_ERROR(codec->compress(input, &compressed_slice));
            RETURN_IF_ERROR(wb.append(compressed_slice));
            compressed_pages_off[i] = offset;
            offset += compressed_slice.get_size();
        }
        compressed_pages_off[npage()] = offset;
        return Status::OK();
    } else {
        return Status::OK();
    }
}

Status ImmutableIndexShard::decompress_pages(const CompressionTypePB& compression_type, uint32_t npage,
                                             size_t uncompressed_size, size_t compressed_size,
                                             const std::vector<int32_t>& pages_off) {
    if (uncompressed_size == 0) {
        // No compression
        return Status::OK();
    }

    if (_page_size * npage != uncompressed_size || _pages.size() != npage * (_page_size / kPageSize)) {
        return Status::Corruption(
                fmt::format("invalid uncompressed shared size, {} / {}", _page_size * npage, uncompressed_size));
    }
    // if element in pages are all 0, the pindex file is generated in old file and compressed by page, so we need
    // to decompress it by shard
    if (pages_off.back() > 0) {
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
        std::vector<IndexPage> uncompressed_pages(npage * (_page_size) / kPageSize);
        for (int i = 0; i < npage; i++) {
            Slice compressed_body((uint8_t*)_pages.data() + pages_off[i], pages_off[i + 1] - pages_off[i]);
            Slice decompressed_body((uint8_t*)uncompressed_pages.data() + i * _page_size, _page_size);
            RETURN_IF_ERROR(codec->decompress(compressed_body, &decompressed_body));
        }
        _pages.swap(uncompressed_pages);
    } else {
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
        Slice compressed_body((uint8_t*)_pages.data(), compressed_size);
        std::vector<IndexPage> uncompressed_pages(npage * (_page_size) / kPageSize);
        Slice decompressed_body((uint8_t*)uncompressed_pages.data(), uncompressed_size);
        RETURN_IF_ERROR(codec->decompress(compressed_body, &decompressed_body));
        _pages.swap(uncompressed_pages);
    }
    return Status::OK();
}

inline size_t num_pack_for_bucket(size_t kv_size, size_t num_kv) {
    return npad(num_kv, kPackSize) + npad(kv_size * num_kv, kPackSize);
}

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

static std::vector<int8_t> get_move_buckets(size_t target, size_t nbucket, const uint8_t* bucket_packs_in_page) {
    vector<int8_t> idxes;
    idxes.reserve(nbucket);
    int32_t total_buckets = 0;
    for (int8_t i = 0; i < nbucket; i++) {
        if (bucket_packs_in_page[i] > 0) {
            idxes.push_back(i);
        }
        total_buckets += bucket_packs_in_page[i];
    }
    std::sort(idxes.begin(), idxes.end(),
              [&](int8_t lhs, int8_t rhs) { return bucket_packs_in_page[lhs] < bucket_packs_in_page[rhs]; });
    // store idx if this sum value uses bucket_packs_in_page[idx], or -1
    std::vector<int8_t> dp(total_buckets + 1, -1);
    dp[0] = nbucket;                   // assign an id that will never be used but >= 0
    int32_t valid_sum = total_buckets; // total_buckets is already a valid solution
    auto get_list_from_dp = [&] {
        vector<int8_t> ret;
        ret.reserve(16);
        while (valid_sum > 0) {
            ret.emplace_back(dp[valid_sum]);
            valid_sum -= bucket_packs_in_page[dp[valid_sum]];
        }
        return ret;
    };
    int32_t max_sum = 0; // current max sum
    for (signed char i : idxes) {
        for (int32_t v = 0; v <= max_sum; v++) {
            if (dp[v] < 0 || dp[v] == i) {
                continue;
            }
            int32_t nv = v + bucket_packs_in_page[i];
            if (dp[nv] >= 0) {
                continue;
            }
            dp[nv] = i;
            if (nv > max_sum) {
                max_sum = nv;
            }
            if (nv >= target) {
                valid_sum = std::min(valid_sum, nv);
                if (valid_sum == target) {
                    return get_list_from_dp();
                }
            }
        }
    }
    return get_list_from_dp();
}

static Status find_buckets_to_move(uint32_t pageid, size_t nbucket, size_t min_pack_to_move,
                                   const uint8_t* bucket_packs_in_page, std::vector<BucketToMove>* buckets_to_move) {
    auto ret = get_move_buckets(min_pack_to_move, nbucket, bucket_packs_in_page);

    size_t move_packs = 0;
    for (signed char& i : ret) {
        buckets_to_move->emplace_back(bucket_packs_in_page[i], pageid, i);
        move_packs += bucket_packs_in_page[i];
    }
    DCHECK(move_packs >= min_pack_to_move);

    return Status::OK();
}

struct BucketMovement {
    uint32_t src_pageid;
    uint32_t src_bucketid;
    uint32_t dest_pageid;
    BucketMovement(uint32_t src_pageid, uint32_t src_bucketid, uint32_t dest_pageid)
            : src_pageid(src_pageid), src_bucketid(src_bucketid), dest_pageid(dest_pageid) {}
};

static void remove_packs_from_dests(std::vector<MoveDest>& dests, int idx, int npack) {
    auto& d = dests[idx];
    d.npack -= npack;
    if (d.npack == 0) {
        dests.erase(dests.begin() + idx);
    } else {
        auto mv_start = std::upper_bound(dests.begin(), dests.begin() + idx, dests[idx]) - dests.begin();
        if (mv_start < idx) {
            MoveDest tmp = dests[idx];
            for (long cur = idx; cur > mv_start; cur--) {
                dests[cur] = dests[cur - 1];
            }
            dests[mv_start] = tmp;
        }
    }
}

static StatusOr<std::vector<BucketMovement>> move_buckets(std::vector<BucketToMove>& buckets_to_move,
                                                          std::vector<MoveDest>& dests) {
    std::vector<BucketMovement> ret;
    std::sort(buckets_to_move.begin(), buckets_to_move.end());
    std::sort(dests.begin(), dests.end());
    // move largest bucket first
    for (ssize_t i = buckets_to_move.size() - 1; i >= 0; i--) {
        auto& src = buckets_to_move[i];
        auto pos = std::lower_bound(dests.begin(), dests.end(), src.npack,
                                    [](const MoveDest& lhs, const uint32_t& rhs) { return lhs.npack < rhs; });
        if (pos == dests.end()) {
            return Status::InternalError("move_buckets failed");
        }
        auto idx = pos - dests.begin();
        auto& dest = dests[idx];
        ret.emplace_back(src.pageid, src.bucketid, dest.pageid);
        remove_packs_from_dests(dests, idx, src.npack);
    }
    return std::move(ret);
}

static void copy_kv_to_page(size_t key_size, size_t num_kv, const KVPairPtr* kv_ptrs, const uint8_t* tags,
                            uint8_t* dest_pack, const uint16_t* kv_size) {
    uint8_t* tags_dest = dest_pack;
    size_t tags_len = pad(num_kv, kPackSize);
    memcpy(tags_dest, tags, num_kv);
    memset(tags_dest + num_kv, 0, tags_len - num_kv);
    uint8_t* kvs_dest = dest_pack + tags_len;
    uint16_t offset = tags_len + (num_kv + 1) * sizeof(uint16_t);
    if (key_size == 0) {
        for (size_t i = 0; i < num_kv; i++) {
            encode_fixed16_le(kvs_dest, offset);
            kvs_dest += sizeof(uint16_t);
            offset += kv_size[i];
        }
        encode_fixed16_le(kvs_dest, offset);
        kvs_dest += sizeof(uint16_t);
    }
    for (size_t i = 0; i < num_kv; i++) {
        memcpy(kvs_dest, kv_ptrs[i], kv_size[i]);
        kvs_dest += kv_size[i];
    }
}

static bool load_bf_or_not() {
    return config::enable_pindex_filter && StorageEngine::instance()->update_manager()->keep_pindex_bf();
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::create(size_t key_size, size_t npage_hint,
                                                                           size_t page_size, size_t nbucket,
                                                                           const std::vector<KVRef>& kv_refs) {
    if (kv_refs.size() == 0) {
        return std::make_unique<ImmutableIndexShard>(0, page_size);
    }
    MonotonicStopWatch watch;
    watch.start();
    uint64_t retry_cnt = 0;
    for (size_t npage = npage_hint; npage < kPageMaxNum;) {
        auto rs_create = ImmutableIndexShard::try_create(key_size, npage, page_size, nbucket, kv_refs);
        // increase npage and retry
        if (!rs_create.ok()) {
            // grows at 50%
            npage = npage + npage / 2 + 1;
            retry_cnt++;
            continue;
        }
        if (retry_cnt > 10) {
            LOG(INFO) << "ImmutableIndexShard create cost(ms): " << watch.elapsed_time() / 1000000;
        }
        return std::move(rs_create.value());
    }
    return Status::InternalError("failed to create immutable index shard");
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::try_create(size_t key_size, size_t npage,
                                                                               size_t page_size, size_t nbucket,
                                                                               const std::vector<KVRef>& kv_refs) {
    if (!kv_refs.empty()) {
        // This scenario should not happen in theory, since the usage and size stats by key size is not exactly
        // accurate, so we add this code as a defense
        if (npage == 0) {
            LOG(ERROR) << "find a empty shard with kvs, key size: " << key_size << ", kv_num: " << kv_refs.size();
            npage = 1;
        }
    }
    // the max packid right now is 256
    size_t pack_size = page_size / 256;
    const size_t total_bucket = npage * nbucket;
    std::vector<uint8_t> bucket_sizes(total_bucket);
    std::vector<std::pair<uint32_t, std::vector<uint16_t>>> bucket_data_size(total_bucket);
    std::vector<std::pair<std::vector<KVPairPtr>, std::vector<uint8_t>>> bucket_kv_ptrs_tags(total_bucket);
    size_t estimated_entry_per_bucket = npad(kv_refs.size() * 100 / 85, total_bucket);
    for (auto& [kv_ptrs, tags] : bucket_kv_ptrs_tags) {
        kv_ptrs.reserve(estimated_entry_per_bucket);
        tags.reserve(estimated_entry_per_bucket);
    }
    for (const auto& kv_ref : kv_refs) {
        auto h = IndexHash(kv_ref.hash);
        auto page = h.page() % npage;
        auto bucket = h.bucket() % nbucket;
        auto bid = page * nbucket + bucket;
        auto& sz = bucket_sizes[bid];
        sz++;
        auto& data_size = bucket_data_size[bid].first;
        data_size += kv_ref.size;
        if (pad(sz, kPackSize) + data_size > page_size) {
            return Status::InternalError("bucket size limit exceeded");
        }
        bucket_data_size[bid].second.emplace_back(kv_ref.size);
        bucket_kv_ptrs_tags[bid].first.emplace_back(kv_ref.kv_pos);
        bucket_kv_ptrs_tags[bid].second.emplace_back(h.tag());
    }
    std::vector<uint8_t> bucket_packs(total_bucket);
    size_t page_pack_size_limit = (page_size - kPageHeaderSize) / pack_size;
    for (size_t i = 0; i < total_bucket; i++) {
        auto npack = 0;
        if (key_size != 0) {
            npack = npad(pad((size_t)bucket_sizes[i], kPackSize) + pad(bucket_data_size[i].first, kPackSize),
                         pack_size);
        } else {
            npack = npad(pad((size_t)bucket_sizes[i], kPackSize) +
                                 pad(bucket_data_size[i].first + sizeof(uint16_t) * ((size_t)bucket_sizes[i] + 1),
                                     kPackSize),
                         pack_size);
        }
        if (npack >= page_pack_size_limit) {
            return Status::InternalError("page page limit exceeded");
        }
        bucket_packs[i] = npack;
    }
    // check over-limit pages and reassign some buckets in those pages to under-limit pages
    std::vector<BucketToMove> buckets_to_move;
    std::vector<MoveDest> dests;
    std::vector<bool> page_has_move(npage, false);
    for (uint32_t pageid = 0; pageid < npage; pageid++) {
        const uint8_t* bucket_packs_in_page = &bucket_packs[pageid * nbucket];
        int npack = std::accumulate(bucket_packs_in_page, bucket_packs_in_page + nbucket, 0);
        if (npack < page_pack_size_limit) {
            dests.emplace_back(page_pack_size_limit - npack, pageid);
        } else if (npack > page_pack_size_limit) {
            page_has_move[pageid] = true;
            RETURN_IF_ERROR(find_buckets_to_move(pageid, nbucket, npack - page_pack_size_limit, bucket_packs_in_page,
                                                 &buckets_to_move));
        }
    }
    auto move_rs = move_buckets(buckets_to_move, dests);
    if (!move_rs.ok()) {
        return std::move(move_rs).status();
    }
    auto& moves = move_rs.value();
    auto bucket_moved = [&](uint32_t pageid, uint32_t bucketid) -> bool {
        for (auto& move : moves) {
            if (move.src_pageid == pageid && move.src_bucketid == bucketid) {
                return true;
            }
        }
        return false;
    };
    // calculate bucket positions
    std::unique_ptr<ImmutableIndexShard> ret = std::make_unique<ImmutableIndexShard>(npage, page_size);
    for (auto& move : moves) {
        ret->num_entry_moved += bucket_sizes[move.src_pageid * nbucket + move.src_bucketid];
    }
    for (uint32_t pageid = 0; pageid < npage; pageid++) {
        PageHeader& header = ret->header(pageid);
        size_t cur_packid = npad(nbucket * kBucketHeaderSize, pack_size);
        for (uint32_t bucketid = 0; bucketid < nbucket; bucketid++) {
            if (page_has_move[pageid] && bucket_moved(pageid, bucketid)) {
                continue;
            }
            auto bid = pageid * nbucket + bucketid;
            auto& bucket_info = header.buckets[bucketid];
            bucket_info.pageid = pageid;
            bucket_info.packid = cur_packid;
            bucket_info.size = bucket_sizes[bid];
            copy_kv_to_page(key_size, bucket_info.size, bucket_kv_ptrs_tags[bid].first.data(),
                            bucket_kv_ptrs_tags[bid].second.data(), ret->pack_in_page(pageid, cur_packid),
                            bucket_data_size[bid].second.data());
            cur_packid += bucket_packs[bid];
            DCHECK(cur_packid <= page_size / pack_size);
        }
        for (auto& move : moves) {
            if (move.dest_pageid == pageid) {
                auto bid = move.src_pageid * nbucket + move.src_bucketid;
                auto& bucket_info = ret->bucket(move.src_pageid, move.src_bucketid);
                bucket_info.pageid = pageid;
                bucket_info.packid = cur_packid;
                bucket_info.size = bucket_sizes[bid];
                copy_kv_to_page(key_size, bucket_info.size, bucket_kv_ptrs_tags[bid].first.data(),
                                bucket_kv_ptrs_tags[bid].second.data(), ret->pack_in_page(pageid, cur_packid),
                                bucket_data_size[bid].second.data());
                cur_packid += bucket_packs[bid];
                DCHECK(cur_packid <= page_size / pack_size);
            }
        }
    }
    return std::move(ret);
}

ImmutableIndexWriter::~ImmutableIndexWriter() {
    if (_idx_wb) {
        WARN_IF_ERROR(FileSystem::Default()->delete_file(_idx_file_path_tmp),
                      "Failed to delete file:" + _idx_file_path_tmp);
    }
    if (_bf_wb) {
        WARN_IF_ERROR(FileSystem::Default()->delete_file(_bf_file_path), "Failed to delete file:" + _bf_file_path);
    }
}

Status ImmutableIndexWriter::init(const string& idx_file_path, const EditVersion& version, bool sync_on_close) {
    _version = version;
    _idx_file_path = idx_file_path;
    _idx_file_path_tmp = _idx_file_path + ".tmp";
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_idx_file_path_tmp));
    WritableFileOptions wblock_opts{.sync_on_close = sync_on_close, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_idx_wb, _fs->new_writable_file(wblock_opts, _idx_file_path_tmp));

    _bf_file_path = _idx_file_path + BloomFilterSuffix;
    ASSIGN_OR_RETURN(_bf_wb, _fs->new_writable_file(wblock_opts, _bf_file_path));
    // The minimum unit of compression is shard now, and read on a page-by-page basis is disable after compression.
    if (config::enable_pindex_compression) {
        _meta.set_compression_type(CompressionTypePB::LZ4_FRAME);
    } else {
        _meta.set_compression_type(CompressionTypePB::NO_COMPRESSION);
    }
    return Status::OK();
}

// write_shard() must be called serially in the order of key_size and it is caller's duty to guarantee this.
Status ImmutableIndexWriter::write_shard(size_t key_size, size_t npage_hint, size_t page_size, size_t nbucket,
                                         const std::vector<KVRef>& kvs) {
    const bool new_key_length = _nshard == 0 || _cur_key_size != key_size;
    if (_nshard == 0) {
        _cur_key_size = key_size;
        _cur_value_size = kIndexValueSize;
    } else {
        if (new_key_length) {
            RETURN_ERROR_IF_FALSE(key_size > _cur_key_size, "key size is smaller than before");
        }
        _cur_key_size = key_size;
    }
    if (write_pindex_bf) {
        std::unique_ptr<BloomFilter> bf;
        Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
        if (!st.ok()) {
            LOG(WARNING) << "failed to create bloom filter, status: " << st;
            return st;
        }
        st = bf->init(kvs.size(), 0.05, HASH_MURMUR3_X64_64);
        if (!st.ok()) {
            LOG(WARNING) << "init bloom filter failed, status: " << st;
            return st;
        }
        for (const auto& kv : kvs) {
            bf->add_hash(kv.hash);
        }
        _shard_bf_size.emplace_back(bf->size());
        // update memory usage is too high, flush bloom filter advance to avoid use too much memory
        if (!StorageEngine::instance()->update_manager()->keep_pindex_bf()) {
            for (auto& bf : _bf_vec) {
                RETURN_IF_ERROR(_bf_wb->append(Slice(bf->data(), bf->size())));
            }
            _bf_vec.clear();
            _bf_flushed = true;
        }
        _bf_vec.emplace_back(std::move(bf));
    }

    auto rs_create = ImmutableIndexShard::create(key_size, npage_hint, page_size, nbucket, kvs);
    if (!rs_create.ok()) {
        return std::move(rs_create).status();
    }
    auto& shard = rs_create.value();
    size_t pos_before = _idx_wb->size();
    size_t uncompressed_size = 0;
    std::vector<int32_t> compressed_pages_off(shard->npage() + 1, 0);
    RETURN_IF_ERROR(shard->compress_and_write(static_cast<CompressionTypePB>(_meta.compression_type()), *_idx_wb,
                                              &uncompressed_size, compressed_pages_off));
    size_t pos_after = _idx_wb->size();
    auto shard_meta = _meta.add_shards();
    shard_meta->set_size(kvs.size());
    shard_meta->set_npage(shard->npage());
    shard_meta->set_key_size(key_size);
    shard_meta->set_value_size(kIndexValueSize);
    shard_meta->set_nbucket(nbucket);
    shard_meta->set_uncompressed_size(uncompressed_size);
    shard_meta->set_page_size(page_size);
    for (auto off : compressed_pages_off) {
        shard_meta->mutable_page_off()->Add(off);
    }

    auto ptr_meta = shard_meta->mutable_data();
    ptr_meta->set_offset(pos_before);
    ptr_meta->set_size(pos_after - pos_before);
    _total += kvs.size();
    _total_moved += shard->num_entry_moved;
    size_t shard_kv_size = 0;
    if (key_size != 0) {
        shard_kv_size = (key_size + kIndexValueSize) * kvs.size();
        _total_kv_size += shard_kv_size;
    } else {
        shard_kv_size =
                std::accumulate(kvs.begin(), kvs.end(), (size_t)0, [](size_t s, const auto& e) { return s + e.size; });
        _total_kv_size += shard_kv_size;
    }
    shard_meta->set_data_size(shard_kv_size);
    _total_kv_bytes += pos_after - pos_before;
    auto iter = _shard_info_by_length.find(_cur_key_size);
    if (iter == _shard_info_by_length.end()) {
        if (auto [it, inserted] = _shard_info_by_length.insert({_cur_key_size, {_nshard, 1}}); !inserted) {
            LOG(WARNING) << "insert shard info failed, key_size: " << _cur_key_size
                         << ", maybe duplicate key size which should not happened.";
            return Status::InternalError("insert shard info failed");
        }
    } else {
        iter->second.second++;
    }
    _nshard++;
    return Status::OK();
}

Status ImmutableIndexWriter::write_bf() {
    size_t pos_before = _idx_wb->size();
    VLOG(2) << "write kv size:" << pos_before << ", _bf_wb size: " << _bf_wb->size();
    if (_bf_wb->size() != 0) {
        VLOG(10) << "_bf_wb already write size: " << _bf_wb->size();
        DCHECK(_bf_flushed);
        uint64_t remaining = _bf_wb->size();
        uint64_t offset = 0;
        std::string read_buffer;
        raw::stl_string_resize_uninitialized(&read_buffer, 4096);
        std::unique_ptr<RandomAccessFile> rfile;
        ASSIGN_OR_RETURN(rfile, _fs->new_random_access_file(_bf_file_path));
        while (remaining > 0) {
            if (remaining < 4096) {
                raw::stl_string_resize_uninitialized(&read_buffer, remaining);
            }
            RETURN_IF_ERROR(rfile->read_at_fully(offset, read_buffer.data(), read_buffer.size()));
            RETURN_IF_ERROR(_idx_wb->append(Slice(read_buffer.data(), read_buffer.size())));
            offset += read_buffer.size();
            remaining -= read_buffer.size();
        }
    }
    for (auto& bf : _bf_vec) {
        RETURN_IF_ERROR(_idx_wb->append(Slice(bf->data(), bf->size())));
    }
    _meta.mutable_shard_bf_off()->Add(pos_before);
    for (auto bf_len : _shard_bf_size) {
        _meta.mutable_shard_bf_off()->Add(pos_before + bf_len);
        pos_before += bf_len;
        _total_bf_bytes += bf_len;
    }
    if (pos_before != _idx_wb->size()) {
        std::string err_msg =
                strings::Substitute("immmutable index file size inconsistent. file: $0, expect: $1, actual: $2",
                                    _idx_wb->filename(), pos_before, _idx_wb->size());
        LOG(ERROR) << err_msg;
        return Status::InternalError(err_msg);
    }
    if (_bf_flushed) {
        _bf_vec.clear();
    }
    return Status::OK();
}

Status ImmutableIndexWriter::finish() {
    if (write_pindex_bf) {
        RETURN_IF_ERROR(write_bf());
    }
    VLOG(2) << strings::Substitute(
            "finish writing immutable index $0 #shard:$1 #kv:$2 #moved:$3($4) kv_bytes:$5 usage:$6 bf_bytes:$7 "
            "compression_type:$8",
            _idx_file_path_tmp, _nshard, _total, _total_moved, _total_moved * 1000 / std::max(_total, 1UL) / 1000.0,
            _total_kv_bytes, _total_kv_size * 1000 / std::max(_total_kv_bytes, 1UL) / 1000.0, _total_bf_bytes,
            _meta.compression_type());
    _version.to_pb(_meta.mutable_version());
    _meta.set_size(_total);
    _meta.set_format_version(PERSISTENT_INDEX_VERSION_7);
    for (const auto& [key_size, shard_info] : _shard_info_by_length) {
        const auto [shard_offset, shard_num] = shard_info;
        auto info = _meta.add_shard_info();
        info->set_key_size(key_size);
        info->set_shard_off(shard_offset);
        info->set_shard_num(shard_num);
    }
    std::string footer;
    if (!_meta.SerializeToString(&footer)) {
        return Status::InternalError("ImmutableIndexMetaPB::SerializeToString failed");
    }
    put_fixed32_le(&footer, static_cast<uint32_t>(footer.size()));
    uint32_t checksum = crc32c::Value(footer.data(), footer.size());
    put_fixed32_le(&footer, checksum);
    footer.append(kIndexFileMagic, 4);
    RETURN_IF_ERROR(_idx_wb->append(Slice(footer)));
    RETURN_IF_ERROR(_idx_wb->close());
    RETURN_IF_ERROR(FileSystem::Default()->rename_file(_idx_file_path_tmp, _idx_file_path));
    _idx_wb.reset();
    RETURN_IF_ERROR(_bf_wb->close());
    (void)FileSystem::Default()->delete_file(_bf_file_path);
    _bf_wb.reset();
    return Status::OK();
}

template <size_t KeySize>
class FixedMutableIndex : public MutableIndex {
public:
    using KeyType = FixedKey<KeySize>;
    FixedMutableIndex() = default;
    ~FixedMutableIndex() override = default;

    Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
               const std::vector<size_t>& idxes) const override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                auto iter = _map.find(key, hash);
                if (iter == _map.end()) {
                    values[idx] = NullIndexValue;
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                } else {
                    values[idx] = iter->second;
                    nfound += iter->second.get_value() != NullIndexValue;
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found, const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
                const auto value = values[idx];
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); inserted) {
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                } else {
                    auto old_value = it->second;
                    old_values[idx] = old_value;
                    nfound += old_value.get_value() != NullIndexValue;
                    it->second = value;
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                  const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
                const auto value = values[idx];
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); inserted) {
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                } else {
                    auto old_value = it->second;
                    nfound += old_value.get_value() != NullIndexValue;
                    it->second = value;
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            for (const auto idx : idxes) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
                const auto value = values[idx];
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                    auto old = reinterpret_cast<uint64_t*>(&(it->second));
                    auto old_rssid = (uint32_t)((*old) >> 32);
                    auto old_rowid = (uint32_t)((*old) & ROWID_MASK);
                    auto new_value = reinterpret_cast<uint64_t*>(const_cast<IndexValue*>(&value));
                    std::string msg = strings::Substitute(
                            "FixedMutableIndex<$0> insert found duplicate key, new(rssid=$1 rowid=$2), old(rssid=$3 "
                            "rowid=$4)",
                            KeySize, (uint32_t)((*new_value) >> 32), (uint32_t)((*new_value) & ROWID_MASK), old_rssid,
                            old_rowid);
                    LOG(WARNING) << msg;
                    return Status::AlreadyExist(msg);
                }
            }
        });
        return Status::OK();
    }

    Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                 const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, IndexValue(NullIndexValue)); inserted) {
                    old_values[idx] = NullIndexValue;
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                } else {
                    old_values[idx] = it->second;
                    nfound += it->second.get_value() != NullIndexValue;
                    it->second = NullIndexValue;
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) override {
        TRY_CATCH_BAD_ALLOC({
            for (unsigned long replace_idxe : replace_idxes) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[replace_idxe].data);
                const auto value = values[replace_idxe];
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                    it->second = value;
                }
            }
        });
        return Status::OK();
    }

    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                      std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) override {
        faststring fixed_buf;
        TRY_CATCH_BAD_ALLOC(
                fixed_buf.reserve(sizeof(size_t) + sizeof(size_t) + idxes.size() * (KeySize + sizeof(IndexValue))));
        put_fixed32_le(&fixed_buf, KeySize);
        put_fixed32_le(&fixed_buf, idxes.size());
        for (const auto idx : idxes) {
            const auto value = (values != nullptr) ? values[idx] : IndexValue(NullIndexValue);
            fixed_buf.append(keys[idx].data, KeySize);
            put_fixed64_le(&fixed_buf, value.get_value());
        }
        RETURN_IF_ERROR(index_file->append(fixed_buf));
        *page_size += fixed_buf.size();
        // incremental calc crc32
        *checksum = crc32c::Extend(*checksum, (const char*)fixed_buf.data(), fixed_buf.size());
        return Status::OK();
    }

    Status load_wals(size_t n, const Slice* keys, const IndexValue* values) override {
        TRY_CATCH_BAD_ALLOC({
            for (size_t i = 0; i < n; i++) {
                const auto& key = *reinterpret_cast<const KeyType*>(keys[i].data);
                const auto value = values[i];
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                    it->second = value;
                }
            }
        });
        return Status::OK();
    }

    Status load_snapshot(phmap::BinaryInputArchive& ar) override {
        if (_mutable_index_format_version == kMutableIndexFormatVersion1) {
            TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_map.load(ar)));
        } else if (_mutable_index_format_version == kMutableIndexFormatVersion2) {
            // We introduced the new format specifically to address cross-platform compatibility issues with snapshot files.
            // In previous format, we met issue when migrate from x86 to arm64.
            // https://github.com/StarRocks/starrocks/issues/57952
            uint64_t size = 0;
            RETURN_IF(!ar.load(&size), Status::Corruption("FixedMutableIndex load snapshot size failed"));
            RETURN_IF(size == 0, Status::OK());
            TRY_CATCH_BAD_ALLOC(reserve(size));
            for (auto i = 0; i < size; ++i) {
                KeyType key;
                IndexValue value;
                RETURN_IF((!ar.load(reinterpret_cast<char*>(&key), sizeof(KeyType))),
                          Status::Corruption("FixedMutableIndex load snapshot failed because load key failed"));
                RETURN_IF((!ar.load(reinterpret_cast<char*>(&value), sizeof(IndexValue))),
                          Status::Corruption("FixedMutableIndex load snapshot failed because load value failed"));
                uint64_t hash = FixedKeyHash<KeySize>()(key);
                if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                    it->second = value;
                }
            }
        } else {
            return Status::Corruption("FixedMutableIndex load snapshot failed because format version is not supported");
        }
        return Status::OK();
    }

    Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) override {
        size_t kv_header_size = 8;
        std::string buff;
        TRY_CATCH_BAD_ALLOC(raw::stl_string_resize_uninitialized(&buff, kv_header_size));
        RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
        uint32_t key_size = UNALIGNED_LOAD32(buff.data());
        DCHECK(key_size == KeySize);
        offset += kv_header_size;
        uint32_t nums = UNALIGNED_LOAD32(buff.data() + 4);
        const size_t kv_pair_size = KeySize + sizeof(IndexValue);
        while (nums > 0) {
            const size_t batch_num = (nums > 4096) ? 4096 : nums;
            TRY_CATCH_BAD_ALLOC(raw::stl_string_resize_uninitialized(&buff, batch_num * kv_pair_size));
            RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
            std::vector<Slice> keys;
            keys.reserve(batch_num);
            std::vector<IndexValue> values;
            values.reserve(batch_num);
            size_t buf_offset = 0;
            for (size_t i = 0; i < batch_num; ++i) {
                keys.emplace_back(buff.data() + buf_offset, KeySize);
                uint64_t value = UNALIGNED_LOAD64(buff.data() + buf_offset + KeySize);
                values.emplace_back(value);
                buf_offset += kv_pair_size;
            }
            RETURN_IF_ERROR(load_wals(batch_num, keys.data(), values.data()));
            offset += batch_num * kv_pair_size;
            nums -= batch_num;
        }
        return Status::OK();
    }

    // return the dump file size if dump _map into a new file
    // If _map is empty, _map.dump_bound() will  set empty hash set serialize_size larger
    // than sizeof(uint64_t) in order to improve count distinct streaming aggregate performance.
    // Howevevr, the real snapshot file will only wite a size_(type is size_t) into file. So we
    // will use `sizeof(size_t)` as return value.
    size_t dump_bound() override { return _map.empty() ? sizeof(size_t) : _map.dump_bound(); }

    Status completeness_check(phmap::BinaryInputArchive& ar) override { return _map.completeness_check(ar); }

    Status dump(phmap::BinaryOutputArchive& ar) override {
        bool use_old_format = false;
        TEST_SYNC_POINT_CALLBACK("FixedMutableIndex::dump::1", &use_old_format);
        if (UNLIKELY(use_old_format)) {
            // For UT only.
            RETURN_IF_ERROR(_map.dump(ar));
            return Status::OK();
        }

        if (!ar.dump(static_cast<uint64_t>(size()))) {
            return Status::InternalError("FixedMutableIndex dump size failed");
        }
        if (size() == 0) {
            return Status::OK();
        }
        for (const auto& each : _map) {
            if (!ar.dump(reinterpret_cast<const char*>(each.first.data), sizeof(KeyType))) {
                return Status::InternalError("FixedMutableIndex dump key failed");
            }
            if (!ar.dump(reinterpret_cast<const char*>(&each.second), sizeof(IndexValue))) {
                return Status::InternalError("FixedMutableIndex dump value failed");
            }
        }
        return Status::OK();
    }

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) override {
        for (const auto& each : _map) {
            RETURN_IF_ERROR(dump->add_pindex_kvs(
                    std::string_view(reinterpret_cast<const char*>(each.first.data), sizeof(KeyType)),
                    each.second.get_value(), dump_pb));
        }
        return dump->finish_pindex_kvs(dump_pb);
    }

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool with_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t shard_bits = log2(nshard);
        for (auto i = 0; i < nshard; ++i) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        auto hasher = FixedKeyHash<KeySize>();
        for (const auto& [key, value] : _map) {
            if (!with_null && value.get_value() == NullIndexValue) {
                continue;
            }
            IndexHash h(hasher(key));
            ret[h.shard(shard_bits)].emplace_back((uint8_t*)&key, h.hash, KeySize + kIndexValueSize);
        }
        return ret;
    }

    Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard, size_t npage_hint,
                                    size_t page_size, size_t nbucket, bool with_null) const override {
        if (nshard > 0) {
            const auto& kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), with_null);
            for (const auto& kvs : kv_ref_by_shard) {
                RETURN_IF_ERROR(writer->write_shard(KeySize, npage_hint, page_size, nbucket, kvs));
            }
        }
        return Status::OK();
    }

    size_t size() const override { return _map.size(); }

    size_t usage() const override { return (KeySize + kIndexValueSize) * _map.size(); }

    size_t capacity() override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); }

    void clear() override { _map.clear(); }

    size_t memory_usage() override { return _map.capacity() * (1 + (KeySize + 3) / 4 * 4 + kIndexValueSize); }

    void set_mutable_index_format_version(uint32_t ver) override { _mutable_index_format_version = ver; }

private:
    phmap::flat_hash_map<KeyType, IndexValue, FixedKeyHash<KeySize>> _map;
    uint32_t _mutable_index_format_version = kMutableIndexFormatVersion2;
};

std::tuple<size_t, size_t, size_t> MutableIndex::estimate_nshard_and_npage(const size_t total_kv_pairs_usage,
                                                                           const size_t total_kv_num) {
    // if size == 0, will return { nshard:1, npage:0 }, meaning an empty shard
    size_t cap = total_kv_pairs_usage * 100 / kDefaultUsagePercent;
    size_t nshard = 1;
    while (nshard * 1024 * 1024 < cap) {
        nshard *= 2;
        if (nshard == kShardMax) {
            break;
        }
    }

    if (total_kv_num == 0) {
        return {nshard, 0, kPageSize};
    }

    size_t avg_kv_len = total_kv_pairs_usage / total_kv_num;
    size_t page_size = std::min(kMaxPerPageSize, pad(avg_kv_len * kRecordPerBucket, kPageSize));

    size_t npage = npad(cap / nshard, page_size);
    return {nshard, npage, page_size};
}

size_t MutableIndex::estimate_nbucket(size_t key_size, size_t size, size_t nshard, size_t npage) {
    // if size == 0, return 1 or return kBucketPerPage?
    if (size == 0) {
        return 1;
    }

    return kBucketPerPage;
}

struct StringHasher2 {
    uint64_t operator()(const std::string& s) const { return key_index_hash(s.data(), s.length() - kIndexValueSize); }
};

class EqualOnStringWithHash {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        return memequal_padded(lhs.data(), lhs.size() - kIndexValueSize, rhs.data(), rhs.size() - kIndexValueSize);
    }
};

DEFINE_FAIL_POINT(phmap_try_consume_mem_failed);
class SliceMutableIndex : public MutableIndex {
public:
    using KeyType = std::string;

    using WALKVSizeType = uint32_t;
    static constexpr size_t kWALKVSize = 4;
    static_assert(sizeof(WALKVSizeType) == kWALKVSize);
    static constexpr size_t kKeySizeMagicNum = 0;

    SliceMutableIndex() = default;
    ~SliceMutableIndex() override = default;

    Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
               const std::vector<size_t>& idxes) const override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                std::string composite_key;
                const auto& skey = keys[idx];
                const auto value = values[idx];
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value.get_value());
                uint64_t hash = StringHasher2()(composite_key);
                auto iter = _set.find(composite_key, hash);
                if (iter == _set.end()) {
                    values[idx] = NullIndexValue;
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                } else {
                    const auto& composite_key = *iter;
                    auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
                    values[idx] = IndexValue(value);
                    nfound += value != NullIndexValue;
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found, const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                std::string composite_key;
                const auto& skey = keys[idx];
                const auto value = values[idx];
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value.get_value());
                uint64_t hash = StringHasher2()(composite_key);
                if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                    _total_kv_pairs_usage += composite_key.size();
                } else {
                    const auto& old_compose_key = *it;
                    auto old_value =
                            UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
                    old_values[idx] = old_value;
                    nfound += old_value != NullIndexValue;
                    _set.erase(it);
                    _set.emplace_with_hash(hash, composite_key);
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                  const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                std::string composite_key;
                const auto& skey = keys[idx];
                const auto value = values[idx];
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value.get_value());
                uint64_t hash = StringHasher2()(composite_key);
                if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                    _total_kv_pairs_usage += composite_key.size();
                } else {
                    const auto& old_compose_key = *it;
                    const auto old_value =
                            UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
                    nfound += old_value != NullIndexValue;
                    // TODO: find a way to modify iterator directly, currently just erase then re-insert
                    _set.erase(it);
                    _set.emplace_with_hash(hash, composite_key);
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            for (const auto idx : idxes) {
                std::string composite_key;
                const auto& skey = keys[idx];
                const auto value = values[idx];
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value.get_value());
                uint64_t hash = StringHasher2()(composite_key);
                if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                    _total_kv_pairs_usage += composite_key.size();
                } else {
                    auto& old_compose_key = *it;
                    auto old_value =
                            UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
                    auto old_rssid = (uint32_t)(old_value >> 32);
                    auto old_rowid = (uint32_t)(old_value & ROWID_MASK);
                    auto new_value = reinterpret_cast<uint64_t*>(const_cast<IndexValue*>(&value));
                    std::string msg = strings::Substitute(
                            "SliceMutableIndex key_size=$0 insert found duplicate key, "
                            "new(rssid=$1 rowid=$2), old(rssid=$3 rowid=$4)",
                            skey.size, (uint32_t)((*new_value) >> 32), (uint32_t)((*new_value) & ROWID_MASK), old_rssid,
                            old_rowid);
                    LOG(WARNING) << msg;
                    return Status::AlreadyExist(msg);
                }
            }
        });
        return Status::OK();
    }

    Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                 const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            size_t nfound = 0;
            for (const auto idx : idxes) {
                std::string composite_key;
                const auto& skey = keys[idx];
                const auto value = NullIndexValue;
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value);
                uint64_t hash = StringHasher2()(composite_key);
                if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                    old_values[idx] = NullIndexValue;
                    not_found->key_infos.emplace_back((uint32_t)idx, hash);
                    _total_kv_pairs_usage += composite_key.size();
                } else {
                    auto& old_compose_key = *it;
                    auto old_value =
                            UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
                    old_values[idx] = old_value;
                    nfound += old_value != NullIndexValue;
                    // TODO: find a way to modify iterator directly, currently just erase then re-insert
                    _set.erase(it);
                    _set.emplace_with_hash(hash, composite_key);
                }
            }
            *num_found = nfound;
        });
        return Status::OK();
    }

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override {
        TRY_CATCH_BAD_ALLOC({
            for (const auto idx : idxes) {
                std::string composite_key;
                const auto& skey = keys[idx];
                const auto value = values[idx];
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value.get_value());
                uint64_t hash = StringHasher2()(composite_key);
                if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                    _total_kv_pairs_usage += composite_key.size();
                } else {
                    // TODO: find a way to modify iterator directly, currently just erase then re-insert
                    _set.erase(it);
                    _set.emplace_with_hash(hash, composite_key);
                }
            }
        });
        return Status::OK();
    }

    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                      std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) override {
        faststring fixed_buf;
        size_t keys_size = 0;
        auto n = idxes.size();
        for (const auto idx : idxes) {
            keys_size += keys[idx].size;
        }
        TRY_CATCH_BAD_ALLOC(fixed_buf.reserve(keys_size + n * (kWALKVSize + kIndexValueSize)));
        put_fixed32_le(&fixed_buf, kKeySizeMagicNum);
        put_fixed32_le(&fixed_buf, idxes.size());
        for (const auto idx : idxes) {
            const auto& key = keys[idx];
            const auto value = (values != nullptr) ? values[idx] : IndexValue(NullIndexValue);
            WALKVSizeType kv_size = key.size + kIndexValueSize;
            put_fixed32_le(&fixed_buf, kv_size);
            fixed_buf.append(key.data, key.size);
            put_fixed64_le(&fixed_buf, value.get_value());
        }
        RETURN_IF_ERROR(index_file->append(fixed_buf));
        *page_size += fixed_buf.size();
        // incremental calc crc32
        *checksum = crc32c::Extend(*checksum, (const char*)fixed_buf.data(), fixed_buf.size());
        return Status::OK();
    }

    Status load_wals(size_t n, const Slice* keys, const IndexValue* values) override {
        TRY_CATCH_BAD_ALLOC({
            for (size_t i = 0; i < n; i++) {
                std::string composite_key;
                const auto& skey = keys[i];
                const auto value = values[i];
                composite_key.reserve(skey.size + kIndexValueSize);
                composite_key.append(skey.data, skey.size);
                put_fixed64_le(&composite_key, value.get_value());
                uint64_t hash = StringHasher2()(composite_key);
                if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                    _total_kv_pairs_usage += composite_key.size();
                } else {
                    // TODO: find a way to modify iterator directly, currently just erase then re-insert
                    _set.erase(it);
                    _set.emplace_with_hash(hash, composite_key);
                }
            }
        });
        return Status::OK();
    }

    // return the dump file size if dump _set into a new file
    //  ｜--------    snapshot file      --------｜
    //  |  size_t ||   size_t  ||  char[]  | ... |   size_t  ||  char[]  |
    //  |total num|| data size ||  data    | ... | data size ||  data    |
    size_t dump_bound() override { return sizeof(size_t) * (1 + size()) + _total_kv_pairs_usage; }

    Status dump(phmap::BinaryOutputArchive& ar) override {
        if (!ar.dump(static_cast<uint64_t>(size()))) {
            return Status::Corruption("SliceMutableIndex dump size failed");
        }
        if (size() == 0) {
            return Status::OK();
        }
        for (const auto& composite_key : _set) {
            if (!ar.dump(static_cast<uint64_t>(composite_key.size()))) {
                return Status::Corruption("SliceMutableIndex dump composite_key size failed");
            }
            if (composite_key.size() == 0) {
                continue;
            }
            if (!ar.dump(composite_key.data(), composite_key.size())) {
                return Status::Corruption("SliceMutableIndex dump composite_key failed");
            }
        }
        return Status::OK();

        // TODO: construct a large buffer and write instead of one by one.
        // TODO: dive in phmap internal detail and implement dump of std::string type inside, use ctrl_&slot_ directly to improve performance
        // return _set.dump(ar);
    }

    Status completeness_check(phmap::BinaryInputArchive& ar) override {
        uint64_t size = 0;
        RETURN_IF(!ar.load(&size), Status::Corruption("Pindex load snapshot size failed"));
        RETURN_IF(size == 0, Status::OK());
        for (auto i = 0; i < size; ++i) {
            uint64_t compose_key_size = 0;
            RETURN_IF(!ar.load(&compose_key_size),
                      Status::Corruption("Pindex load snapshot failed because load compose_key_size failed"));
            if (compose_key_size == 0) {
                continue;
            }
            std::string composite_key;
            TRY_CATCH_BAD_ALLOC(raw::stl_string_resize_uninitialized(&composite_key, compose_key_size));
            RETURN_IF((!ar.load(composite_key.data(), composite_key.size())),
                      Status::Corruption("Pindex load snapshot failed because load composite_key failed"));
        }
        return Status::OK();
    }

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) override {
        for (const auto& composite_key : _set) {
            auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
            RETURN_IF_ERROR(dump->add_pindex_kvs(
                    std::string_view(composite_key.data(), composite_key.size() - kIndexValueSize), value, dump_pb));
        }
        return dump->finish_pindex_kvs(dump_pb);
    }

    Status load_snapshot(phmap::BinaryInputArchive& ar) override {
        uint64_t size = 0;
        RETURN_IF(!ar.load(&size), Status::Corruption("Pindex load snapshot size failed"));
        RETURN_IF(size == 0, Status::OK());
        TRY_CATCH_BAD_ALLOC(reserve(size));
        FAIL_POINT_TRIGGER_EXECUTE(phmap_try_consume_mem_failed, {
            CurrentThread::current().set_try_consume_mem_size(10);
            return Status::MemoryLimitExceeded("error phmap size");
        });
        for (auto i = 0; i < size; ++i) {
            uint64_t compose_key_size = 0;
            RETURN_IF(!ar.load(&compose_key_size),
                      Status::Corruption("Pindex load snapshot failed because load compose_key_size failed"));
            if (compose_key_size == 0) {
                continue;
            }
            std::string composite_key;
            TRY_CATCH_BAD_ALLOC(raw::stl_string_resize_uninitialized(&composite_key, compose_key_size));
            RETURN_IF((!ar.load(composite_key.data(), composite_key.size())),
                      Status::Corruption("Pindex load snapshot failed because load composite_key failed"));
            auto [it, inserted] = _set.emplace(composite_key);
            if (inserted) {
                _total_kv_pairs_usage += composite_key.size();
            } else {
                _set.erase(it);
                _set.emplace(composite_key);
            }
        }
        return Status::OK();

        // TODO: read a large buffer and parse instead of one by one.
        // TODO: dive in phmap internal detail and implement load of std::string type inside, use ctrl_&slot_ directly to improve performance
        // return _set.load(ar);
    }

    // TODO: read data in less batch, not one by one.
    Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) override {
        const auto kv_header_size = 8;
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, kv_header_size);
        RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
        offset += kv_header_size;
        const auto key_size = UNALIGNED_LOAD32(buff.data());
        DCHECK(key_size == kKeySizeMagicNum);
        auto nums = UNALIGNED_LOAD32(buff.data() + kv_header_size - 4);
        while (nums > 0) {
            size_t batch_num = (nums > 4096) ? 4096 : nums;
            Slice keys[batch_num];
            std::vector<IndexValue> values;
            values.reserve(batch_num);
            std::vector<std::string> kv_buffs(batch_num);
            for (size_t i = 0; i < batch_num; ++i) {
                raw::stl_string_resize_uninitialized(&buff, sizeof(uint32_t));
                RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
                offset += sizeof(uint32_t);
                const auto kv_pair_size = UNALIGNED_LOAD32(buff.data());
                raw::stl_string_resize_uninitialized(&kv_buffs[i], kv_pair_size);
                RETURN_IF_ERROR(file->read_at_fully(offset, kv_buffs[i].data(), kv_buffs[i].size()));
                keys[i] = Slice(kv_buffs[i].data(), kv_pair_size - kIndexValueSize);
                const auto value = UNALIGNED_LOAD64(kv_buffs[i].data() + kv_pair_size - kIndexValueSize);
                values.emplace_back(value);
                offset += kv_pair_size;
            }
            RETURN_IF_ERROR(load_wals(batch_num, keys, values.data()));
            nums -= batch_num;
        }
        return Status::OK();
    }

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool with_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t shard_bits = log2(nshard);
        for (auto i = 0; i < nshard; ++i) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        for (const auto& composite_key : _set) {
            const auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
            IndexHash h(StringHasher2()(composite_key));
            if (!with_null && value == NullIndexValue) {
                continue;
            }
            ret[h.shard(shard_bits)].emplace_back((uint8_t*)(composite_key.data()), h.hash, composite_key.size());
        }
        return ret;
    }

    Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard, size_t npage_hint,
                                    size_t page_size, size_t nbucket, bool with_null) const override {
        if (nshard > 0) {
            const auto& kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), with_null);
            for (const auto& kvs : kv_ref_by_shard) {
                RETURN_IF_ERROR(writer->write_shard(kKeySizeMagicNum, npage_hint, page_size, nbucket, kvs));
            }
        }
        return Status::OK();
    }

    size_t size() const override { return _set.size(); }

    size_t usage() const override { return _total_kv_pairs_usage; }

    size_t capacity() override { return _set.capacity(); }

    void reserve(size_t size) override { _set.reserve(size); }

    void clear() override {
        _set.clear();
        _total_kv_pairs_usage = 0;
    }

    // TODO: more accurate estimation for phmap::flat_hash_set<std::string, ...
    size_t memory_usage() override {
        auto ret = capacity() * (1 + 32);
        if (size() > 0 && _total_kv_pairs_usage / size() > 15) {
            // std::string with size > 15 will alloc new memory for storage
            ret += _total_kv_pairs_usage;
            // an malloc extra cost estimation
            ret += size() * 8;
        }
        return ret;
    }

    void set_mutable_index_format_version(uint32_t ver) override {}

private:
    friend ShardByLengthMutableIndex;
    friend PersistentIndex;
    phmap::flat_hash_set<KeyType, StringHasher2, EqualOnStringWithHash> _set;
    size_t _total_kv_pairs_usage = 0;
};

StatusOr<std::unique_ptr<MutableIndex>> MutableIndex::create(size_t key_size) {
#define CASE_SIZE(s) \
    case s:          \
        return std::make_unique<FixedMutableIndex<s>>();
#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)
    switch (key_size) {
    case 0:
        return std::make_unique<SliceMutableIndex>();
        CASE_SIZE_8(1)
        CASE_SIZE_8(9)
        CASE_SIZE_8(17)
        CASE_SIZE_8(25)
        CASE_SIZE_8(33)
        CASE_SIZE_8(41)
        CASE_SIZE_8(49)
        CASE_SIZE_8(57)
        CASE_SIZE_8(65)
        CASE_SIZE_8(73)
        CASE_SIZE_8(81)
        CASE_SIZE_8(89)
        CASE_SIZE_8(97)
        CASE_SIZE_8(105)
        CASE_SIZE_8(113)
        CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
    default:
        return Status::NotSupported("FixedMutableIndex not support key size large than 128");
    }
}

template <>
void ShardByLengthMutableIndex::_init_loop_helper<0>() {
    _shards.push_back(std::make_unique<SliceMutableIndex>());
    _shard_info_by_key_size[0] = std::make_pair(0, 1);
}

template <int N>
void ShardByLengthMutableIndex::_init_loop_helper() {
    _init_loop_helper<N - 1>();
    _shards.push_back(std::make_unique<FixedMutableIndex<N>>());
    _shard_info_by_key_size[N] = std::make_pair(N, 1);
}

Status ShardByLengthMutableIndex::init() {
    if (_fixed_key_size > 0) {
        auto st = MutableIndex::create(_fixed_key_size);
        if (!st.ok()) {
            return st.status();
        }
        _shards.push_back(std::move(st).value());
        _shard_info_by_key_size[_fixed_key_size] = std::make_pair(0, 1);
    } else if (_fixed_key_size == 0) {
        _shards.reserve(kSliceMaxFixLength + 1);
        _init_loop_helper<kSliceMaxFixLength>();
        return Status::OK();
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ShardByLengthMutableIndex>> ShardByLengthMutableIndex::create(size_t key_size,
                                                                                       const std::string& path) {
    auto mutable_index = std::make_unique<ShardByLengthMutableIndex>(key_size, path);
    RETURN_IF_ERROR(mutable_index->init());
    return mutable_index;
}

std::vector<std::vector<size_t>> ShardByLengthMutableIndex::split_keys_by_shard(size_t nshard, const Slice* keys,
                                                                                size_t idx_begin, size_t idx_end) {
    uint32_t shard_bits = log2(nshard);
    std::vector<std::vector<size_t>> idxes_by_shard(nshard);
    if (_fixed_key_size > 0) {
#define CASE_SIZE(s)                                                                        \
    case s: {                                                                               \
        auto hash_func = FixedKeyHash<s>();                                                 \
        for (auto i = idx_begin; i < idx_end; i++) {                                        \
            IndexHash hash(hash_func(*reinterpret_cast<const FixedKey<s>*>(keys[i].data))); \
            idxes_by_shard[hash.shard(shard_bits)].push_back(i);                            \
        }                                                                                   \
    } break;

#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)

        switch (_fixed_key_size) {
            CASE_SIZE_8(1)
            CASE_SIZE_8(9)
            CASE_SIZE_8(17)
            CASE_SIZE_8(25)
            CASE_SIZE_8(33)
            CASE_SIZE_8(41)
            CASE_SIZE_8(49)
            CASE_SIZE_8(57)
            CASE_SIZE_8(65)
            CASE_SIZE_8(73)
            CASE_SIZE_8(81)
            CASE_SIZE_8(89)
            CASE_SIZE_8(97)
            CASE_SIZE_8(105)
            CASE_SIZE_8(113)
            CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
        }
    } else if (_fixed_key_size == 0) {
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        for (size_t i = idx_begin; i < idx_end; i++) {
            const auto& key = fkeys[i];
            IndexHash hash(key_index_hash(key.data, key.size));
            idxes_by_shard[hash.shard(shard_bits)].push_back(i);
        }
    }
    return idxes_by_shard;
}

std::vector<std::vector<size_t>> ShardByLengthMutableIndex::split_keys_by_shard(size_t nshard, const Slice* keys,
                                                                                const std::vector<size_t>& idxes) {
    uint32_t shard_bits = log2(nshard);
    std::vector<std::vector<size_t>> idxes_by_shard(nshard);
    if (_fixed_key_size > 0) {
#define CASE_SIZE(s)                                                                          \
    case s: {                                                                                 \
        auto hash_func = FixedKeyHash<s>();                                                   \
        for (const auto idx : idxes) {                                                        \
            IndexHash hash(hash_func(*reinterpret_cast<const FixedKey<s>*>(keys[idx].data))); \
            idxes_by_shard[hash.shard(shard_bits)].emplace_back(idx);                         \
        }                                                                                     \
    } break;

#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)

        switch (_fixed_key_size) {
            CASE_SIZE_8(1)
            CASE_SIZE_8(9)
            CASE_SIZE_8(17)
            CASE_SIZE_8(25)
            CASE_SIZE_8(33)
            CASE_SIZE_8(41)
            CASE_SIZE_8(49)
            CASE_SIZE_8(57)
            CASE_SIZE_8(65)
            CASE_SIZE_8(73)
            CASE_SIZE_8(81)
            CASE_SIZE_8(89)
            CASE_SIZE_8(97)
            CASE_SIZE_8(105)
            CASE_SIZE_8(113)
            CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
        }
    } else if (_fixed_key_size == 0) {
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        for (const auto idx : idxes) {
            const auto& key = fkeys[idx];
            IndexHash hash(key_index_hash(key.data, key.size));
            idxes_by_shard[hash.shard(shard_bits)].emplace_back(idx);
        }
    }
    return idxes_by_shard;
}

Status ShardByLengthMutableIndex::get(size_t n, const Slice* keys, IndexValue* values, size_t* num_found,
                                      std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& not_found = not_founds_by_key_size[_fixed_key_size];
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->get(keys, values, &not_found, num_found, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->get(keys, values, &not_found, num_found, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                         size_t* num_found, std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& keys_info = not_founds_by_key_size[_fixed_key_size];
        for (auto i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->upsert(keys, values, old_values, &keys_info, num_found,
                                                              idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (auto i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->upsert(keys, values, old_values, &not_found, num_found,
                                                                  idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, size_t* num_found,
                                         std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& keys_info = not_founds_by_key_size[_fixed_key_size];
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->upsert(keys, values, &keys_info, num_found, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(
                        _shards[shard_offset + i]->upsert(keys, values, &not_found, num_found, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::insert(size_t n, const Slice* keys, const IndexValue* values,
                                         std::set<size_t>& check_l1_key_sizes) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->insert(keys, values, idxes_by_shard[i]));
        }
        check_l1_key_sizes.insert(shard_offset);
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->insert(keys, values, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::replace(const Slice* keys, const IndexValue* values,
                                          const std::vector<size_t>& idxes) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->replace(keys, values, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (const auto idx : idxes) {
            auto key_size = fkeys[idx].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(idx);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->replace(keys, values, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::erase(size_t n, const Slice* keys, IndexValue* old_values, size_t* num_found,
                                        std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& keys_info = not_founds_by_key_size[_fixed_key_size];
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(
                    _shards[shard_offset + i]->erase(keys, old_values, &keys_info, num_found, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(
                        _shards[shard_offset + i]->erase(keys, old_values, &not_found, num_found, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::append_wal(size_t n, const Slice* keys, const IndexValue* values) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                  &_page_size, &_checksum));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                      &_page_size, &_checksum));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::append_wal(const Slice* keys, const IndexValue* values,
                                             const std::vector<size_t>& idxes) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                  &_page_size, &_checksum));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (const auto idx : idxes) {
            auto key_size = fkeys[idx].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(idx);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                      &_page_size, &_checksum));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::check_snapshot_file(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& idxes) {
    // Check if this file is generated by old version of SR. There could be two types based on whether support SSE.
    // https://github.com/StarRocks/starrocks/blob/0d19cb4f9bc58d0cab5237a469b9e4bd30c0eb31/be/src/util/phmap/phmap.h#L447
    // If the `completeness_check` fails or `ar` doesn't reach end of file, it indicates that the file is either corrupted
    // or was generated on a different CPU architecture. In this case, compatibility loading will be skipped,
    // and the snapshot will be rebuilt.
    ar.reset();
    for (const auto idx : idxes) {
        RETURN_IF_ERROR(_shards[idx]->completeness_check(ar));
    }
    // Must reach the end of the file.
    if (!ar.eof()) {
        return Status::Corruption(fmt::format(
                "ShardByLengthMutableIndex snapshot file {} is generated by different arch or corrupt, will rebuild.",
                _path));
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::load_snapshot(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& idxes) {
    uint32_t magic_num = 0;
    RETURN_IF(!ar.load(&magic_num), Status::Corruption("ShardByLengthMutableIndex load snapshot magic num failed"));
    if (magic_num != kSnapshotMagicNum) {
        // There are three possible reasons:
        // 1. This file is corrupted.
        // 2. This file was generated at a different cpu architecture.
        // 3. This file was generated by a old version of SR.
        RETURN_IF_ERROR(check_snapshot_file(ar, idxes));
        // keep load snapshot using old format.
        for (const auto idx : idxes) {
            _shards[idx]->set_mutable_index_format_version(kMutableIndexFormatVersion1);
        }
        ar.reset();
    }
    for (const auto idx : idxes) {
        RETURN_IF_ERROR(_shards[idx]->load_snapshot(ar));
    }
    return Status::OK();
    // notice: accumulate will keep iterate the container, not return early.
    // return std::accumulate(idxes.begin(), idxes.end(), true, [](bool prev, size_t idx) { return _shards[idx]->load_snapshot(ar_in) && prev; });
}

size_t ShardByLengthMutableIndex::dump_bound() {
    return std::accumulate(_shards.begin(), _shards.end(), 0UL,
                           [](size_t s, const auto& e) { return e->size() > 0 ? s + e->dump_bound() : s; });
}

Status ShardByLengthMutableIndex::dump(phmap::BinaryOutputArchive& ar_out, std::set<uint32_t>& dumped_shard_idxes) {
    bool use_old_format = false;
    TEST_SYNC_POINT_CALLBACK("ShardByLengthMutableIndex::dump::1", &use_old_format);
    // We introduced the new format specifically to address cross-platform compatibility issues with snapshot files.
    // In previous format, we met issue when migrate from x86 to arm64.
    // https://github.com/StarRocks/starrocks/issues/57952
    if (LIKELY(!use_old_format)) {
        if (!ar_out.dump(kSnapshotMagicNum)) {
            return Status::InternalError("ShardByLengthMutableIndex dump snapshot magic num failed");
        }
    }
    for (uint32_t i = 0; i < _shards.size(); ++i) {
        const auto& shard = _shards[i];
        if (shard->size() > 0) {
            RETURN_IF_ERROR(shard->dump(ar_out));
            dumped_shard_idxes.insert(i);
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) {
    for (uint32_t i = 0; i < _shards.size(); ++i) {
        const auto& shard = _shards[i];
        RETURN_IF_ERROR(shard->pk_dump(dump, dump_pb));
    }
    return Status::OK();
}

static Status checksum_of_file(RandomAccessFile* file, uint64_t offset, uint32_t size, uint32* checksum) {
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, size);
    RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
    *checksum = crc32c::Value(buff.data(), buff.size());
    return Status::OK();
}

Status ShardByLengthMutableIndex::commit(MutableIndexMetaPB* meta, const EditVersion& version, const CommitType& type) {
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_path));
    switch (type) {
    case kFlush: {
        // create a new empty _l0 file because all data in _l0 has write into _l1 files
        std::string file_name = get_l0_index_file_name(_path, version);
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wblock_opts, file_name));
        DeferOp close_block([&wfile] {
            if (wfile) {
                WARN_IF_ERROR(wfile->close(), fmt::format("failed to close writable_file: {}", wfile->filename()));
            }
        });
        meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = meta->mutable_snapshot();
        snapshot->clear_dumped_shard_idxes();
        version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        // create a new empty _l0 file, set _offset to 0
        data->set_offset(0);
        data->set_size(0);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_7);
        _offset = 0;
        _page_size = 0;
        _checksum = 0;
        break;
    }
    case kSnapshot: {
        std::string file_name = get_l0_index_file_name(_path, version);
        // be maybe crash after create index file during last commit
        // so we delete expired index file first to make sure no garbage left
        (void)FileSystem::Default()->delete_file(file_name);
        std::set<uint32_t> dumped_shard_idxes;
        {
            // File is closed when archive object is destroyed and file size will be updated after file is
            // closed. So the archive object needed to be destroyed before reopen the file and assigned it
            // to _index_file. Otherwise some data of file maybe overwrite in future append.
            phmap::BinaryOutputArchive ar_out(file_name.data());
            RETURN_IF_ERROR(dump(ar_out, dumped_shard_idxes));
            if (!ar_out.close()) {
                std::string err_msg =
                        strings::Substitute("failed to dump snapshot to file $0, because of close", file_name);
                LOG(WARNING) << err_msg;
                return Status::InternalError(err_msg);
            }
        }
        // dump snapshot success, set _index_file to new snapshot file
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::MUST_EXIST;
        ASSIGN_OR_RETURN(_index_file, fs->new_writable_file(wblock_opts, file_name));
        // open l0 to calc checksum
        std::unique_ptr<RandomAccessFile> l0_rfile;
        ASSIGN_OR_RETURN(l0_rfile, fs->new_random_access_file(file_name));
        MonotonicStopWatch watch;
        watch.start();
        size_t snapshot_size = _index_file->size();
        // special case, snapshot file was written by phmap::BinaryOutputArchive which does not use system profiled API
        // so add write stats manually
#ifndef __APPLE__
        IOProfiler::add_write(snapshot_size, watch.elapsed_time());
#endif
        meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = meta->mutable_snapshot();
        version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(snapshot_size);
        snapshot->clear_dumped_shard_idxes();
        snapshot->mutable_dumped_shard_idxes()->Add(dumped_shard_idxes.begin(), dumped_shard_idxes.end());
        RETURN_IF_ERROR(checksum_of_file(l0_rfile.get(), 0, snapshot_size, &_checksum));
        snapshot->set_checksum(_checksum);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_7);
        _offset = snapshot_size;
        _page_size = 0;
        _checksum = 0;
        break;
    }
    case kAppendWAL: {
        IndexWalMetaPB* wal_pb = meta->add_wals();
        version.to_pb(wal_pb->mutable_version());
        PagePointerPB* data = wal_pb->mutable_data();
        data->set_offset(_offset);
        data->set_size(_page_size);
        wal_pb->set_checksum(_checksum);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_7);
        _offset += _page_size;
        _page_size = 0;
        _checksum = 0;
        break;
    }
    default: {
        return Status::InternalError("Unknown commit type");
    }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::load(const MutableIndexMetaPB& meta) {
    auto format_version = meta.format_version();
    if (format_version != PERSISTENT_INDEX_VERSION_2 && format_version != PERSISTENT_INDEX_VERSION_3 &&
        format_version != PERSISTENT_INDEX_VERSION_4 && format_version != PERSISTENT_INDEX_VERSION_5 &&
        format_version != PERSISTENT_INDEX_VERSION_6 && format_version != PERSISTENT_INDEX_VERSION_7) {
        std::string msg = strings::Substitute("different l0 format, should rebuid index. actual:$0, expect:$1",
                                              format_version, PERSISTENT_INDEX_VERSION_5);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    const IndexSnapshotMetaPB& snapshot_meta = meta.snapshot();
    const EditVersion& start_version = snapshot_meta.version();
    const PagePointerPB& page_pb = snapshot_meta.data();
    const auto snapshot_off = page_pb.offset();
    const auto snapshot_size = page_pb.size();
    std::set<uint32_t> dumped_shard_idxes;
    for (auto i = 0; i < snapshot_meta.dumped_shard_idxes_size(); ++i) {
        auto [_, insert] = dumped_shard_idxes.insert(snapshot_meta.dumped_shard_idxes(i));
        if (!insert) {
            LOG(WARNING) << "duplicate shard idx: " << snapshot_meta.dumped_shard_idxes(i)
                         << " which should not happened.";
            return Status::InternalError("duplicate shard idx");
        }
    }
    std::string index_file_name = get_l0_index_file_name(_path, start_version);
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_path));
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(index_file_name));
    phmap::BinaryInputArchive ar(index_file_name.data());
    if (snapshot_size > 0) {
        // check snapshot's crc32 checksum
        const uint32_t expected_checksum = snapshot_meta.checksum();
        // If expected crc32 is 0, which means no crc32 here, skip check.
        // This may happen when upgrade from old version.
        if (expected_checksum > 0) {
            uint32_t current_checksum = 0;
            RETURN_IF_ERROR(checksum_of_file(read_file.get(), snapshot_off, snapshot_size, &current_checksum));
            if (current_checksum != expected_checksum) {
                std::string error_msg = fmt::format(
                        "persistent index l0 crc checksum fail. filename: {} offset: {} cur_crc: {} expect_crc: {}",
                        index_file_name, snapshot_off, current_checksum, expected_checksum);
                LOG(ERROR) << error_msg;
                return Status::Corruption(error_msg);
            }
        }
        MonotonicStopWatch watch;
        watch.start();
        // do load snapshot
        RETURN_IF_ERROR(load_snapshot(ar, dumped_shard_idxes));
        // special case, snapshot file was written by phmap::BinaryOutputArchive which does not use system profiled API
        // so add read stats manually
#ifndef __APPLE__
        IOProfiler::add_read(snapshot_size, watch.elapsed_time());
#endif
    }
    // if mutable index is empty, set _offset as 0, otherwise set _offset as snapshot size
    _offset = snapshot_off + snapshot_size;
    const int n = meta.wals_size();
    // read wals and build hash map
    for (int i = 0; i < n; i++) {
        const auto& page_pointer_pb = meta.wals(i).data();
        size_t offset = page_pointer_pb.offset();
        const auto end = offset + page_pointer_pb.size();
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, 4);
        // check crc32
        const uint32_t expected_checksum = meta.wals(i).checksum();
        if (expected_checksum > 0) {
            uint32_t current_checksum = 0;
            RETURN_IF_ERROR(checksum_of_file(read_file.get(), offset, page_pointer_pb.size(), &current_checksum));
            if (current_checksum != expected_checksum) {
                std::string error_msg = fmt::format(
                        "persistent index l0 crc checksum fail. filename: {} offset: {} cur_crc: {} expect_crc: {}",
                        index_file_name, page_pointer_pb.offset(), current_checksum, expected_checksum);
                LOG(ERROR) << error_msg;
                return Status::Corruption(error_msg);
            }
        }
        while (offset < end) {
            RETURN_IF_ERROR(read_file->read_at_fully(offset, buff.data(), buff.size()));
            const auto key_size = UNALIGNED_LOAD32(buff.data());
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            for (auto i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->load(offset, read_file));
            }
        }
        _offset += page_pointer_pb.size();
    }
    RETURN_IF_ERROR(FileSystemUtil::resize_file(index_file_name, _offset));
    WritableFileOptions wblock_opts;
    wblock_opts.mode = FileSystem::MUST_EXIST;
    ASSIGN_OR_RETURN(_index_file, fs->new_writable_file(wblock_opts, index_file_name));
    return Status::OK();
}

Status ShardByLengthMutableIndex::flush_to_immutable_index(const std::string& path, const EditVersion& version,
                                                           bool write_tmp_l1, bool keep_delete) {
    auto writer = std::make_unique<ImmutableIndexWriter>();
    std::string idx_file_path;
    if (!write_tmp_l1) {
        idx_file_path = strings::Substitute("$0/index.l1.$1.$2", path, version.major_number(), version.minor_number());
    } else {
        idx_file_path = path;
    }
    RETURN_IF_ERROR(writer->init(idx_file_path, version, !write_tmp_l1));
    DCHECK(_fixed_key_size != -1);
    for (const auto& [key_size, shard_info] : _shard_info_by_key_size) {
        const auto [shard_offset, shard_size] = shard_info;
        const auto size = std::accumulate(std::next(_shards.begin(), shard_offset),
                                          std::next(_shards.begin(), shard_offset + shard_size), (size_t)0,
                                          [](size_t s, const auto& e) { return s + e->size(); });
        if (size != 0) {
            size_t total_kv_pairs_usage = 0;
            if (key_size == 0) {
                total_kv_pairs_usage = dynamic_cast<SliceMutableIndex*>(_shards[0].get())->_total_kv_pairs_usage;
            } else {
                total_kv_pairs_usage = (key_size + kIndexValueSize) * size;
            }
            const auto [nshard, npage_hint, page_size] =
                    MutableIndex::estimate_nshard_and_npage(total_kv_pairs_usage, size);
            const auto nbucket = MutableIndex::estimate_nbucket(key_size, size, nshard, npage_hint);
            const auto expand_exponent = nshard / shard_size;
            for (auto i = 0; i < shard_size; ++i) {
                // if keep_delete == true, flush immutable index with Delete Flag
                RETURN_IF_ERROR(_shards[shard_offset + i]->flush_to_immutable_index(writer, expand_exponent, npage_hint,
                                                                                    page_size, nbucket, keep_delete));
            }
        }
    }
    RETURN_IF_ERROR(writer->finish());
    return Status::OK();
}

size_t ShardByLengthMutableIndex::size() {
    return std::accumulate(_shards.begin(), _shards.end(), (size_t)0,
                           [](size_t s, const auto& e) { return s + e->size(); });
}

size_t ShardByLengthMutableIndex::capacity() {
    return std::accumulate(_shards.begin(), _shards.end(), (size_t)0,
                           [](size_t s, const auto& e) { return s + e->capacity(); });
}

size_t ShardByLengthMutableIndex::memory_usage() {
    return std::accumulate(_shards.begin(), _shards.end(), 0UL,
                           [](size_t s, const auto& e) { return s + e->memory_usage(); });
}

void ShardByLengthMutableIndex::clear() {
    for (const auto& shard : _shards) {
        shard->clear();
    }
}

Status ShardByLengthMutableIndex::create_index_file(std::string& path) {
    if (_index_file != nullptr) {
        std::string msg = strings::Substitute("l0 index file already exist: $0", _index_file->filename());
        return Status::InternalError(msg);
    }
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_index_file, _fs->new_writable_file(wblock_opts, path));
    return Status::OK();
}

#ifdef __SSE2__

#include <emmintrin.h>

size_t get_matched_tag_idxes(const uint8_t* tags, size_t ntag, uint8_t tag, uint8_t* matched_idxes) {
    size_t nmatched = 0;
    auto tests = _mm_set1_epi8(tag);
    for (size_t i = 0; i < ntag; i += 16) {
        auto tags16 = _mm_load_si128((__m128i*)(tags + i));
        auto eqs = _mm_cmpeq_epi8(tags16, tests);
        auto mask = _mm_movemask_epi8(eqs);
        while (mask != 0) {
            uint32_t match_pos = __builtin_ctz(mask);
            if (i + match_pos < ntag) {
                matched_idxes[nmatched++] = i + match_pos;
            }
            mask &= (mask - 1);
        }
    }
    return nmatched;
}

#else

size_t get_matched_tag_idxes(const uint8_t* tags, size_t ntag, uint8_t tag, uint8_t* matched_idxes) {
    size_t nmatched = 0;
    for (size_t i = 0; i < ntag; i++) {
        if (tags[i] == tag) {
            matched_idxes[nmatched++] = i;
        }
    }
    return nmatched;
}

#endif

Status ImmutableIndex::_get_fixlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                                 uint32_t shard_bits,
                                                 std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    for (uint32_t pageid = 0; pageid < shard_info.npage; pageid++) {
        auto& header = (*shard)->header(pageid);
        for (uint32_t bucketid = 0; bucketid < shard_info.nbucket; bucketid++) {
            auto& info = header.buckets[bucketid];
            const uint8_t* bucket_pos = (*shard)->pack_in_page(info.pageid, info.packid);
            size_t nele = info.size;
            const uint8_t* kvs = bucket_pos + pad(nele, kPackSize);
            for (size_t i = 0; i < nele; i++) {
                const uint8_t* kv = kvs + (shard_info.key_size + shard_info.value_size) * i;
                auto hash = IndexHash(key_index_hash(kv, shard_info.key_size));
                kvs_by_shard[hash.shard(shard_bits)].emplace_back(kv, hash.hash,
                                                                  shard_info.key_size + shard_info.value_size);
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_varlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                                 uint32_t shard_bits,
                                                 std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    for (uint32_t pageid = 0; pageid < shard_info.npage; pageid++) {
        auto& header = (*shard)->header(pageid);
        for (uint32_t bucketid = 0; bucketid < shard_info.nbucket; bucketid++) {
            auto& info = header.buckets[bucketid];
            const uint8_t* bucket_pos = (*shard)->pack_in_page(info.pageid, info.packid);
            size_t nele = info.size;
            const uint8_t* offsets = bucket_pos + pad(nele, kPackSize);
            for (size_t i = 0; i < nele; i++) {
                auto kv_offset = UNALIGNED_LOAD16(offsets + sizeof(uint16_t) * i);
                auto kv_size = UNALIGNED_LOAD16(offsets + sizeof(uint16_t) * (i + 1)) - kv_offset;
                const uint8_t* kv = bucket_pos + kv_offset;
                auto hash = IndexHash(key_index_hash(kv, kv_size - shard_info.value_size));
                kvs_by_shard[hash.shard(shard_bits)].emplace_back(kv, hash.hash, kv_size);
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                          uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0) {
        return Status::OK();
    }
    *shard = std::make_unique<ImmutableIndexShard>(shard_info.npage, shard_info.page_size);
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, (*shard)->data(), shard_info.bytes));
    RETURN_IF_ERROR((*shard)->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                               shard_info.bytes, shard_info.page_off));
    if (shard_info.key_size != 0) {
        return _get_fixlen_kvs_for_shard(kvs_by_shard, shard_idx, shard_bits, shard);
    } else {
        return _get_varlen_kvs_for_shard(kvs_by_shard, shard_idx, shard_bits, shard);
    }
}

Status ImmutableIndex::_get_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                            const std::vector<KeyInfo>& keys_info, IndexValue* values,
                                            KeysInfo* found_keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (const auto& key_info : keys_info) {
        IndexHash h(key_info.second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pack_in_page(bucket_info.pageid, bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = key_info.first;
        const auto* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
        auto kv_pos = bucket_pos + pad(nele, kPackSize);
        values[key_idx] = NullIndexValue;
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                values[key_idx] = UNALIGNED_LOAD64(candidate_kv + shard_info.key_size);
                found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                break;
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                            std::vector<KeyInfo>& keys_info, IndexValue* values,
                                            KeysInfo* found_keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];

    for (const auto& key_info : keys_info) {
        IndexHash h(key_info.second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pack_in_page(bucket_info.pageid, bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = key_info.first;
        const auto* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
        auto offset_pos = bucket_pos + pad(nele, kPackSize);
        values[key_idx] = NullIndexValue;
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto kv_offset = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * idx);
            auto kv_size = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * (idx + 1)) - kv_offset;
            auto candidate_kv = bucket_pos + kv_offset;
            if (keys[key_idx].size == kv_size - shard_info.value_size &&
                strings::memeq(candidate_kv, key_probe, kv_size - shard_info.value_size)) {
                values[key_idx] = UNALIGNED_LOAD64(candidate_kv + kv_size - shard_info.value_size);
                found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                break;
            }
        }
    }
    return Status::OK();
}

bool ImmutableIndex::_filter(size_t shard_idx, std::vector<KeyInfo>& keys_info, std::vector<KeyInfo>* res) const {
    // add configure enable_pindex_filter, if there are some bug exists, set it to false
    if (!config::enable_pindex_filter || _bf_off.empty()) {
        return false;
    }
    if (!_bf_vec.empty() && _bf_vec.size() <= shard_idx) {
        LOG(ERROR) << "read bloom filter failed, error shard idx:" << shard_idx << ", size:" << _bf_vec.size();
        return false;
    }

    if (!_bf_vec.empty() && _bf_vec[shard_idx] != nullptr) {
        for (size_t i = 0; i < keys_info.size(); i++) {
            auto key_idx = keys_info[i].first;
            auto hash = keys_info[i].second;
            if (_bf_vec[shard_idx]->test_hash(hash)) {
                res->emplace_back(std::make_pair(key_idx, hash));
            }
        }
        return true;
    }

    // read bloom filter for specified shard
    size_t off = _bf_off[shard_idx];
    size_t len = _bf_off[shard_idx + 1] - off;
    std::string bf_buff;
    raw::stl_string_resize_uninitialized(&bf_buff, len);
    Status st = _file->read_at_fully(off, bf_buff.data(), bf_buff.size());
    if (!st.ok()) {
        LOG(WARNING) << "shard_idx: " << shard_idx << "read bloom filter failed, " << st;
        return false;
    }
    std::unique_ptr<BloomFilter> bf;
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    if (!st.ok()) {
        LOG(WARNING) << "shard_idx: " << shard_idx << "bloom filter create failed, " << st;
        return false;
    }
    st = bf->init(bf_buff.data(), len, HASH_MURMUR3_X64_64);
    if (!st.ok()) {
        LOG(WARNING) << "shard_idx: " << shard_idx << "bloom filter init failed, " << st;
        return false;
    }
    for (size_t i = 0; i < keys_info.size(); i++) {
        auto key_idx = keys_info[i].first;
        auto hash = keys_info[i].second;
        if (bf->test_hash(hash)) {
            res->emplace_back(std::make_pair(key_idx, hash));
        }
    }
    return true;
}

Status ImmutableIndex::_split_keys_info_by_page(size_t shard_idx, std::vector<KeyInfo>& keys_info,
                                                std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page) const {
    const auto& shard_info = _shards[shard_idx];
    for (size_t i = 0; i < keys_info.size(); i++) {
        auto key_idx = keys_info[i].first;
        auto hash = keys_info[i].second;
        auto pageid = IndexHash(hash).page() % shard_info.npage;
        auto iter = keys_info_by_page.find(pageid);
        if (iter == keys_info_by_page.end()) {
            std::vector<KeyInfo> k;
            k.emplace_back(key_idx, hash);
            keys_info_by_page[pageid] = std::move(k);
        } else {
            iter->second.emplace_back(key_idx, hash);
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_read_page(size_t shard_idx, size_t pageid, LargeIndexPage* page, IOStat* stat) const {
    const auto& shard_info = _shards[shard_idx];
    IndexPage compressed_page;
    if (_compression_type == CompressionTypePB::NO_COMPRESSION) {
        RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset + shard_info.page_size * pageid, page->data(),
                                             shard_info.page_size));
    } else {
        RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset + shard_info.page_off[pageid], compressed_page.data,
                                             shard_info.page_off[pageid + 1] - shard_info.page_off[pageid]));
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(_compression_type, &codec));
        Slice compressed_body((uint8_t*)compressed_page.data,
                              shard_info.page_off[pageid + 1] - shard_info.page_off[pageid]);
        Slice decompressed_body((uint8_t*)page->data(), shard_info.page_size);
        RETURN_IF_ERROR(codec->decompress(compressed_body, &decompressed_body));
    }
    if (stat != nullptr) {
        stat->read_iops++;
        stat->read_io_bytes += (_compression_type == CompressionTypePB::NO_COMPRESSION)
                                       ? shard_info.page_size
                                       : shard_info.page_off[pageid + 1] - shard_info.page_off[pageid];
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_fixlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                                    KeysInfo* found_keys_info,
                                                    std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                                    std::map<size_t, LargeIndexPage>& pages) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (auto [_, keys_info] : keys_info_by_page) {
        for (size_t i = 0; i < keys_info.size(); i++) {
            IndexHash h(keys_info[i].second);
            auto pageid = h.page() % shard_info.npage;
            auto bucketid = h.bucket() % shard_info.nbucket;
            auto iter = pages.find(pageid);
            RETURN_ERROR_IF_FALSE(iter != pages.end());
            auto& bucket_info = iter->second.header().buckets[bucketid];
            uint8_t* bucket_pos;
            if (pageid == bucket_info.pageid) {
                bucket_pos = iter->second.pack(bucket_info.packid);
            } else {
                auto it = pages.find(bucket_info.pageid);
                if (it != pages.end()) {
                    bucket_pos = it->second.pack(bucket_info.packid);
                } else {
                    LargeIndexPage page(shard_info.page_size / kPageSize);
                    RETURN_IF_ERROR(_read_page(shard_idx, bucket_info.pageid, &page, nullptr));
                    pages[bucket_info.pageid] = std::move(page);
                    bucket_pos = pages[bucket_info.pageid].pack(bucket_info.packid);
                }
            }
            auto nele = bucket_info.size;
            auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
            auto key_idx = keys_info[i].first;
            const auto* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
            auto kv_pos = bucket_pos + pad(nele, kPackSize);
            values[key_idx] = NullIndexValue;
            for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
                auto idx = candidate_idxes[candidate_idx];
                auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
                if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                    values[key_idx] = UNALIGNED_LOAD64(candidate_kv + shard_info.key_size);
                    found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                    break;
                }
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_varlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                                    KeysInfo* found_keys_info,
                                                    std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                                    std::map<size_t, LargeIndexPage>& pages) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (auto [_, keys_info] : keys_info_by_page) {
        for (size_t i = 0; i < keys_info.size(); i++) {
            IndexHash h(keys_info[i].second);
            auto pageid = h.page() % shard_info.npage;
            auto bucketid = h.bucket() % shard_info.nbucket;
            auto iter = pages.find(pageid);
            RETURN_ERROR_IF_FALSE(iter != pages.end());
            auto& bucket_info = iter->second.header().buckets[bucketid];
            uint8_t* bucket_pos;
            if (pageid == bucket_info.pageid) {
                bucket_pos = iter->second.pack(bucket_info.packid);
            } else {
                auto it = pages.find(bucket_info.pageid);
                if (it != pages.end()) {
                    bucket_pos = it->second.pack(bucket_info.packid);
                } else {
                    LargeIndexPage page(shard_info.page_size / kPageSize);
                    RETURN_IF_ERROR(_read_page(shard_idx, bucket_info.pageid, &page, nullptr));
                    pages[bucket_info.pageid] = std::move(page);
                    bucket_pos = pages[bucket_info.pageid].pack(bucket_info.packid);
                }
            }
            auto nele = bucket_info.size;
            auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
            auto key_idx = keys_info[i].first;
            const auto* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
            auto offset_pos = bucket_pos + pad(nele, kPackSize);
            values[key_idx] = NullIndexValue;
            for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
                auto idx = candidate_idxes[candidate_idx];
                auto kv_offset = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * idx);
                auto kv_size = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * (idx + 1)) - kv_offset;
                auto candidate_kv = bucket_pos + kv_offset;
                if (keys[key_idx].size == kv_size - shard_info.value_size &&
                    strings::memeq(candidate_kv, key_probe, kv_size - shard_info.value_size)) {
                    values[key_idx] = UNALIGNED_LOAD64(candidate_kv + kv_size - shard_info.value_size);
                    found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                    break;
                }
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                             KeysInfo* found_keys_info,
                                             std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                             IOStat* stat) const {
    const auto& shard_info = _shards[shard_idx];
    std::map<size_t, LargeIndexPage> pages;
    for (const auto& [pageid, keys_info] : keys_info_by_page) {
        LargeIndexPage page(shard_info.page_size / kPageSize);
        RETURN_IF_ERROR(_read_page(shard_idx, pageid, &page, stat));
        pages[pageid] = std::move(page);
    }
    if (shard_info.key_size != 0) {
        return _get_in_fixlen_shard_by_page(shard_idx, n, keys, values, found_keys_info, keys_info_by_page, pages);
    } else {
        return _get_in_varlen_shard_by_page(shard_idx, n, keys, values, found_keys_info, keys_info_by_page, pages);
    }
}

Status ImmutableIndex::pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) {
    // put all kvs in one shard
    std::vector<std::vector<KVRef>> kvs_by_shard(1);
    std::vector<std::unique_ptr<ImmutableIndexShard>> shard_ptrs(_shards.size());
    for (size_t shard_idx = 0; shard_idx < _shards.size(); shard_idx++) {
        const auto& shard_info = _shards[shard_idx];
        if (shard_info.size == 0) {
            // skip empty shard
            continue;
        }
        shard_ptrs[shard_idx] = std::make_unique<ImmutableIndexShard>(shard_info.npage, shard_info.page_size);
        RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard_ptrs[shard_idx]->data(), shard_info.bytes));
        RETURN_IF_ERROR(shard_ptrs[shard_idx]->decompress_pages(_compression_type, shard_info.npage,
                                                                shard_info.uncompressed_size, shard_info.bytes,
                                                                shard_info.page_off));
        if (shard_info.key_size != 0) {
            RETURN_IF_ERROR(_get_fixlen_kvs_for_shard(kvs_by_shard, shard_idx, 0, &shard_ptrs[shard_idx]));
        } else {
            RETURN_IF_ERROR(_get_varlen_kvs_for_shard(kvs_by_shard, shard_idx, 0, &shard_ptrs[shard_idx]));
        }
    }

    // read kv from KVRef
    for (const auto& each : kvs_by_shard) {
        for (const auto& each_kv : each) {
            auto value = UNALIGNED_LOAD64(each_kv.kv_pos + each_kv.size - kIndexValueSize);
            RETURN_IF_ERROR(dump->add_pindex_kvs(
                    std::string_view(reinterpret_cast<const char*>(each_kv.kv_pos), each_kv.size - kIndexValueSize),
                    value, dump_pb));
        }
    }
    return dump->finish_pindex_kvs(dump_pb);
}

Status ImmutableIndex::_get_in_shard(size_t shard_idx, size_t n, const Slice* keys, std::vector<KeyInfo>& keys_info,
                                     IndexValue* values, KeysInfo* found_keys_info, IOStat* stat) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || shard_info.npage == 0 || keys_info.size() == 0) {
        return Status::OK();
    }

    DCHECK(_bf_vec.empty() || _bf_vec.size() > shard_idx);
    std::vector<KeyInfo> check_keys_info;
    bool filter = _filter(shard_idx, keys_info, &check_keys_info);
    if (!filter) {
        check_keys_info.swap(keys_info);
    } else {
        if (stat != nullptr) {
            stat->filtered_kv_cnt += (keys_info.size() - check_keys_info.size());
        }
    }

    if (check_keys_info.empty()) {
        // All keys have been filtered by bloom filter.
        return Status::OK();
    }

    // uncompressed_size == 0: upgrade from old version and no compression
    // uncompressed_size != 0 && page_off.back() > 0: new version, compress by page
    if (config::enable_pindex_read_by_page && (shard_info.uncompressed_size == 0 || shard_info.page_off.back() > 0)) {
        std::map<size_t, std::vector<KeyInfo>> keys_info_by_page;
        RETURN_IF_ERROR(_split_keys_info_by_page(shard_idx, check_keys_info, keys_info_by_page));
        return _get_in_shard_by_page(shard_idx, n, keys, values, found_keys_info, keys_info_by_page, stat);
    }

    std::unique_ptr<ImmutableIndexShard> shard =
            std::make_unique<ImmutableIndexShard>(shard_info.npage, shard_info.page_size);
    if (shard_info.uncompressed_size == 0) {
        RETURN_ERROR_IF_FALSE(shard->npage() * shard_info.page_size == shard_info.bytes, "illegal shard size");
    } else {
        RETURN_ERROR_IF_FALSE(shard->npage() * shard_info.page_size == shard_info.uncompressed_size,
                              "illegal shard size");
    }
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->data(), shard_info.bytes));
    RETURN_IF_ERROR(shard->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                            shard_info.bytes, shard_info.page_off));
    if (stat != nullptr) {
        stat->read_iops++;
        stat->read_io_bytes += shard_info.bytes;
    }
    if (shard_info.key_size != 0) {
        return _get_in_fixlen_shard(shard_idx, n, keys, check_keys_info, values, found_keys_info, &shard);
    } else {
        return _get_in_varlen_shard(shard_idx, n, keys, check_keys_info, values, found_keys_info, &shard);
    }
}

Status ImmutableIndex::_check_not_exist_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                        const KeysInfo& keys_info,
                                                        std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.key_infos[i].second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pack_in_page(bucket_info.pageid, bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_infos[i].first;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const auto* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
        auto kv_pos = bucket_pos + pad(nele, kPackSize);
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                return Status::AlreadyExist("key already exists in immutable index");
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_check_not_exist_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                        const KeysInfo& keys_info,
                                                        std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    DCHECK(shard_info.key_size == 0);
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.key_infos[i].second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pack_in_page(bucket_info.pageid, bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_infos[i].first;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const auto* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
        auto offset_pos = bucket_pos + pad(nele, kPackSize);
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto kv_offset = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * idx);
            auto kv_size = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * (idx + 1)) - kv_offset;
            auto candidate_kv = bucket_pos + kv_offset;
            if (keys[key_idx].size == kv_size - shard_info.value_size &&
                strings::memeq(candidate_kv, key_probe, kv_size - shard_info.value_size)) {
                return Status::AlreadyExist("key already exists in immutable index");
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_check_not_exist_in_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                 const KeysInfo& keys_info) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || keys_info.size() == 0) {
        return Status::OK();
    }
    std::unique_ptr<ImmutableIndexShard> shard =
            std::make_unique<ImmutableIndexShard>(shard_info.npage, shard_info.page_size);
    if (shard_info.uncompressed_size == 0) {
        RETURN_ERROR_IF_FALSE(shard->npage() * shard_info.page_size == shard_info.bytes, "illegal shard size");
    } else {
        RETURN_ERROR_IF_FALSE(shard->npage() * shard_info.page_size == shard_info.uncompressed_size,
                              "illegal shard size");
    }
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->data(), shard_info.bytes));
    RETURN_IF_ERROR(shard->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                            shard_info.bytes, shard_info.page_off));
    if (shard_info.key_size != 0) {
        return _check_not_exist_in_fixlen_shard(shard_idx, n, keys, keys_info, &shard);
    } else {
        return _check_not_exist_in_varlen_shard(shard_idx, n, keys, keys_info, &shard);
    }
}

static void split_keys_info_by_shard(std::vector<KeyInfo>& keys_info, std::vector<KeysInfo>& keys_info_by_shards) {
    uint32_t shard_bits = log2(keys_info_by_shards.size());
    for (const auto& key_info : keys_info) {
        auto key_idx = key_info.first;
        auto hash = key_info.second;
        size_t shard = IndexHash(hash).shard(shard_bits);
        keys_info_by_shards[shard].key_infos.emplace_back(key_idx, hash);
    }
}

bool ImmutableIndex::_need_bloom_filter(size_t idx_begin, size_t idx_end,
                                        std::vector<KeysInfo>& keys_info_by_shard) const {
    if (_bf_off.empty()) {
        return false;
    }

    if (!config::enable_pindex_filter || !StorageEngine::instance()->update_manager()->keep_pindex_bf()) {
        return false;
    }

    DCHECK(idx_end < _bf_off.size());
    size_t bf_bytes = _bf_off[idx_end] - _bf_off[idx_begin];
    size_t read_shard_bytes = 0;
    for (size_t i = 0; i < keys_info_by_shard.size(); i++) {
        if (!keys_info_by_shard[i].key_infos.empty()) {
            read_shard_bytes += _shards[i].bytes;
        }
    }
    return bf_bytes * config::max_bf_read_bytes_percent <= read_shard_bytes;
}

// There are several conditions
// 1. enable_pindex_filter is false, bloom filter is disable
// 2. _bf_off is empty which means there are no bloom filter exist in index file, this could be happened when we upgrade from
//    elder version
// 3. bloom filter already kept in memory
// 4. bloom filter is not in memory and memory usage is too high, skip the bloom filter to reduce memory usage
// 5. bloom filter is not in memory and memory usage is not high, we will read bloom filter from index file
Status ImmutableIndex::_prepare_bloom_filter(size_t idx_begin, size_t idx_end) const {
    if (!config::enable_pindex_filter || _bf_off.empty()) {
        return Status::OK();
    }
    if (_bf_vec.empty()) {
        _bf_vec.resize(_shards.size());
    }
    DCHECK(idx_begin < idx_end);
    DCHECK(_bf_vec.size() >= _shards.size() && _bf_vec.size() >= idx_end);
    if (_bf_vec.size() < _shards.size()) {
        return Status::OK();
    }
    // alread loaded in memory
    if (_bf_vec[idx_begin] != nullptr) {
        return Status::OK();
    }
    DCHECK(_bf_off.size() > idx_end);
    size_t batch_bytes = kBatchBloomFilterReadSize;
    size_t read_bytes = 0;
    size_t start_idx = idx_begin;
    size_t num = 0;
    for (size_t i = idx_begin; i < idx_end; i++) {
        if (read_bytes >= batch_bytes) {
            size_t offset = _bf_off[start_idx];
            size_t bytes = _bf_off[start_idx + num] - offset;
            std::string buff;
            raw::stl_string_resize_uninitialized(&buff, bytes);
            RETURN_IF_ERROR(_file->read_at_fully(offset, buff.data(), buff.size()));
            for (size_t i = 0; i < num; i++) {
                size_t buff_off = _bf_off[start_idx + i] - _bf_off[start_idx];
                size_t buff_size = _bf_off[start_idx + i + 1] - _bf_off[start_idx + i];
                std::unique_ptr<BloomFilter> bf;
                RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
                RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
                _bf_vec[start_idx + i] = std::move(bf);
            }
            start_idx = i;
            read_bytes = _bf_off[i + 1] - _bf_off[i];
            num = 1;
        } else {
            num++;
            read_bytes += _bf_off[i + 1] - _bf_off[i];
        }
    }
    if (start_idx < idx_end) {
        size_t offset = _bf_off[start_idx];
        size_t bytes = _bf_off[start_idx + num] - offset;
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, bytes);
        RETURN_IF_ERROR(_file->read_at_fully(offset, buff.data(), buff.size()));
        for (size_t i = 0; i < num; i++) {
            size_t buff_off = _bf_off[start_idx + i] - _bf_off[start_idx];
            size_t buff_size = _bf_off[start_idx + i + 1] - _bf_off[start_idx + i];
            std::unique_ptr<BloomFilter> bf;
            RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
            RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
            _bf_vec[start_idx + i] = std::move(bf);
        }
    }
    return Status::OK();
}

Status ImmutableIndex::get(size_t n, const Slice* keys, KeysInfo& keys_info, IndexValue* values,
                           KeysInfo* found_keys_info, size_t key_size, IOStat* stat) {
    auto iter = _shard_info_by_length.find(key_size);
    if (iter == _shard_info_by_length.end()) {
        return Status::OK();
    }

    const auto [shard_off, nshard] = iter->second;
    if (nshard > 1) {
        std::vector<KeysInfo> keys_info_by_shard(nshard);
        MonotonicStopWatch watch;
        watch.start();
        split_keys_info_by_shard(keys_info.key_infos, keys_info_by_shard);
        if (_need_bloom_filter(shard_off, shard_off + nshard, keys_info_by_shard)) {
            RETURN_IF_ERROR(_prepare_bloom_filter(shard_off, shard_off + nshard));
        }
        for (size_t i = 0; i < nshard; i++) {
            RETURN_IF_ERROR(_get_in_shard(shard_off + i, n, keys, keys_info_by_shard[i].key_infos, values,
                                          found_keys_info, stat));
        }
        if (stat != nullptr) {
            stat->get_in_shard_cost += watch.elapsed_time();
        }
    } else {
        MonotonicStopWatch watch;
        watch.start();
        KeysInfo infos;
        infos.key_infos.assign(keys_info.key_infos.begin(), keys_info.key_infos.end());
        if (config::enable_pindex_filter && StorageEngine::instance()->update_manager()->keep_pindex_bf()) {
            RETURN_IF_ERROR(_prepare_bloom_filter(shard_off, shard_off + nshard));
        }
        RETURN_IF_ERROR(_get_in_shard(shard_off, n, keys, infos.key_infos, values, found_keys_info, stat));
        if (stat != nullptr) {
            stat->get_in_shard_cost += watch.elapsed_time();
        }
    }
    return Status::OK();
}

Status ImmutableIndex::check_not_exist(size_t n, const Slice* keys, size_t key_size) {
    auto iter = _shard_info_by_length.find(key_size);
    if (iter == _shard_info_by_length.end()) {
        return Status::OK();
    }
    const auto [shard_off, nshard] = iter->second;
    uint32_t shard_bits = log2(nshard);
    std::vector<KeysInfo> keys_info_by_shard(nshard);
    for (size_t i = 0; i < n; i++) {
        IndexHash h(key_index_hash(keys[i].data, keys[i].size));
        auto shard = h.shard(shard_bits);
        keys_info_by_shard[shard].key_infos.emplace_back(i, h.hash);
    }
    for (size_t i = 0; i < nshard; i++) {
        RETURN_IF_ERROR(_check_not_exist_in_shard(shard_off + i, n, keys, keys_info_by_shard[i]));
    }
    return Status::OK();
}

DEFINE_FAIL_POINT(immutable_index_no_page_off);
StatusOr<std::unique_ptr<ImmutableIndex>> ImmutableIndex::load(std::unique_ptr<RandomAccessFile>&& file,
                                                               bool load_bf_data) {
    ASSIGN_OR_RETURN(auto file_size, file->get_size());
    if (file_size < 12) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: file size $1 < 12", file->filename(), file_size));
    }
    size_t footer_read_size = std::min<size_t>(4096, file_size);
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, footer_read_size);
    RETURN_IF_ERROR(file->read_at_fully(file_size - footer_read_size, buff.data(), buff.size()));
    uint32_t footer_length = UNALIGNED_LOAD32(buff.data() + footer_read_size - 12);
    uint32_t checksum = UNALIGNED_LOAD32(buff.data() + footer_read_size - 8);
    uint32_t magic = UNALIGNED_LOAD32(buff.data() + footer_read_size - 4);
    if (magic != UNALIGNED_LOAD32(kIndexFileMagic)) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 illegal magic", file->filename()));
    }
    std::string_view meta_str;
    if (footer_length <= footer_read_size - 12) {
        meta_str = std::string_view(buff.data() + footer_read_size - 12 - footer_length, footer_length + 4);
    } else {
        raw::stl_string_resize_uninitialized(&buff, footer_length + 4);
        RETURN_IF_ERROR(file->read_at_fully(file_size - 12 - footer_length, buff.data(), buff.size()));
        meta_str = std::string_view(buff.data(), footer_length + 4);
    }
    auto actual_checksum = crc32c::Value(meta_str.data(), meta_str.size());
    if (checksum != actual_checksum) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 checksum not match", file->filename()));
    }
    ImmutableIndexMetaPB meta;
    if (!meta.ParseFromArray(meta_str.data(), meta_str.size() - 4)) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 parse meta pb failed", file->filename()));
    }

    auto format_version = meta.format_version();
    if (format_version != PERSISTENT_INDEX_VERSION_2 && format_version != PERSISTENT_INDEX_VERSION_3 &&
        format_version != PERSISTENT_INDEX_VERSION_4 && format_version != PERSISTENT_INDEX_VERSION_5 &&
        format_version != PERSISTENT_INDEX_VERSION_6 && format_version != PERSISTENT_INDEX_VERSION_7) {
        std::string msg =
                strings::Substitute("different immutable index format, should rebuid index. actual:$0, expect:$1",
                                    format_version, PERSISTENT_INDEX_VERSION_7);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    std::unique_ptr<ImmutableIndex> idx = std::make_unique<ImmutableIndex>();
    idx->_version = EditVersion(meta.version());
    idx->_size = meta.size();
    if (meta.compression_type() > 0) {
        idx->_compression_type = static_cast<CompressionTypePB>(meta.compression_type());
    } else {
        idx->_compression_type = CompressionTypePB::NO_COMPRESSION;
    }
    size_t nshard = meta.shards_size();
    idx->_shards.resize(nshard);
    for (size_t i = 0; i < nshard; i++) {
        const auto& src = meta.shards(i);
        auto& dest = idx->_shards[i];
        dest.size = src.size();
        dest.npage = src.npage();
        dest.offset = src.data().offset();
        dest.bytes = src.data().size();
        dest.key_size = src.key_size();
        dest.value_size = src.value_size();
        dest.nbucket = src.nbucket();
        auto page_size = src.page_size();
        if (page_size == 0) {
            page_size = 4096;
        }
        dest.page_size = page_size;
        dest.uncompressed_size = src.uncompressed_size();
        if (idx->_compression_type == CompressionTypePB::NO_COMPRESSION) {
            RETURN_ERROR_IF_FALSE(dest.uncompressed_size == 0,
                                  "compression type: " + std::to_string(idx->_compression_type) +
                                          " uncompressed_size: " + std::to_string(dest.uncompressed_size));
        }
        // This is for compatibility, we don't add data_size in shard_info in the rc version
        // And data_size is added to reslove some bug(https://github.com/StarRocks/starrocks/issues/11868)
        // However, if we upgrade from rc version, the data_size will be used as default value(0) which will cause
        // some error in the subsequent logic
        // So we will use file size as data_size which will cause some of disk space to be wasted, but it is a acceptable
        // problem. And the wasted disk space will be reclaimed in the subsequent compaction, so it is acceptable
        if (src.size() != 0 && src.data_size() == 0) {
            dest.data_size = src.data().size();
        } else {
            dest.data_size = src.data_size();
        }
        FAIL_POINT_TRIGGER_EXECUTE(immutable_index_no_page_off, { meta.mutable_shards(i)->clear_page_off(); });
        if (src.page_off().size() == 0) {
            // When upgrading from a historical version that does not support page compression, set page off to 0 to distinguish it
            // from the new version which support page compression.
            dest.page_off.resize(src.npage() + 1, 0);
        } else {
            for (int i = 0; i < src.npage() + 1; i++) {
                dest.page_off.emplace_back(src.page_off(i));
            }
        }
    }
    size_t nlength = meta.shard_info_size();
    for (size_t i = 0; i < nlength; i++) {
        const auto& src = meta.shard_info(i);
        if (auto [_, inserted] =
                    idx->_shard_info_by_length.insert({src.key_size(), {src.shard_off(), src.shard_num()}});
            !inserted) {
            LOG(WARNING) << "load failed because insert shard info failed, maybe duplicate, key size: "
                         << src.key_size();
            return Status::InternalError("load failed because of insert failed");
        }
    }

    std::vector<std::unique_ptr<BloomFilter>> bf_vec(nshard);
    size_t nshard_bf = meta.shard_bf_off_size();
    DCHECK(nshard_bf == 0 || nshard_bf == nshard + 1);
    std::vector<size_t> bf_off;
    for (size_t i = 0; i < nshard_bf; i++) {
        bf_off.emplace_back(meta.shard_bf_off(i));
    }

    if (load_bf_data && nshard_bf != 0) {
        size_t batch_bytes = kBatchBloomFilterReadSize;
        size_t read_bytes = 0;
        size_t start_idx = 0;
        size_t num = 0;
        for (size_t i = 0; i < nshard; i++) {
            if (read_bytes >= batch_bytes) {
                size_t offset = bf_off[start_idx];
                size_t bytes = bf_off[start_idx + num] - offset;
                std::string buff;
                raw::stl_string_resize_uninitialized(&buff, bytes);
                RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
                for (size_t i = 0; i < num; i++) {
                    size_t buff_off = bf_off[start_idx + i] - bf_off[start_idx];
                    size_t buff_size = bf_off[start_idx + i + 1] - bf_off[start_idx + i];
                    std::unique_ptr<BloomFilter> bf;
                    RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
                    RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
                    bf_vec[start_idx + i] = std::move(bf);
                }
                start_idx = i;
                read_bytes = bf_off[i + 1] - bf_off[i];
                num = 1;
            } else {
                num++;
                read_bytes += bf_off[i + 1] - bf_off[i];
            }
        }
        if (start_idx < nshard) {
            size_t offset = bf_off[start_idx];
            size_t bytes = bf_off[start_idx + num] - offset;
            std::string buff;
            raw::stl_string_resize_uninitialized(&buff, bytes);
            RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
            for (size_t i = 0; i < num; i++) {
                size_t buff_off = bf_off[start_idx + i] - bf_off[start_idx];
                size_t buff_size = bf_off[start_idx + i + 1] - bf_off[start_idx + i];
                std::unique_ptr<BloomFilter> bf;
                RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
                RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
                bf_vec[start_idx + i] = std::move(bf);
            }
        }
        idx->_bf_vec.swap(bf_vec);
    }
    idx->_file.swap(file);
    idx->_bf_off.swap(bf_off);
    return std::move(idx);
} // namespace starrocks
