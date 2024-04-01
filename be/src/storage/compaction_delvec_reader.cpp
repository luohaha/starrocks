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

#include "storage/compaction_delvec_reader.h"

#include <fmt/format.h>

#include "fs/fs.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/raw_container.h"

namespace starrocks {

Status RowsMapperBuilder::_init() {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_filename));
    WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_wfile, fs->new_writable_file(wblock_opts, _filename));
    return Status::OK();
}

Status RowsMapperBuilder::append(const std::vector<uint64_t>& rssid_rowids) {
    if (_wfile == nullptr) {
        RETURN_IF_ERROR(_init());
    }
    RETURN_IF_ERROR(_wfile->append(Slice((const char*)rssid_rowids.data(), rssid_rowids.size() * 8)));
    _checksum = crc32c::Extend(_checksum, (const char*)rssid_rowids.data(), rssid_rowids.size() * 8);
    _footer.set_row_nums(_footer.row_nums() + rssid_rowids.size());
    return Status::OK();
}

void RowsMapperBuilder::finalize_segment(uint32_t row_num) {
    const size_t sz = _footer.segment_row_offsets_size();
    if (sz == 0) {
        _footer.add_segment_row_offsets(row_num - 1);
    } else {
        _footer.add_segment_row_offsets(_footer.segment_row_offsets(sz - 1) + row_num);
    }
}

static Status footer_check(const RowsetMapperFooterPB& footer) {
    if (footer.segment_row_offsets_size() > 0 &&
        footer.segment_row_offsets(footer.segment_row_offsets_size() - 1) + 1 != footer.row_nums()) {
        std::string error_msg = fmt::format("Invalid footer, footer: {}", footer.ShortDebugString());
        LOG(ERROR) << error_msg;
        return Status::InternalError(error_msg);
    }
    return Status::OK();
}

Status RowsMapperBuilder::finalize() {
    if (_wfile == nullptr) {
        // Empty rows, skip finalize
        return Status::OK();
    }
    RETURN_IF_ERROR(footer_check(_footer));
    std::string footer_str;
    if (_footer.SerializeToString(&footer_str)) {
        _checksum = crc32c::Extend(_checksum, footer_str.data(), footer_str.size());
        std::string fixed_buf;
        // footer's size
        put_fixed32_le(&fixed_buf, footer_str.size());
        // checksum
        put_fixed32_le(&fixed_buf, _checksum);
        RETURN_IF_ERROR(_wfile->append(footer_str));
        RETURN_IF_ERROR(_wfile->append(fixed_buf));
    } else {
        return Status::InternalError("RowsetMapperFooterPB serialize fail");
    }
    return _wfile->close();
}

// Open file
Status RowsMapperIterator::open(const std::string& filename) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
    ASSIGN_OR_RETURN(_rfile, fs->new_random_access_file(filename));
    ASSIGN_OR_RETURN(int64_t file_size, _rfile->get_size());
    // 1. read checksum
    std::string checksum_str;
    raw::stl_string_resize_uninitialized(&checksum_str, 4);
    RETURN_IF_ERROR(_rfile->read_at_fully(file_size - 4, checksum_str.data(), checksum_str.size()));
    _expected_checksum = decode_fixed32_le((const uint8_t*)checksum_str.data());
    // 2. read footer size
    std::string footer_size_str;
    raw::stl_string_resize_uninitialized(&footer_size_str, 4);
    RETURN_IF_ERROR(_rfile->read_at_fully(file_size - 8, footer_size_str.data(), footer_size_str.size()));
    const uint32_t footer_size = decode_fixed32_le((const uint8_t*)footer_size_str.data());
    // 3. read footer
    std::string footer_str;
    raw::stl_string_resize_uninitialized(&footer_str, footer_size);
    RETURN_IF_ERROR(_rfile->read_at_fully(file_size - 8 - footer_size, footer_str.data(), footer_str.size()));
    if (!_footer.ParseFromString(footer_str)) {
        return Status::Corruption("RowsetMapperFooterPB parse fail");
    }
    if (file_size != footer_size + 8 + _footer.row_nums() * 8) {
        return Status::Corruption(fmt::format("RowsMapper file corruption. file size: {}, footer size: {}, row num: {}",
                                              file_size, footer_size, _footer.row_nums()));
    }
    // 4. check footer
    RETURN_IF_ERROR(footer_check(_footer));
    // 5. build offset map
    for (uint32_t i = 0; i < _footer.segment_row_offsets_size(); i++) {
        _offset_to_sid[_footer.segment_row_offsets(i)] = i;
    }
    // 6. get next
    RETURN_IF_ERROR(_next());
    return Status::OK();
}

// Return true when iterator meet end of file
bool RowsMapperIterator::end_of_file() const {
    // When pos large than row number, means end of file.
    return _pos > _footer.row_nums();
}

// Move to next position
void RowsMapperIterator::next() {
    _internal_status = _next();
}

Status RowsMapperIterator::_next() {
    if (end_of_file()) {
        return Status::OK();
    }
    _current_val.clear();
    size_t prefetch_cnt = std::min(READ_ROW_COUNT_EACH_TIME, _footer.row_nums() - _pos);
    if (prefetch_cnt == 0) {
        // End of file
        _pos++;
        std::string footer_str;
        if (_footer.SerializeToString(&footer_str)) {
            _current_checksum = crc32c::Extend(_current_checksum, footer_str.data(), footer_str.size());
        } else {
            return Status::InternalError("RowsetMapperFooterPB serialize fail");
        }
        return Status::OK();
    }
    std::vector<uint64_t> rssid_rowids(prefetch_cnt);
    RETURN_IF_ERROR(_rfile->read_at_fully(_pos * EACH_ROW_SIZE, rssid_rowids.data(), prefetch_cnt * EACH_ROW_SIZE));
    _current_checksum = crc32c::Extend(_current_checksum, (const char*)rssid_rowids.data(), rssid_rowids.size() * 8);
    // Transfer from rssid rowid to RowsMapper
    RETURN_IF_ERROR(_transfer_row_mapper(rssid_rowids, _pos));
    _pos += prefetch_cnt;
    return Status::OK();
}

Status RowsMapperIterator::_transfer_row_mapper(const std::vector<uint64_t>& rssid_rowids, uint64_t start_pos) {
    for (size_t i = 0; i < rssid_rowids.size(); i++) {
        const uint64_t id = rssid_rowids[i];
        auto iter = _offset_to_sid.lower_bound(id);
        if (iter == _offset_to_sid.end()) {
            // Not found
            return Status::Corruption(
                    fmt::format("unexpected rssid & rowid : {}, footer : {}", id, _footer.ShortDebugString()));
        }
        // <output segment id, output rowid, input segment id, input rowid>
        if (iter == _offset_to_sid.begin()) {
            // first segment in output rowset
            _current_val.emplace_back(iter->second, start_pos + i, id >> 32, id & 0xffffffff);
        } else {
            auto prevIt = std::prev(iter);
            _current_val.emplace_back(iter->second, start_pos + i - prevIt->first - 1, id >> 32, id & 0xffffffff);
        }
    }
    return Status::OK();
}

// Return current value
void RowsMapperIterator::value(std::vector<RowsMapper>* rows_mapper) {
    *rows_mapper = _current_val;
}

Status RowsMapperIterator::status() const {
    if (end_of_file()) {
        // check checksum
        if (_expected_checksum != _current_checksum) {
            return Status::Corruption(fmt::format("checksum mismatch. cur: {} expected: {} filename: {}",
                                                  _current_checksum, _expected_checksum, _rfile->filename()));
        }
    }
    return _internal_status;
}

Status CompactionDelvecReader::read_delvec(uint32_t rowset_seg_id, std::vector<uint32_t>* deletes) {
    return Status::OK();
}

} // namespace starrocks