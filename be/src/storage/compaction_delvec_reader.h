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

#pragma once

#include "common/status.h"
#include "gen_cpp/persistent_index.pb.h"

namespace starrocks {

class WritableFile;
class RandomAccessFile;

// Build the connection between input rowsets' rows and ouput rowsets' rows,
// and then write to file.
//
// Format: [data, RowsetMapperFooterPB, RowsetMapperFooterPBSize(4), Checksum(4)]
//
class RowsMapperBuilder {
public:
    RowsMapperBuilder(const std::string& filename) : _filename(filename) {}
    ~RowsMapperBuilder() {}
    // append rssid rowids to file
    Status append(const std::vector<uint64_t>& rssid_rowids);
    // record segment row number
    void finalize_segment(uint32_t row_num);
    Status finalize();

private:
    Status _init();

private:
    std::string _filename;
    std::unique_ptr<WritableFile> _wfile;
    uint32_t _checksum = 0;
    RowsetMapperFooterPB _footer;
};

struct RowsMapper {
    RowsMapper(uint32_t ose, uint32_t oro, uint32_t irs, uint32_t iro)
            : output_segment_id(ose), output_rowid(oro), input_rssid(irs), input_rowid(iro) {}
    uint32_t output_segment_id = 0;
    uint32_t output_rowid = 0;
    uint32_t input_rssid = 0;
    uint32_t input_rowid = 0;
};

// Iterator for rows mapper file
class RowsMapperIterator {
public:
    RowsMapperIterator() {}
    ~RowsMapperIterator() {}
    // Open file
    Status open(const std::string& filename);
    // Return true when iterator meet end of file
    bool end_of_file() const;
    // Move to next position
    void next();
    // Return number of rows mapper that we get
    void value(std::vector<RowsMapper>* rows_mapper);
    // Return status when using iterator
    Status status() const;

private:
    Status _transfer_row_mapper(const std::vector<uint64_t>& rssid_rowids, uint64_t start_pos);
    Status _next();
    static const size_t READ_ROW_COUNT_EACH_TIME = 1024;
    static const size_t EACH_ROW_SIZE = 8; // 8 bytes

private:
    std::unique_ptr<RandomAccessFile> _rfile;
    RowsetMapperFooterPB _footer;
    Status _internal_status;
    // Point to the next position that we want to read
    uint64_t _pos = 0;
    // From each segment's last row's offset to segment id
    std::map<uint32_t, uint32_t> _offset_to_sid;
    // Current checksum and expected checksum, will check if mismatch when iterator end
    uint32_t _expected_checksum = 0;
    uint32_t _current_checksum = 0;
    std::vector<RowsMapper> _current_val;
};

class CompactionDelvecReader {
public:
    CompactionDelvecReader() {}
    ~CompactionDelvecReader() {}

    Status read_delvec(uint32_t rowset_seg_id, std::vector<uint32_t>* deletes);

private:
};

} // namespace starrocks