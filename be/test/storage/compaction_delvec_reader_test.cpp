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

#include "fs/fs.h"

namespace starrocks {

class CompactionDelvecReaderTest : public testing::Test {
public:
    CompactionDelvecReaderTest() : TestBase(kTestDirectory) {}

protected:
    constexpr static const char* const kTestDirectory = "test_compaction_delvec_reader";

    void SetUp() override {}

    void TearDown() override { (void)fs::remove_all(kTestDirectory); }

    void generate_rssid_rowids(std::vector<uint64_t>* rssid_rowids, uint64_t start, size_t count) {
        for (uint64_t i = start; i < count; i++) {
            rssid_rowids->push_back(i);
        }
    }
};

TEST_P(CompactionDelvecReaderTest, test_write_read) {
    const std::string filename = kTestDirectory + "/test_write_read.crm";
    RowsMapperBuilder buidler(filename);
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(rssid_rowids, 0, 1000);
    ASSERT_OK(buidler.append(rssid_rowids));
    rssid_rowids.clear();
    generate_rssid_rowids(rssid_rowids, 1000, 2000);
    ASSERT_OK(buidler.append(rssid_rowids));
    builder.finalize_segment(3000);
    ASSERT_OK(builder.finalize());

    // read from file
    uint32_t counter = 0;
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));
    for (; !iterator.end_of_file() && iterator.status().ok(); iterator.next()) {
        std::vector<RowsMapper> rows_mapper;
        iterator.value(&rows_mapper);
        ASSERT_TRUE(!rows_mapper.empty());
        for (const auto& each : rows_mapper) {
            ASSERT_TRUE(each.input_rssid == 0);
            ASSERT_TRUE(each.input_rowid == counter);
            ASSERT_TRUE(each.output_segment_id == 0);
            ASSERT_TRUE(each.output_rowid == counter);
            counter++;
        }
    }
    ASSERT_TRUE(counter == 3000);
    ASSERT_OK(iterator.status());
}

} // namespace starrocks