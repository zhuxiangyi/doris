// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <memory>

#include "vec/spill/spill_object_storage_writer.h"
#include "olap/storage_policy.h"
#include "runtime/runtime_state.h"
#include "runtime/query_context.h"
#include "runtime/workload_management/resource_context.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "io/fs/file_system.h"
#include "io/fs/s3_file_system.h"

namespace doris::vectorized {

class SpillObjectStorageWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建测试用的 RuntimeProfile
        _profile = std::make_unique<RuntimeProfile>("test_profile");
        RuntimeProfile* common_profile = _profile->create_child("CommonCounters", true, true);
        common_profile->add_counter("MemoryUsage", TUnit::BYTES);
        
        RuntimeProfile* custom_profile = _profile->create_child("CustomCounters", true, true);
        custom_profile->add_counter("SpillWriteFileTime", TUnit::TIME_NS);
        custom_profile->add_counter("SpillWriteSerializeBlockTime", TUnit::TIME_NS);
        custom_profile->add_counter("SpillWriteBlockCount", TUnit::UNIT);
        custom_profile->add_counter("SpillWriteBlockBytes", TUnit::BYTES);
        custom_profile->add_counter("SpillWriteFileBytes", TUnit::BYTES);
        custom_profile->add_counter("SpillWriteFileCurrentBytes", TUnit::BYTES);
        custom_profile->add_counter("SpillWriteRows", TUnit::UNIT);
        
        // 创建 ResourceContext
        _resource_ctx = std::make_shared<ResourceContext>();
    }
    
    void TearDown() override {
        // 清理测试环境
    }
    
    // 创建测试 Block
    Block create_test_block(size_t rows) {
        Block block;
        auto int32_type = DataTypeFactory::instance().get("Int32");
        auto string_type = DataTypeFactory::instance().get("String");
        
        auto int32_col = int32_type->create_column();
        auto string_col = string_type->create_column();
        
        for (size_t i = 0; i < rows; ++i) {
            int32_col->insert(static_cast<Int32>(i));
            string_col->insert(std::to_string(i));
        }
        
        block.insert(ColumnWithTypeAndName(std::move(int32_col), int32_type, "id"));
        block.insert(ColumnWithTypeAndName(std::move(string_col), string_type, "name"));
        
        return block;
    }
    
    std::unique_ptr<RuntimeProfile> _profile;
    std::shared_ptr<ResourceContext> _resource_ctx;
};

// 注意：以下测试需要 mock StorageResource 和文件系统
// 实际测试中需要根据测试框架进行调整

TEST_F(SpillObjectStorageWriterTest, DISABLED_TestOpen) {
    // 测试打开对象存储写入器
    // 需要 mock StorageResource 和文件系统
    
    // StorageResource storage_resource = create_mock_storage_resource();
    // std::string remote_path = "s3://test-bucket/spill/test/0";
    // 
    // SpillObjectStorageWriter writer(
    //     _resource_ctx, _profile.get(), 1, 4096, storage_resource, remote_path);
    // 
    // writer.set_counters(_profile.get());
    // Status status = writer.open();
    // EXPECT_TRUE(status.ok());
}

TEST_F(SpillObjectStorageWriterTest, DISABLED_TestWrite) {
    // 测试写入 Block
    // 需要 mock StorageResource 和文件系统
    
    // StorageResource storage_resource = create_mock_storage_resource();
    // std::string remote_path = "s3://test-bucket/spill/test/0";
    // 
    // SpillObjectStorageWriter writer(
    //     _resource_ctx, _profile.get(), 1, 4096, storage_resource, remote_path);
    // 
    // writer.set_counters(_profile.get());
    // RETURN_IF_ERROR(writer.open());
    // 
    // Block block = create_test_block(1000);
    // size_t written_bytes = 0;
    // Status status = writer.write(nullptr, block, written_bytes);
    // EXPECT_TRUE(status.ok());
    // EXPECT_GT(written_bytes, 0);
}

TEST_F(SpillObjectStorageWriterTest, DISABLED_TestClose) {
    // 测试关闭和元数据写入
    // 需要 mock StorageResource 和文件系统
    
    // StorageResource storage_resource = create_mock_storage_resource();
    // std::string remote_path = "s3://test-bucket/spill/test/0";
    // 
    // SpillObjectStorageWriter writer(
    //     _resource_ctx, _profile.get(), 1, 4096, storage_resource, remote_path);
    // 
    // writer.set_counters(_profile.get());
    // RETURN_IF_ERROR(writer.open());
    // 
    // Block block = create_test_block(1000);
    // size_t written_bytes = 0;
    // RETURN_IF_ERROR(writer.write(nullptr, block, written_bytes));
    // 
    // Status status = writer.close();
    // EXPECT_TRUE(status.ok());
    // EXPECT_GT(writer.get_written_bytes(), 0);
}

TEST_F(SpillObjectStorageWriterTest, DISABLED_TestLargeBlock) {
    // 测试大 Block 的分批写入
    // 需要 mock StorageResource 和文件系统
    
    // StorageResource storage_resource = create_mock_storage_resource();
    // std::string remote_path = "s3://test-bucket/spill/test/0";
    // 
    // SpillObjectStorageWriter writer(
    //     _resource_ctx, _profile.get(), 1, 4096, storage_resource, remote_path);
    // 
    // writer.set_counters(_profile.get());
    // RETURN_IF_ERROR(writer.open());
    // 
    // // 创建大于 batch_size 的 Block
    // Block block = create_test_block(10000);
    // size_t written_bytes = 0;
    // Status status = writer.write(nullptr, block, written_bytes);
    // EXPECT_TRUE(status.ok());
}

} // namespace doris::vectorized

