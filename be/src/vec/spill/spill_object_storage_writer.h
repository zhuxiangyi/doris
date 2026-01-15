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

#pragma once

#include "vec/spill/spill_writer_base.h"
#include "io/fs/file_writer.h"
#include "olap/storage_policy.h"
#include "runtime/workload_management/resource_context.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {

// 对象存储 SpillWriter
class SpillObjectStorageWriter : public SpillWriterBase {
public:
    SpillObjectStorageWriter(std::shared_ptr<ResourceContext> resource_context,
                             RuntimeProfile* profile, int64_t id, size_t batch_size,
                             StorageResource storage_resource, const std::string& remote_path)
            : storage_resource_(std::move(storage_resource)),
              stream_id_(id),
              batch_size_(batch_size),
              remote_path_(remote_path),
              _resource_ctx(std::move(resource_context)) {
        RuntimeProfile* common_profile = profile->get_child("CommonCounters");
        DCHECK(common_profile != nullptr);
        _memory_used_counter = common_profile->get_counter("MemoryUsage");
    }

    Status open() override;

    Status close() override;

    Status write(RuntimeState* state, const Block& block, size_t& written_bytes) override;

    int64_t get_id() const override { return stream_id_; }

    int64_t get_written_bytes() const override { return total_written_bytes_; }

    const std::string& get_file_path() const override { return remote_path_; }

    void set_counters(RuntimeProfile* operator_profile) override;

private:
    Status _write_internal(const Block& block, size_t& written_bytes);

    std::atomic_bool closed_ = false;
    int64_t stream_id_;
    size_t batch_size_;
    size_t max_sub_block_size_ = 0;
    std::string remote_path_; // 对象存储路径，如 s3://bucket/path/to/file
    StorageResource storage_resource_;
    std::unique_ptr<doris::io::FileWriter> file_writer_;

    size_t written_blocks_ = 0;
    int64_t total_written_bytes_ = 0;
    std::string meta_;

    RuntimeProfile::Counter* _write_file_timer = nullptr;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    RuntimeProfile::Counter* _write_block_counter = nullptr;
    RuntimeProfile::Counter* _write_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _write_file_total_size = nullptr;
    RuntimeProfile::Counter* _write_file_current_size = nullptr;
    RuntimeProfile::Counter* _write_rows_counter = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};
using SpillObjectStorageWriterUPtr = std::unique_ptr<SpillObjectStorageWriter>;
} // namespace vectorized
} // namespace doris
#include "common/compile_check_end.h"

