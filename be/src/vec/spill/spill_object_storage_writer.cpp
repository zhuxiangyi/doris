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

#include "vec/spill/spill_object_storage_writer.h"

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/pretty_printer.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status SpillObjectStorageWriter::open() {
    if (file_writer_) {
        return Status::OK();
    }

    // 从 StorageResource 获取远程文件系统
    if (!storage_resource_.fs) {
        return Status::InternalError("Failed to get remote filesystem from storage resource");
    }

    // 创建文件写入器
    io::FileWriterOptions opts;
    RETURN_IF_ERROR(storage_resource_.fs->create_file(remote_path_, &file_writer_, &opts));

    LOG(INFO) << "Opened spill object storage writer, path: " << remote_path_;
    return Status::OK();
}

Status SpillObjectStorageWriter::close() {
    if (closed_ || !file_writer_) {
        return Status::OK();
    }
    closed_ = true;

    // 写入元数据
    meta_.append((const char*)&max_sub_block_size_, sizeof(max_sub_block_size_));
    meta_.append((const char*)&written_blocks_, sizeof(written_blocks_));

    {
        SCOPED_TIMER(_write_file_timer);
        RETURN_IF_ERROR(file_writer_->append(meta_));
    }

    total_written_bytes_ += meta_.size();
    COUNTER_UPDATE(_write_file_total_size, meta_.size());
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(meta_.size());
    }
    if (_write_file_current_size) {
        COUNTER_UPDATE(_write_file_current_size, meta_.size());
    }
    ExecEnv::GetInstance()->spill_stream_mgr()->update_spill_write_bytes(meta_.size());

    RETURN_IF_ERROR(file_writer_->close());

    file_writer_.reset();
    return Status::OK();
}

Status SpillObjectStorageWriter::write(RuntimeState* state, const Block& block, size_t& written_bytes) {
    written_bytes = 0;
    DCHECK(file_writer_);
    auto rows = block.rows();
    COUNTER_UPDATE(_write_rows_counter, rows);
    COUNTER_UPDATE(_write_block_bytes_counter, block.bytes());
    // file format: block1, block2, ..., blockn, meta
    if (rows <= batch_size_) {
        return _write_internal(block, written_bytes);
    } else {
        auto tmp_block = block.clone_empty();
        const auto& src_data = block.get_columns_with_type_and_name();

        for (size_t row_idx = 0; row_idx < rows && !state->is_cancelled();) {
            tmp_block.clear_column_data();

            const auto& dst_data = tmp_block.get_columns_with_type_and_name();

            size_t block_rows = std::min(rows - row_idx, batch_size_);
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t col_idx = 0; col_idx < block.columns(); ++col_idx) {
                    dst_data[col_idx].column->assume_mutable()->insert_range_from(
                            *src_data[col_idx].column, row_idx, block_rows);
                }
            });

            int64_t tmp_blcok_mem = tmp_block.allocated_bytes();
            COUNTER_UPDATE(_memory_used_counter, tmp_blcok_mem);
            Defer defer {[&]() { COUNTER_UPDATE(_memory_used_counter, -tmp_blcok_mem); }};
            RETURN_IF_ERROR(_write_internal(tmp_block, written_bytes));

            row_idx += block_rows;
        }
        return Status::OK();
    }
}

Status SpillObjectStorageWriter::_write_internal(const Block& block, size_t& written_bytes) {
    size_t uncompressed_bytes = 0, compressed_bytes = 0;

    Status status;
    std::string buff;
    int64_t buff_size {0};

    if (block.rows() > 0) {
        {
            PBlock pblock;
            SCOPED_TIMER(_serialize_timer);
            int64_t compressed_time = 0;
            status = block.serialize(
                    BeExecVersionManager::get_newest_version(), &pblock, &uncompressed_bytes,
                    &compressed_bytes, &compressed_time,
                    segment_v2::CompressionTypePB::ZSTD); // ZSTD for better compression ratio
            RETURN_IF_ERROR(status);
            int64_t pblock_mem = pblock.ByteSizeLong();
            COUNTER_UPDATE(_memory_used_counter, pblock_mem);
            Defer defer {[&]() { COUNTER_UPDATE(_memory_used_counter, -pblock_mem); }};
            if (!pblock.SerializeToString(&buff)) {
                return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                        "serialize spill data error. [path={}]", remote_path_);
            }
            buff_size = buff.size();
            COUNTER_UPDATE(_memory_used_counter, buff_size);
            Defer defer2 {[&]() { COUNTER_UPDATE(_memory_used_counter, -buff_size); }};
        }
        // 对象存储不需要容量检查（由 Storage Vault 和云服务商管理）

        {
            Defer defer {[&]() {
                if (status.ok()) {
                    ExecEnv::GetInstance()->spill_stream_mgr()->update_spill_write_bytes(buff_size);

                    written_bytes += buff_size;
                    max_sub_block_size_ = std::max(max_sub_block_size_, (size_t)buff_size);

                    meta_.append((const char*)&total_written_bytes_, sizeof(size_t));
                    COUNTER_UPDATE(_write_file_total_size, buff_size);
                    if (_resource_ctx) {
                        _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(
                                buff_size);
                    }
                    if (_write_file_current_size) {
                        COUNTER_UPDATE(_write_file_current_size, buff_size);
                    }
                    COUNTER_UPDATE(_write_block_counter, 1);
                    total_written_bytes_ += buff_size;
                    ++written_blocks_;
                }
            }};
            {
                SCOPED_TIMER(_write_file_timer);
                status = file_writer_->append(buff);
                RETURN_IF_ERROR(status);
            }
        }
    }

    return status;
}

void SpillObjectStorageWriter::set_counters(RuntimeProfile* operator_profile) {
    RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
    _write_file_timer = custom_profile->get_counter("SpillWriteFileTime");
    _serialize_timer = custom_profile->get_counter("SpillWriteSerializeBlockTime");
    _write_block_counter = custom_profile->get_counter("SpillWriteBlockCount");
    _write_block_bytes_counter = custom_profile->get_counter("SpillWriteBlockBytes");
    _write_file_total_size = custom_profile->get_counter("SpillWriteFileBytes");
    _write_file_current_size = custom_profile->get_counter("SpillWriteFileCurrentBytes");
    _write_rows_counter = custom_profile->get_counter("SpillWriteRows");
}

} // namespace doris::vectorized

