// 这是一个简化的实现示例，展示如何实现 SpillObjectStorageWriter 的关键部分

#include "vec/spill/spill_object_storage_writer.h"
#include "cloud/storage_resource.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"

namespace doris::vectorized {

// 关键实现：如何从 Storage Vault 创建文件写入器
Status SpillObjectStorageWriter::open() {
    if (file_writer_) {
        return Status::OK();
    }
    
    // 1. 从 StorageResource 获取远程文件系统
    auto fs = storage_resource_.remote_fs();
    if (!fs) {
        return Status::InternalError("Failed to get remote filesystem from storage resource");
    }
    
    // 2. 创建文件写入器
    // StorageResource 已经包含了 S3/HDFS 的配置信息
    io::FileWriterOptions opts;
    RETURN_IF_ERROR(fs->create_file(remote_path_, &file_writer_, &opts));
    
    LOG(INFO) << "Opened spill object storage writer, path: " << remote_path_;
    return Status::OK();
}

// 关键实现：写入数据（与本地磁盘类似，但使用对象存储的文件写入器）
Status SpillObjectStorageWriter::_write_internal(const Block& block, size_t& written_bytes) {
    // 序列化和压缩逻辑与 SpillLocalWriter 完全相同
    // 区别在于 file_writer_ 是 S3FileWriter 或 HdfsFileWriter
    
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    Status status;
    std::string buff;
    int64_t buff_size {0};
    
    if (block.rows() > 0) {
        // 1. 序列化 Block
        {
            PBlock pblock;
            SCOPED_TIMER(_serialize_timer);
            int64_t compressed_time = 0;
            
            status = block.serialize(
                BeExecVersionManager::get_newest_version(),
                &pblock,
                &uncompressed_bytes,
                &compressed_bytes,
                &compressed_time,
                segment_v2::CompressionTypePB::ZSTD);
            RETURN_IF_ERROR(status);
            
            if (!pblock.SerializeToString(&buff)) {
                return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                    "serialize spill data error. [path={}]", remote_path_);
            }
            buff_size = buff.size();
        }
        
        // 2. 写入对象存储（不需要容量检查）
        {
            SCOPED_TIMER(_write_file_timer);
            status = file_writer_->append(buff);  // 这里会调用 S3FileWriter::append()
            RETURN_IF_ERROR(status);
        }
        
        // 3. 更新统计信息
        written_bytes += buff_size;
        total_written_bytes_ += buff_size;
        meta_.append((const char*)&total_written_bytes_, sizeof(size_t));
        ++written_blocks_;
    }
    
    return status;
}

} // namespace doris::vectorized

// ============================================
// SpillStreamManager 中的关键实现
// ============================================

Status SpillStreamManager::register_spill_stream_to_object_storage(
    RuntimeState* state, SpillStreamSPtr& spill_stream,
    const std::string& query_id, const std::string& operator_name,
    int32_t node_id, int32_t batch_rows, size_t batch_bytes,
    RuntimeProfile* operator_profile,
    const std::string& vault_id) {
    
    // 1. 获取 StorageResource
    auto* engine = ExecEnv::GetInstance()->storage_engine().to_cloud();
    if (!engine) {
        return Status::InternalError("Cloud storage engine not available");
    }
    
    auto opt_resource = engine->get_storage_resource(vault_id);
    if (!opt_resource.has_value()) {
        return Status::InternalError("Failed to get storage resource for vault_id: {}", vault_id);
    }
    StorageResource storage_resource = opt_resource.value();
    
    // 2. 构建对象存储路径
    uint64_t id = id_++;
    auto fs = storage_resource.remote_fs();
    if (!fs) {
        return Status::InternalError("Failed to get remote filesystem");
    }
    
    // 路径格式: s3://bucket/spill/query_id/operator_name-node_id-task_id-stream_id/0
    std::string path_prefix = fmt::format("spill/{}/{}-{}-{}-{}", 
                                          query_id, operator_name,
                                          node_id, state->task_id(), id);
    std::string remote_path = fs->root_path() + "/" + path_prefix + "/0";
    
    // 3. 创建 SpillStream（使用对象存储构造函数）
    spill_stream = std::make_shared<SpillStream>(
        state, id, storage_resource, remote_path, batch_rows, batch_bytes, operator_profile);
    
    RETURN_IF_ERROR(spill_stream->prepare());
    return Status::OK();
}

// ============================================
// 算子中的使用示例
// ============================================

// 在 SpillSortSinkLocalState::revoke_memory() 中
Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state,
                                              const std::shared_ptr<SpillContext>& spill_context) {
    // ... 现有代码 ...
    
    // 判断使用哪种存储
    bool use_object_storage = state->enable_spill_to_object_storage();
    std::string vault_id = state->spill_object_storage_vault_id();
    
    Status status;
    if (use_object_storage) {
        // 使用对象存储
        status = ExecEnv::GetInstance()->spill_stream_mgr()
            ->register_spill_stream_to_object_storage(
                state, _spilling_stream, print_id(state->query_id()), "sort", 
                _parent->node_id(), batch_size, state->spill_sort_batch_bytes(), 
                operator_profile(), vault_id);
    } else {
        // 使用本地磁盘（原有逻辑）
        status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            state, _spilling_stream, print_id(state->query_id()), "sort", 
            _parent->node_id(), batch_size, state->spill_sort_batch_bytes(), 
            operator_profile());
    }
    RETURN_IF_ERROR(status);
    
    // ... 后续代码保持不变 ...
}

