#include "base_db_event_listener.h"

#include "blob_file_size_collector.h"

namespace rocksdb {
namespace titandb {

BlobFileChangeListener::BlobFileChangeListener(TitanDBImpl* db,
                                               port::Mutex* db_mutex,
                                               VersionSet* versions)
    : db_(db), db_mutex_(db_mutex), versions_(versions) {}

BlobFileChangeListener::~BlobFileChangeListener() {}

void BlobFileChangeListener::OnFlushCompleted(
    DB* /*db*/, const FlushJobInfo& flush_job_info) {
  std::set<uint64_t> outputs;

  const auto& tp = flush_job_info.table_properties;
  auto ucp_iter =
      tp.user_collected_properties.find(BlobFileSizeCollector::kPropertiesName);
  // this sst file doesn't contain any blob index
  if (ucp_iter == tp.user_collected_properties.end()) {
    return;
  }
  std::map<uint64_t, uint64_t> input_blob_files_size;
  Slice slice{ucp_iter->second};
  if (!BlobFileSizeCollector::Decode(&slice, &input_blob_files_size)) {
    fprintf(stderr, "BlobFileSizeCollector::Decode failed size:%lu\n",
            ucp_iter->second.size());
    abort();
  }
  for (const auto& input_bfs : input_blob_files_size) {
    outputs.insert(input_bfs.first);
  }

  {
    MutexLock l(db_mutex_);

    Version* current = versions_->current();
    current->Ref();
    auto bs = current->GetBlobStorage(flush_job_info.cf_id).lock();
    if (!bs) {
      fprintf(stderr, "Column family id:%u Not Found\n", flush_job_info.cf_id);
      current->Unref();
      return;
    }
    for (const auto& o : outputs) {
      auto file = bs->FindFile(o).lock();
      // maybe gced from last OnFlushCompleted
      if (!file) {
        continue;
      }
      // one blob file may contain value of multiple sst file
      //    assert(file->pending);
      file->pending = false;
    }
    current->Unref();
  }
}

void BlobFileChangeListener::OnCompactionCompleted(
    DB* /* db */, const CompactionJobInfo& ci) {
  std::map<uint64_t, int64_t> blob_files_size;
  std::set<uint64_t> outputs;
  std::set<uint64_t> inputs;
  auto calc_bfs = [&ci, &blob_files_size, &outputs, &inputs](
                      const std::vector<std::string>& files, int coefficient,
                      bool output) {
    for (const auto& file : files) {
      auto tp_iter = ci.table_properties.find(file);
      if (tp_iter == ci.table_properties.end()) {
        if (output) {
          fprintf(stderr, "can't find property for output\n");
          abort();
        }
        continue;
      }
      auto ucp_iter = tp_iter->second->user_collected_properties.find(
          BlobFileSizeCollector::kPropertiesName);
      // this sst file doesn't contain any blob index
      if (ucp_iter == tp_iter->second->user_collected_properties.end()) {
        continue;
      }
      std::map<uint64_t, uint64_t> input_blob_files_size;
      std::string s = ucp_iter->second;
      Slice slice{s};
      if (!BlobFileSizeCollector::Decode(&slice, &input_blob_files_size)) {
        fprintf(stderr, "BlobFileSizeCollector::Decode failed\n");
        abort();
      }
      for (const auto& input_bfs : input_blob_files_size) {
        if (output) {
          if (inputs.find(input_bfs.first) == inputs.end()) {
            outputs.insert(input_bfs.first);
          }
        } else {
          inputs.insert(input_bfs.first);
        }
        auto bfs_iter = blob_files_size.find(input_bfs.first);
        if (bfs_iter == blob_files_size.end()) {
          blob_files_size[input_bfs.first] = coefficient * input_bfs.second;
        } else {
          bfs_iter->second += coefficient * input_bfs.second;
        }
      }
    }
  };

  calc_bfs(ci.input_files, -1, false);
  calc_bfs(ci.output_files, 1, true);

  {
    MutexLock l(db_mutex_);
    Version* current = versions_->current();
    current->Ref();
    auto bs = current->GetBlobStorage(ci.cf_id).lock();
    if (!bs) {
      fprintf(stderr, "Column family id:%u Not Found\n", ci.cf_id);
      current->Unref();
      return;
    }
    for (const auto& o : outputs) {
      auto file = bs->FindFile(o).lock();
      if (!file) {
        fprintf(stderr, "OnCompactionCompleted get file failed\n");
        abort();
      }
      assert(file->pending);
      file->pending = false;
    }

    for (const auto& bfs : blob_files_size) {
      // blob file size < 0 means discardable size > 0
      if (bfs.second >= 0) {
        continue;
      }
      auto file = bs->FindFile(bfs.first).lock();
      if (!file) {
        // file has been gc out
        continue;
      }
      file->discardable_size += static_cast<uint64_t>(-bfs.second);
      assert(file->discardable_size > 0);
    }
    bs->ComputeGCScore();
    current->Unref();
    if (db_ != nullptr) {
      db_->AddToGCQueue(ci.cf_id);
      db_->MaybeScheduleGC();
    } else {
      fprintf(stderr, "This is a test\n");
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb
