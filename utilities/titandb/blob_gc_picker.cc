#include "utilities/titandb/blob_gc_picker.h"

namespace rocksdb {
namespace titandb {

BasicBlobGCPicker::BasicBlobGCPicker(TitanDBOptions db_options,
                                     TitanCFOptions cf_options)
    : db_options_(db_options), cf_options_(cf_options) {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  Status s;
  std::vector<BlobFileMeta*> blob_files;

  uint64_t batch_size = 0;
  ROCKS_LOG_INFO(db_options_.info_log, "blob file num:%lu gc score:%lu",
                 blob_storage->NumBlobFiles(), blob_storage->gc_score().size());
  for (auto& gc_score : blob_storage->gc_score()) {
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    assert(blob_file);

    ROCKS_LOG_INFO(db_options_.info_log,
                   "file number:%lu score:%f being_gc:%d pending:%d, "
                   "size:%lu discard:%lu mark_for_gc:%d mark_for_sample:%d",
                   blob_file->file_number, gc_score.score, blob_file->being_gc,
                   blob_file->pending, blob_file->file_size,
                   blob_file->discardable_size, blob_file->marked_for_gc,
                   blob_file->marked_for_sample);

    if (!CheckBlobFile(blob_file.get(), gc_score)) {
      ROCKS_LOG_INFO(db_options_.info_log, "file number:%lu no need gc",
                     blob_file->file_number);
      continue;
    }
    blob_files.push_back(blob_file.get());

    batch_size += blob_file->file_size;
    if (batch_size >= cf_options_.max_gc_batch_size) break;
  }

  if (blob_files.empty() || batch_size < cf_options_.min_gc_batch_size)
    return nullptr;

  return std::unique_ptr<BlobGC>(
      new BlobGC(std::move(blob_files), std::move(cf_options_)));
}

bool BasicBlobGCPicker::CheckBlobFile(BlobFileMeta* blob_file,
                                      const GCScore& gc_score) const {
  if (blob_file->pending) return false;
  if (blob_file->being_gc) return false;
  if (blob_file->pending_gc) return false;

  if (gc_score.score >= cf_options_.blob_file_discardable_ratio)
    blob_file->marked_for_sample = false;

  return true;
}

}  // namespace titandb
}  // namespace rocksdb
