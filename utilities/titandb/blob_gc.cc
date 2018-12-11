#include "utilities/titandb/blob_gc.h"

#include "utilities/titandb/version.h"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<BlobFileMeta*>&& blob_files,
               TitanCFOptions&& _titan_cf_options)
    : inputs_(std::move(blob_files)),
      titan_cf_options_(std::move(_titan_cf_options)) {
  MarkFilesBeingGC(true);
}

BlobGC::~BlobGC() {
  if (current_ != nullptr) {
    current_->Unref();
  }
}

void BlobGC::SetInputVersion(ColumnFamilyHandle* cfh, Version* version) {
  cfh_ = cfh;
  current_ = version;

  current_->Ref();
}

ColumnFamilyData* BlobGC::GetColumnFamilyData() {
  auto* cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh_);
  return cfhi->cfd();
}

void BlobGC::MarkFilesBeingGC(bool flag) {
  for (auto& f : inputs_) {
    assert(flag == !f->being_gc);
    f->being_gc = flag;
  }
}

void BlobGC::AddOutputFile(BlobFileMeta* blob_file) {
  assert(blob_file->pending);
  blob_file->pending_gc = true;
  outputs_.push_back(blob_file);
}

void BlobGC::ReleaseGcFiles() {
  MarkFilesBeingGC(false);

  for (auto& f : outputs_) {
    f->pending = false;
    f->pending_gc = false;
  }
}

}  // namespace titandb
}  // namespace rocksdb
