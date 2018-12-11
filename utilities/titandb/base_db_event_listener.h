#pragma once

#include "rocksdb/listener.h"
#include "util/coding.h"
#include "utilities/titandb/db_impl.h"

namespace rocksdb {

namespace titandb {

class BlobFileChangeListener final : public EventListener {
 public:
  BlobFileChangeListener(TitanDBImpl* db, port::Mutex* db_mutex,
                         VersionSet* versions);
  ~BlobFileChangeListener();

  void OnFlushCompleted(DB* db, const FlushJobInfo& flush_job_info) override;

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override;

 private:
  rocksdb::titandb::TitanDBImpl* db_;
  rocksdb::port::Mutex* db_mutex_;
  rocksdb::titandb::VersionSet* versions_;
};

}  // namespace titandb

}  // namespace rocksdb
