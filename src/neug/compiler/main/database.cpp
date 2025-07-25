#include "neug/compiler/main/database.h"

#include <thread>

#include "neug/compiler/extension/extension_manager.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/database_manager.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "neug/compiler/common/exception/exception.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/main/db_config.h"
#include "neug/compiler/storage/storage_extension.h"
#include "neug/compiler/storage/storage_manager.h"
#include "neug/compiler/transaction/transaction_manager.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::storage;
using namespace gs::transaction;

namespace gs {
namespace main {

SystemConfig::SystemConfig(uint64_t bufferPoolSize_, uint64_t maxNumThreads,
                           bool enableCompression, bool readOnly,
                           uint64_t maxDBSize, bool autoCheckpoint,
                           uint64_t checkpointThreshold)
    : maxNumThreads{maxNumThreads},
      enableCompression{enableCompression},
      readOnly{readOnly},
      autoCheckpoint{autoCheckpoint},
      checkpointThreshold{checkpointThreshold} {
  if (bufferPoolSize_ == -1u || bufferPoolSize_ == 0) {
#if defined(_WIN32)
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    GlobalMemoryStatusEx(&status);
    auto systemMemSize = (std::uint64_t) status.ullTotalPhys;
#else
    auto systemMemSize = static_cast<std::uint64_t>(sysconf(_SC_PHYS_PAGES)) *
                         static_cast<std::uint64_t>(sysconf(_SC_PAGESIZE));
#endif
    bufferPoolSize_ = static_cast<uint64_t>(
        BufferPoolConstants::DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM *
        static_cast<double>(
            std::min(systemMemSize, static_cast<uint64_t>(UINTPTR_MAX))));
    bufferPoolSize_ = static_cast<uint64_t>(
        std::min(static_cast<double>(bufferPoolSize_),
                 BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE *
                     BufferPoolConstants::DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM));
  }
  bufferPoolSize = bufferPoolSize_;
#ifndef __SINGLE_THREADED__
  if (maxNumThreads == 0) {
    this->maxNumThreads = std::thread::hardware_concurrency();
  }
#else
  this->maxNumThreads = 1;
#endif
  if (maxDBSize == -1u) {
    maxDBSize = BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE;
  }
  this->maxDBSize = maxDBSize;
}

static void getLockFileFlagsAndType(bool readOnly, bool createNew, int& flags,
                                    FileLockType& lock) {
  flags = readOnly ? FileFlags::READ_ONLY : FileFlags::WRITE;
  if (createNew && !readOnly) {
    flags |= FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS;
  }
  lock = readOnly ? FileLockType::READ_LOCK : FileLockType::WRITE_LOCK;
}

Database::Database(const SystemConfig& systemConfig) : dbConfig{systemConfig} {}

Database::Database(std::string_view databasePath, SystemConfig systemConfig)
    : dbConfig{systemConfig} {}

Database::Database(std::string_view databasePath, SystemConfig systemConfig,
                   construct_bm_func_t constructBMFunc)
    : dbConfig(systemConfig) {}

Database::~Database() {
  if (!dbConfig.readOnly && dbConfig.forceCheckpointOnClose) {
    try {
      ClientContext clientContext(this);
      transactionManager->checkpoint(clientContext);
    } catch (...) {}
  }
}

void Database::registerFileSystem(std::unique_ptr<FileSystem> fs) {
  vfs->registerFileSystem(std::move(fs));
}

void Database::addExtensionOption(std::string name, LogicalTypeID type,
                                  Value defaultValue, bool isConfidential) {
  extensionManager->addExtensionOption(name, type, std::move(defaultValue),
                                       isConfidential);
}

void Database::openLockFile() {}

void Database::initAndLockDBDir() {
  if (DBConfig::isDBPathInMemory(databasePath)) {
    if (dbConfig.readOnly) {
      throw Exception(
          "Cannot open an in-memory database under READ ONLY mode.");
    }
    return;
  }
  if (!vfs->fileOrPathExists(databasePath)) {
    if (dbConfig.readOnly) {
      throw Exception("Cannot create an empty database under READ ONLY mode.");
    }
    vfs->createDir(databasePath);
  }
  openLockFile();
}

uint64_t Database::getNextQueryID() {
  std::lock_guard<std::mutex> lock(queryIDGenerator.queryIDLock);
  return queryIDGenerator.queryID++;
}

}  // namespace main
}  // namespace gs
