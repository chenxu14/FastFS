/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

extern "C" {
#include <CUnit/Basic.h>
#include <CUnit/CUnit.h>

#include "rte_mempool.h"
#include "spdk_internal/mock.h"
}
#include "core/FastFS.h"

static uint32_t BLOCK_SIZE = 4096;
static bool NO_INODES = false;
static bool FSYNC = false;
static bool LARGE_FILE = false;
static uint32_t LARGE_FILE_ID = 0;
static int LARGE_FILE_SIZE = 2000;

static bool LARGE_DIR = false;
static uint32_t LARGE_DIR_ID = 0;
static int LARGE_DIR_SIZE = 2000;

// format(0) -> mount(1) -> loadInodes(2) -> loadDentry(3) -> replayJournal(4)
static int READ_STAGE = 0;
// format(0) -> ckptINodes(1) -> ckptDentry(2) -> writeSuperBlock(3)
static int WRITE_STAGE = 0;

size_t spdk_bdev_get_buf_align(const struct spdk_bdev *bdev) { return 1; }
struct rte_mempool *rte_mempool_create(const char *name,
                                       unsigned n,
                                       unsigned elt_size,
                                       unsigned cache_size,
                                       unsigned private_data_size,
                                       rte_mempool_ctor_t *mp_init,
                                       void *mp_init_arg,
                                       rte_mempool_obj_cb_t *obj_init,
                                       void *obj_init_arg,
                                       int socket_id,
                                       unsigned flags) {
  return nullptr;
}
void *spdk_mempool_get(struct spdk_mempool *mp) { return malloc(sizeof(FastInode)); }
void spdk_mempool_put(struct spdk_mempool *mp, void *ele) { free(ele); }
void *spdk_malloc(size_t size, size_t align, uint64_t *unused, int numa_id, uint32_t flags) { return malloc(size); }
void spdk_free(void *buf) { free(buf); }
void *spdk_realloc(void *buf, size_t size, size_t align) { return realloc(buf, size); }
void *spdk_dma_zmalloc_socket(size_t size, size_t align, uint64_t *unused, int numa_id) { return malloc(size); }
void spdk_dma_free(void *buf) { free(buf); }
struct spdk_poller *spdk_poller_register_named(spdk_poller_fn fn, void *arg, uint64_t period, const char *name) {
  return nullptr;
}
uint32_t spdk_bdev_get_write_unit_size(const struct spdk_bdev *bdev) { return 1; }
uint32_t spdk_bdev_get_block_size(const struct spdk_bdev *bdev) { return BLOCK_SIZE; }
uint64_t spdk_bdev_get_num_blocks(const struct spdk_bdev *bdev) { return 5000; }

static INodeCache mocked(0, MemAllocator<FastInode>(0, 1));

static void mockFile(FastInode *file) {
  file->parentId_ = 0;
  file->size_ = 0;
  file->mode_ = 493;
}

static void mockFsync(ByteBuffer *buffer, int ino, int extentId) {
  buffer->putByte(4);         // opType
  buffer->write<int32_t>(22); // opSize
  buffer->write<uint32_t>(ino);
  buffer->write<uint64_t>(BLOCK_SIZE); // file size
  buffer->write<uint16_t>(1);          // dirty count
  buffer->write<uint32_t>(0);          // extent index
  buffer->write<uint32_t>(extentId);
}

static void mockDeletes(ByteBuffer *buffer, int start, int end) {
  DeleteContext deleteCtx;
  deleteCtx.recursive = true;
  for (int i = start; i < end; i += 2) {
    deleteCtx.parentId = 0;
    deleteCtx.name = "dir_" + std::to_string(i);
    buffer->putByte(2 /*type*/);
    buffer->write<int32_t>(6 + deleteCtx.name.size());
    deleteCtx.serialize(buffer);
  }
}

static void mockCreates(ByteBuffer *buffer, int start, int end) {
  CreateContext createCtx;
  createCtx.mode = 493;
  for (int i = start; i < end; i++) {
    createCtx.ino = i;
    if (i % 2 == 0) {
      createCtx.parentId = 0;
      createCtx.name = "dir_" + std::to_string(i);
      createCtx.type = FASTFS_DIR;

    } else {
      // mocked file has no extents
      createCtx.parentId = i - 1;
      createCtx.name = "file";
      createCtx.type = FASTFS_REGULAR_FILE;
    }
    buffer->putByte(0 /*type*/);
    buffer->write<int32_t>(14 + createCtx.name.size());
    createCtx.serialize(buffer);
  }
}

int spdk_bdev_read(struct spdk_bdev_desc *desc,
                   struct spdk_io_channel *ch,
                   void *buf,
                   uint64_t offset,
                   uint64_t nbytes,
                   spdk_bdev_io_completion_cb cb,
                   void *cb_arg) {
  if (READ_STAGE == 0) { // format
    READ_STAGE++;
    cb(nullptr, false, cb_arg);
    return 0;
  } else {
    ByteBuffer *buffer = reinterpret_cast<ByteBuffer *>(cb_arg);
    fs_context_t &ctx = FastFS::fs_context;
    switch (READ_STAGE) {
    case 1: { // mount
      // mock super block
      ctx.superBlock.journalLoc = 1;
      ctx.superBlock.journalSkipBlocks = 1;
      ctx.superBlock.journalSkipOps = 4;
      ctx.superBlock.ckptInodesLoc = 2;
      ctx.superBlock.ckptDentryLoc = 3;
      ctx.superBlock.lastTxid = 100;
      ctx.superBlock.serialize(buffer);
      buffer->position(0);
      break;
    }
    case 2: { // loadInodes
      // mock INodes
      buffer->write<uint32_t>(0);  // nextExtent
      buffer->write<uint32_t>(10); // numOps
      INodeFile inodeProto;
      inodeProto.size = 0;
      inodeProto.mode = 0;
      for (int i = 10; i < 20; i++) {
        inodeProto.ino = i;
        if (i % 2 == 0) {
          inodeProto.parent_id = 0;
          inodeProto.type = FASTFS_DIR;
          inodeProto.name = "dir_" + std::to_string(i);
        } else {
          inodeProto.parent_id = i - 1;
          inodeProto.type = FASTFS_REGULAR_FILE;
          inodeProto.name = "file";
        }
        int size = INodeFile::kFixSize + inodeProto.name.size();
        buffer->write<int32_t>(size);
        inodeProto.serialize(buffer);
      }
      buffer->position(0);
      break;
    }
    case 3: { // loadDentry
      // mock dentry
      buffer->write<uint32_t>(0);  // nextExtent
      buffer->write<uint32_t>(11); // numOps
      for (int i = 10; i < 20; i++) {
        if (i % 2 == 0) {
          buffer->putByte(FASTFS_DIR);    // type
          buffer->write<uint32_t>(i);     // inodeId
          buffer->write<uint32_t>(1);     // child count
          buffer->write<uint32_t>(i + 1); // child inodeId
        } else {
          buffer->putByte(FASTFS_REGULAR_FILE); // type
          buffer->write<uint32_t>(i);           // inodeId
          buffer->write<uint32_t>(1);           // extents count
          buffer->write<uint32_t>(i);           // extentId
        }
      }
      // record root
      buffer->putByte(FASTFS_DIR); // type
      buffer->write<uint32_t>(0);  // inodeId
      buffer->write<uint32_t>(5);  // child count
      for (int i = 10; i < 20; i += 2) {
        buffer->write<uint32_t>(i); // child inodeId
      }
      buffer->position(0);
      break;
    }
    case 4: { // replayJournal
      // mock first block (will skip during mount)
      buffer->putByte(1 /*flag*/);
      buffer->write<uint32_t>(0 /*epoch*/);
      buffer->write<uint64_t>(94 /*txid*/);
      buffer->write<uint32_t>(6 /*num_ops*/);
      mockCreates(buffer, 10, 16);
      // mock second block (will skip 4 OPs during mount)
      buffer->position(BLOCK_SIZE);
      buffer->putByte(0 /*flag*/);
      buffer->write<uint32_t>(0 /*epoch*/);
      buffer->write<uint64_t>(100 /*txid*/);
      if (NO_INODES) {
        buffer->write<uint32_t>(4 /*create*/ + 5 /*delete*/);
        mockCreates(buffer, 16, 20);
        mockDeletes(buffer, 10, 20);
      } else if (FSYNC) {
        buffer->write<uint32_t>(6 /*create*/ + 2 /*fsync*/);
        mockCreates(buffer, 16, 22);
        mockFsync(buffer, 21 /*ino*/, 21 /*extentId*/);
        mockFsync(buffer, 21 /*ino*/, 22 /*extentId*/);
      } else {
        buffer->write<uint32_t>(8 /*num_ops*/);
        mockCreates(buffer, 16, 24);
      }
      buffer->position(0);
      break;
    }
    default: {
      CU_FAIL("has wrong read stage!");
      break;
    }
    }
    READ_STAGE++;
  }
  cb(nullptr, true, cb_arg);
  return 0;
}

int spdk_bdev_write(struct spdk_bdev_desc *desc,
                    struct spdk_io_channel *ch,
                    void *buf,
                    uint64_t offset,
                    uint64_t nbytes,
                    spdk_bdev_io_completion_cb cb,
                    void *cb_arg) {
  ByteBuffer *buffer = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(buffer->private_data);
  fs_context_t &ctx = FastFS::fs_context;
  switch (WRITE_STAGE) {
  case 0: {
    if (NO_INODES) {
      WRITE_STAGE = 3; // if only root, no need to do checkpoint
    } else {
      WRITE_STAGE++;
    }
    break;
  }
  case 1: { // ckptINodes
    uint32_t extentId = 0;
    uint32_t numOps = 0;
    buffer->pread<uint32_t>(0, extentId);
    buffer->pread<uint32_t>(4, numOps);
    if (LARGE_DIR || LARGE_FILE) {
      if (extentId > 0) {
        WRITE_STAGE = 1; // has more data to write
      } else {
        WRITE_STAGE++;
      }
    } else {
      // normal case
      CU_ASSERT_EQUAL(extentId, 0);
      // 1 root, 10 in checkpoint, 4 in journal
      CU_ASSERT_EQUAL(numOps, 15);
      WRITE_STAGE++;
    }
    break;
  }
  case 2: { // ckptDentry
    uint32_t extentId = 0;
    uint32_t numOps = 0;
    if (LARGE_DIR || LARGE_FILE) {
      buffer->position(0);
      CU_ASSERT(fastfs->checkpoint->parseExtent(buffer, extentId, mocked));
      if (extentId > 0) {
        WRITE_STAGE = 2; // has more data to write
      } else {
        WRITE_STAGE++;
        if (LARGE_DIR) {
          int size = mocked[LARGE_DIR_ID].children_->size();
          CU_ASSERT_EQUAL(size, LARGE_DIR_SIZE);
        } else {
          int size = mocked[LARGE_FILE_ID].extents_->size();
          CU_ASSERT_EQUAL(size, LARGE_FILE_SIZE);
        }
      }
    } else {
      // normal case
      buffer->pread<uint32_t>(0, extentId);
      buffer->pread<uint32_t>(4, numOps);
      CU_ASSERT_EQUAL(extentId, 0);
      CU_ASSERT_EQUAL(numOps, 13); // 2 file has no extents
      WRITE_STAGE++;
    }
    break;
  }
  case 3: { // writeSuperBlock
    // no new OP add in journal
    CU_ASSERT_EQUAL(ctx.superBlock.lastTxid, 100);
    CU_ASSERT_EQUAL(ctx.superBlock.journalSkipBlocks, 1);
    WRITE_STAGE++;
    break;
  }
  default: {
    CU_FAIL("has wrong write stage!");
    break;
  }
  }
  cb(nullptr, true, cb_arg);
  return 0;
}

static void ckpt_complete(FastFS *fastfs, int code) {
  CU_ASSERT_EQUAL(code, 0);
  auto &ckpt = *fastfs->checkpoint;
  fs_context_t &ctx = FastFS::fs_context;
  if (NO_INODES) {
    CU_ASSERT_EQUAL(ckpt.inodeExtents, 0);
    CU_ASSERT_EQUAL(ckpt.dentryExtents, 0);
    CU_ASSERT_EQUAL(ctx.superBlock.ckptInodesLoc, 0);
    CU_ASSERT_EQUAL(ctx.superBlock.ckptDentryLoc, 0);
    // plus 5 delete OPs in journal
    CU_ASSERT_EQUAL(ctx.superBlock.journalSkipOps, 4 + 5);
  } else if (LARGE_DIR || LARGE_FILE) {
    CU_ASSERT(ckpt.dentryExtents > 1);
  } else {
    CU_ASSERT(ckpt.inodeExtents == 1);
    CU_ASSERT(ckpt.dentryExtents == 1);
    CU_ASSERT(ctx.superBlock.ckptInodesLoc > 0);
    CU_ASSERT(ctx.superBlock.ckptDentryLoc > 0);
    // plus 4 create OPs in journal
    CU_ASSERT_EQUAL(ctx.superBlock.journalSkipOps, 4 + 4);
  }
}

static void mount_complete(FastFS *fastfs, int code) { CU_ASSERT_FATAL(code == 0 && fastfs != nullptr); }

static void do_mount(FastFS &fastfs, uint32_t extentSize, uint32_t inodes = 128, uint32_t files = 128) {
  fastfs.format(extentSize, mount_complete);
  fastfs.mount(mount_complete, inodes, files);
  CU_ASSERT(fastfs.checkpoint != nullptr);
}

static void test_large_dir(void) {
  READ_STAGE = 0;
  WRITE_STAGE = 0;
  LARGE_DIR = true;
  FastFS fastfs("Malloc0");
  fs_context_t &ctx = FastFS::fs_context;
  do_mount(fastfs, BLOCK_SIZE * 2, 2048);

  // mock 1000 file under large dir
  LARGE_DIR_ID = ctx.inodeAllocator->allocate();
  FastInode &largeDir = (*fastfs.inodes)[LARGE_DIR_ID];
  largeDir.create(LARGE_DIR_ID, 0, "largeDir", FASTFS_DIR);
  mockFile(&largeDir);

  for (int i = 0; i < LARGE_DIR_SIZE; i++) {
    uint32_t ino = ctx.inodeAllocator->allocate();
    FastInode &file = (*fastfs.inodes)[ino];
    file.create(ino, 0, "file_" + std::to_string(i), FASTFS_REGULAR_FILE);
    mockFile(&file);
    largeDir.children_->insert(ino);
  }
  mocked = *fastfs.inodes;
  // clear children to verify FastCkpt::parseExtent worked
  mocked[LARGE_DIR_ID].children_ = new std::set<uint32_t>();

  // do checkpoint
  fastfs.checkpoint->checkpoint(ckpt_complete);
  LARGE_DIR = false;
}

static void test_large_file(void) {
  READ_STAGE = 0;
  WRITE_STAGE = 0;
  LARGE_FILE = true;
  FastFS fastfs("Malloc0");
  fs_context_t &ctx = FastFS::fs_context;
  do_mount(fastfs, BLOCK_SIZE * 2);

  // mock one file with 2000 extents
  LARGE_FILE_ID = ctx.inodeAllocator->allocate();
  FastInode &file = (*fastfs.inodes)[LARGE_FILE_ID];
  file.create(LARGE_FILE_ID, 0, "largeFile", FASTFS_REGULAR_FILE);
  mockFile(&file);
  for (int i = 0; i < LARGE_FILE_SIZE; i++) {
    file.extents_->emplace_back(100 + i);
  }

  mocked = *fastfs.inodes;
  // clear extents to verify FastCkpt::parseExtent worked
  mocked[LARGE_FILE_ID].extents_ = new std::vector<uint32_t>();

  // do checkpoint
  fastfs.checkpoint->checkpoint(ckpt_complete);
  LARGE_FILE = false;
}

static void test_normal(void) {
  READ_STAGE = 0;
  WRITE_STAGE = 0;
  FastFS fastfs("Malloc0");
  do_mount(fastfs, BLOCK_SIZE * 2);
  BitsAllocator *allocator = FastFS::fs_context.inodeAllocator;
  CU_ASSERT_EQUAL(fastfs.journal->extents_.front(), 1);
  CU_ASSERT_EQUAL(fastfs.journal->txid, 100);

  // total 2500, superBlock 1, journal 1, checkpoint 2, file 5
  // 2 files has no extents
  CU_ASSERT_EQUAL(FastFS::fs_context.allocator->getFree(), 2491);

  // total 14, first block skip 6, second block skip 4
  CU_ASSERT_EQUAL(fastfs.journal->replayOps, 4);

  // 1 root, 10 in ckpt, 4 in journal
  CU_ASSERT(allocator->getAllocated() == 15);

  // 5 in ckpt, 2 in journal
  CU_ASSERT_EQUAL(fastfs.root->children_->size(), 7);

  // do checkpoint
  fastfs.checkpoint->checkpoint(ckpt_complete);
}

static void test_no_inodes(void) {
  READ_STAGE = 0;
  WRITE_STAGE = 0;
  NO_INODES = true;
  FastFS fastfs("Malloc0");
  do_mount(fastfs, BLOCK_SIZE * 2);
  BitsAllocator *allocator = FastFS::fs_context.inodeAllocator;
  // only root left
  CU_ASSERT(allocator->getAllocated() == 1);
  CU_ASSERT_EQUAL(fastfs.root->children_->size(), 0);

  // total 15, first block skip 6, second block skip 4
  CU_ASSERT_EQUAL(fastfs.journal->replayOps, 5);

  // do checkpoint
  fastfs.checkpoint->checkpoint(ckpt_complete);

  // total 2500, superBlock 1, journal 1
  CU_ASSERT_EQUAL(FastFS::fs_context.allocator->getFree(), 2498);
  NO_INODES = false;
}

static void test_fsync_record(void) {
  READ_STAGE = 0;
  WRITE_STAGE = 0;
  FSYNC = true;
  FastFS fastfs("Malloc0");
  do_mount(fastfs, BLOCK_SIZE * 2);
  // total 2500, superBlock 1, journal 1, checkpoint 2
  // 5 file in ckpt, 1 file in journal
  CU_ASSERT_EQUAL(FastFS::fs_context.allocator->getFree(), 2490);
  FSYNC = false;
}

int main() {
  if (CUE_SUCCESS != CU_initialize_registry()) {
    return CU_get_error();
  }

  CU_pSuite suite = CU_add_suite("FastCkpt Tests", NULL, NULL);
  if (suite == NULL) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  if (CU_add_test(suite, "large dir", test_large_dir) == NULL ||
      CU_add_test(suite, "large file", test_large_file) == NULL ||
      CU_add_test(suite, "normal case", test_normal) == NULL ||
      CU_add_test(suite, "fsync record", test_fsync_record) == NULL ||
      CU_add_test(suite, "only root inode", test_no_inodes) == NULL) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  CU_basic_set_mode(CU_BRM_VERBOSE);
  CU_basic_run_tests();

  CU_cleanup_registry();
  return CU_get_number_of_failures();
}
