/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

extern "C" {
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include "spdk_internal/mock.h"
#include "rte_mempool.h"
}
#include "core/FastFS.h"

static uint32_t BLOCK_SIZE = 4096;
static bool RANDOM_WRITE = false;
static bool SMALL_WRITE = false;
static bool WRITE_ERROR = false;
static bool FILE_READ = false;
static bool FORMAT_FS = false;
static bool MOUNT_FS = false;

struct read_write_task {
  int offset;
  int count;
  char* data;
  read_write_task(int off, int len) : offset(off), count(len) {
    data = new char[count];
    for (int i = 0; i < count; i++) {
      data[i] = i;
    }
  }
};
static read_write_task task(44960, 8384);
static read_write_task smallTask(41050, 4000);
// 3 extents, last writeLen is 4000
static read_write_task largeTask(44960, 57344);
// 2 block plus 4000
static read_write_task alignTask(4096, 12192);
static read_write_task alignSmallTask(4096, 4000);

size_t spdk_bdev_get_buf_align(const struct spdk_bdev *bdev) {
  return 1;
}
struct rte_mempool * rte_mempool_create(
    const char *name, unsigned n, unsigned elt_size, unsigned cache_size,
    unsigned private_data_size, rte_mempool_ctor_t *mp_init, void *mp_init_arg,
    rte_mempool_obj_cb_t *obj_init, void *obj_init_arg, int socket_id, unsigned flags) {
  return nullptr;
}
void* spdk_mempool_get(struct spdk_mempool *mp) {
  return malloc(sizeof(FastInode));
}
void spdk_mempool_put(struct spdk_mempool *mp, void *ele) {
  free(ele);
}
void *spdk_malloc(size_t size, size_t align, uint64_t *unused, int numa_id, uint32_t flags) {
  return malloc(size);
}
void spdk_free(void *buf) {
  free(buf);
}
void* spdk_realloc(void *buf, size_t size, size_t align) {
  return realloc(buf, size);
}
void* spdk_dma_zmalloc_socket(
    size_t size, size_t align, uint64_t *unused, int numa_id) {
  return malloc(size);
}
void spdk_dma_free(void *buf) {
  free(buf);
}
struct spdk_poller* spdk_poller_register_named(
    spdk_poller_fn fn, void *arg, uint64_t period, const char *name) {
  return nullptr;
}
uint32_t spdk_bdev_get_write_unit_size(const struct spdk_bdev *bdev) {
  return 1;
}
uint32_t spdk_bdev_get_block_size(const struct spdk_bdev *bdev) {
  return BLOCK_SIZE;
}
uint64_t spdk_bdev_get_num_blocks(const struct spdk_bdev *bdev) {
  return 100;
}
int spdk_bdev_read(
    struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
    void *buf, uint64_t offset, uint64_t nbytes,
    spdk_bdev_io_completion_cb cb, void *cb_arg) {
  if (FILE_READ) {
    ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
    for (uint32_t i = 0; i < buffer->remaining(); i++) {
      uint32_t index = buffer->position() + i;
      char val = buffer->mark_ + i;
      buffer->putByte(index, val);
    }
  } else if (FORMAT_FS) {
    // mock no super block yet
    cb(nullptr, false, cb_arg);
    return 0;
  } else if (MOUNT_FS) {
    MOUNT_FS = false; // avoid conflicts with reading journal
    ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
    fs_context_t& ctx = FastFS::fs_context;
    ctx.superBlock.serialize(buffer);
    buffer->position(0);
  }
  cb(nullptr, true, cb_arg);
  return 0;
}
int spdk_bdev_write(
    struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
    void *buf, uint64_t offset, uint64_t nbytes,
    spdk_bdev_io_completion_cb cb, void *cb_arg) {
  if (SMALL_WRITE) {
    SMALL_WRITE = false; // avoid conflicts with writing journal
    ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
    fs_op_context* ctx = reinterpret_cast<fs_op_context*>(buffer->private_data);
    WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
    CU_ASSERT(writeCtx->file != nullptr);

    // verify tail block
    ByteBuffer* tailBlock = writeCtx->file->tail_block;
    CU_ASSERT(tailBlock->position() > writeCtx->count);
    uint32_t start = tailBlock->position() - writeCtx->count;
    char* buf = tailBlock->p_buffer_ + start;
    char val = 0;
    uint32_t index = 0;
    for (; index < writeCtx->count; index++) {
      if (buf[index] != val++) {
        break;
      }
    }
    CU_ASSERT(index == writeCtx->count);

    // trigger callback
    cb(nullptr, true, cb_arg);
    // trigger FastFS::recordWrite
    ctx->fastfs->journal->pollEditOp();
  } else if (FORMAT_FS || MOUNT_FS || RANDOM_WRITE) {
    cb(nullptr, true, cb_arg);
  } else {
    FastJournal* journal = reinterpret_cast<FastJournal*>(cb_arg);
    journal->writeComplete(0);
  }
  return 0;
}
int spdk_bdev_writev(
    struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
    struct iovec *iov, int iovcnt, uint64_t offset, uint64_t len,
    spdk_bdev_io_completion_cb cb, void *cb_arg) {
  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  fs_op_context* ctx = reinterpret_cast<fs_op_context*>(buffer->private_data);
  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  CU_ASSERT((offset & (BLOCK_SIZE - 1)) == 0);
  CU_ASSERT((len & (BLOCK_SIZE - 1)) == 0);
  CU_ASSERT((iov[0].iov_len & (BLOCK_SIZE - 1)) == 0);

  if (WRITE_ERROR) {
    WRITE_ERROR = false;
    cb(nullptr, false, cb_arg);
    return 0;
  }

  if (iovcnt == 2) { // first extent
    // verify tail block
    CU_ASSERT(iov[0].iov_len == BLOCK_SIZE);
    char* tailBlock = (char*) iov[0].iov_base;
    CU_ASSERT(tailBlock != nullptr);
    uint32_t blockOffset = writeCtx->offset & (BLOCK_SIZE - 1);
    int tailBlockWrite = BLOCK_SIZE - blockOffset;
    char val = 0;
    uint32_t index = blockOffset;
    for (; index < BLOCK_SIZE; index++) {
      if (tailBlock[index] != val++) {
        break;
      }
    }
    CU_ASSERT(index == BLOCK_SIZE);
    // verify remaining
    CU_ASSERT((iov[1].iov_len & (BLOCK_SIZE - 1)) == 0);
    uint32_t toWrite = std::min(
        (uint32_t) iov[1].iov_len, (writeCtx->count - tailBlockWrite));
    char* extentBuf = (char*) iov[1].iov_base;
    CU_ASSERT(extentBuf != nullptr);
    bool correct = true;
    for (uint32_t i = 0; i < toWrite; i++) {
      if (extentBuf[i] != val++) {
        correct = false;
        break;
      }
    }
    CU_ASSERT(correct);
  }

  // trigger callback
  cb(nullptr, true, cb_arg);
  ctx->fastfs->journal->pollEditOp();
  return 0;
}

static void create_complete(void* cb_args, int code) {
  if (cb_args) {
    CreateContext* ctx = reinterpret_cast<CreateContext*>(cb_args);
    if (code == 0) {
      CU_ASSERT(ctx->ino != UINT32_MAX);
    }
    if (std::string(ctx->name) == "dir-not-exist") {
      CU_ASSERT(code == -2);
    }
  } else { // create recursive
    CU_ASSERT(code == 0);
  }
}

static void mount_complete(FastFS* fastfs, int code) {
  CU_ASSERT_FATAL(code == 0 && fastfs != nullptr);
}

static void do_mount(FastFS& fastfs, uint32_t extentSize,
    uint32_t inodes = 128, uint32_t files = 128) {
  FORMAT_FS = true;
  fastfs.format(extentSize, mount_complete);
  FORMAT_FS = false;

  MOUNT_FS = true;
  fastfs.mount(mount_complete, inodes, files);
  MOUNT_FS = false;
}

static void delete_complete(void* cb_args, int code) {
  CU_ASSERT(code == 0);
}

static void delete_failed(void* cb_args, int code) {
  CU_ASSERT(code == -3); // dir not empty
}

static bool mockCreate(FastFS& fs, const std::string& name,
    FileType type = FASTFS_REGULAR_FILE,
    uint32_t parentId = 0) {
  bool res = true;
  fs_op_context* ctx = fs.allocFsOp();
  CreateContext* createCtx = new (ctx->private_data) CreateContext();
  createCtx->parentId = parentId;
  createCtx->name = name.c_str();
  createCtx->mode = 493;
  createCtx->type = type;
  ctx->callback = create_complete;
  ctx->cb_args = createCtx;
  fs.create(*ctx);
  fs.journal->pollEditOp();
  if (createCtx->ino == UINT32_MAX) {
    res = false;
  }
  fs.freeFsOp(ctx);
  return res;
}

static void mockDelete(FastFS& fs, uint32_t pid, const std::string& name,
    bool recursive = false, bool shouldFail = false) {
  fs_op_context* ctx = fs.allocFsOp();
  DeleteContext* delCtx = new (ctx->private_data) DeleteContext();
  delCtx->parentId = pid;
  delCtx->name = name.c_str();
  delCtx->recursive = recursive;
  if (shouldFail) {
    ctx->callback = delete_failed;
  } else {
    ctx->callback = delete_complete;
  }
  ctx->cb_args = delCtx;
  fs.remove(*ctx);
  fs.journal->pollEditOp();
}

static void test_alloc_buffer(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  ByteBuffer* buffer = fs.allocReadBuffer(task.offset, task.count);
  CU_ASSERT(!buffer->alloc_);
  CU_ASSERT(buffer->position_ == 4000);
  CU_ASSERT(buffer->capacity_ == FastFS::fs_context.extentSize);
  buffer = fs.allocReadBuffer(alignTask.offset, alignTask.count);
  CU_ASSERT(!buffer->alloc_);
  CU_ASSERT(buffer->position_ == 0);
  CU_ASSERT(buffer->capacity_ == FastFS::fs_context.extentSize);
  buffer = fs.allocReadBuffer(largeTask.offset, largeTask.count);
  CU_ASSERT(buffer->alloc_);
  CU_ASSERT(buffer->position_ == 4000);
  CU_ASSERT(buffer->capacity_ == 61440); // (6 + 8 + 1) * 4096
}

static void test_mkdir(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 2);
  BitsAllocator* allocator = FastFS::fs_context.inodeAllocator;
  // ino 0 is reserved
  CU_ASSERT(allocator->getAllocated() == 1);
  std::string name = "dir";
  mockCreate(fs, name, FASTFS_DIR);
  CU_ASSERT(allocator->getAllocated() == 2);

  // target dir exist
  mockCreate(fs, name, FASTFS_DIR);
  CU_ASSERT(allocator->getAllocated() == 2);

  // parent dir not exist
  name = "dir-not-exist";
  mockCreate(fs, name, FASTFS_DIR, 100/*parentId*/);
  CU_ASSERT(allocator->getAllocated() == 2);

  // create recursive
  std::string path = "/parent/child";
  fs.createRecursive(path, create_complete, nullptr);
  fs.journal->pollEditOp(); // create parent
  fs.journal->pollEditOp(); // create child
  CU_ASSERT(fs.status(path) != nullptr);

  // path end with '/'
  path = "/parent/child2/";
  fs.createRecursive(path, create_complete, nullptr);
  fs.journal->pollEditOp(); // create child2
  CU_ASSERT(fs.status(path) != nullptr);
}

static void test_delete(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 2);
  BitsAllocator* allocator = FastFS::fs_context.inodeAllocator;

  std::string dirName = "dir";
  mockCreate(fs, dirName, FASTFS_DIR);
  FastInode* dirInode = fs.lookup(0, dirName);
  CU_ASSERT_FATAL(dirInode != nullptr);

  std::string fileName = "file";
  mockCreate(fs, fileName, FASTFS_REGULAR_FILE, dirInode->ino_);
  fileName = "file2";
  mockCreate(fs, fileName, FASTFS_REGULAR_FILE, dirInode->ino_);

  CU_ASSERT(allocator->getAllocated() == 4);
  CU_ASSERT(dirInode->children_->size() == 2);

  std::string delPath = "file2";
  mockDelete(fs, dirInode->ino_, delPath);
  CU_ASSERT(allocator->getAllocated() == 3);
  CU_ASSERT(dirInode->children_->size() == 1);

  delPath = "dir";
  mockDelete(fs, 0, delPath, false, true);
  CU_ASSERT(allocator->getAllocated() == 3);
  mockDelete(fs, 0, delPath, true/*recursive*/);
  CU_ASSERT(allocator->getAllocated() == 1);
}

static void test_open_close(void) {
  uint32_t count = 16;
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 2, count/*inodes*/, count/*files*/);
  BitsAllocator* allocator = FastFS::fs_context.inodeAllocator;

  std::string fileName = "file";
  std::string path = "/" + fileName;
  mockCreate(fs, fileName);
  CU_ASSERT(allocator->getAllocated() == 2);

  uint32_t free = FastFS::fs_context.fdAllocator->getFree();
  CU_ASSERT(free == count - 3); // 3 reserved
  int fd = fs.open(path, 1/*flag*/);
  CU_ASSERT(fd == 3); // start from 3
  free = FastFS::fs_context.fdAllocator->getFree();
  CU_ASSERT(free == count - 4);

  mockDelete(fs, 0, fileName, true);
  CU_ASSERT(allocator->getAllocated() == 2);
  CU_ASSERT((*fs.files)[fd].inode_->refCnts_ > 0);
  CU_ASSERT(fs.close(fd) == 0);
  // target inode's slot should free
  CU_ASSERT(allocator->getAllocated() == 1);
  free = FastFS::fs_context.fdAllocator->getFree();
  CU_ASSERT(free == count - 3);
  fd = fs.open(path, 0);
  CU_ASSERT(fd == -1); // inode deleted
}

static void test_fd_overflow(void) {
  uint32_t count = 16;
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 2, count/*inodes*/, count/*files*/);
  bool flag = true;
  int val = 3; // 0, 1 and 2 reserved
  for (uint32_t i = 0; i < count - 3; i++) {
    int fd = fs.open("/", 0);
    if (fd != val) {
      flag = false;
      break;
    }
    val++;
  }
  int fd = fs.open("/", 0);
  CU_ASSERT(fd == -2); // no free FD
  CU_ASSERT(fs.close(3) == 0);
  fd = fs.open("/", 0);
  CU_ASSERT(fd == 3); // FD 3 available
  CU_ASSERT(flag);
}

static void test_inode_overflow(void) {
  uint32_t count = 16;
  FastFS fs("Malloc0");
  bool flag = true;
  do_mount(fs, BLOCK_SIZE * 2, count/*inodes*/, count/*files*/);
  for (uint32_t i = 0; i < count - 1/*root*/; i++) {
    if (!mockCreate(fs, "file_" + std::to_string(i))) {
      flag = false;
      break;
    }
  }
  CU_ASSERT(flag);
  flag = mockCreate(fs, "overflow");
  CU_ASSERT(!flag);
  mockDelete(fs, 0, "file_0");
  flag = mockCreate(fs, "overflow");
  CU_ASSERT(flag);
}

static void write_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  WriteContext* ctx = reinterpret_cast<WriteContext*>(opCtx->private_data);
  if (code == 0) {
    CU_ASSERT(ctx->file->inode_->size_ == ctx->offset + ctx->count);
    auto* extents = ctx->file->inode_->extents_;
    for (auto& extent : ctx->writeExtents) {
      CU_ASSERT(extent.index < extents->size());
    }
    // verify tail block's data
    ByteBuffer* tailBlock = ctx->file->tail_block;
    char val = ctx->count - 1;
    bool correct = true;
    int start = 0;
    if (tailBlock->position() >= ctx->count) {
      start = tailBlock->position() - ctx->count;
    }
    for (int i = tailBlock->position() - 1; i >= start; i--) {
      if (tailBlock->p_buffer_[i] != val--) {
        correct = false;
        break;
      }
    }
    CU_ASSERT(correct);
  } else {
    CU_ASSERT(ctx->file->inode_->size_ == ctx->offset);
    CU_ASSERT(ctx->file->tail_block->position() == 0);
  }
}

static void read_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  ReadContext* ctx = reinterpret_cast<ReadContext*>(opCtx->private_data);
  CU_ASSERT(ctx->file!= nullptr);
}

static int createTestFile(FastFS& fs, const std::string& name, int size) {
  mockCreate(fs, name);
  // mock file
  FastInode* inode = fs.lookup(0, name);
  CU_ASSERT_FATAL(inode != nullptr);
  inode->size_ = size;
  int len = 0;
  while (len < size) {
    len += FastFS::fs_context.extentSize;
    uint32_t extentId = FastFS::fs_context.allocator->allocate();
    (*inode->extents_).emplace_back(extentId);
  }
  // do open
  std::string path = "/" + name;
  int fd = fs.open(path, F_MULTI_WRITE);
  CU_ASSERT(fd > 0);
  return fd;
}

static void truncate_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  opCtx->fastfs->freeFsOp(opCtx);
  CU_ASSERT(code == 0);
}

static void test_truncate(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 2);
  std::string fileName = "test_file";
  int fd = createTestFile(fs, fileName, BLOCK_SIZE * 8); // 4 extents
  uint32_t ino = (*fs.files)[fd].inode_->ino_;
  fs_op_context* ctx = fs.allocFsOp();
  TruncateContext* truncateCtx = new (ctx->private_data) TruncateContext();
  fileName = "/" + fileName;
  truncateCtx->ino = ino;
  truncateCtx->size = BLOCK_SIZE * 4; // 2 extents
  ctx->callback = truncate_complete;
  ctx->cb_args = ctx;
  fs.truncate(*ctx);
  fs.journal->pollEditOp();
  FastInode* inode = fs.status(fileName);
  CU_ASSERT(inode != nullptr);
  CU_ASSERT(inode->extents_->size() == 2);
  CU_ASSERT(inode->size_ == truncateCtx->size);
  // size not align
  fileName = "test_file2";
  fd = createTestFile(fs, fileName, BLOCK_SIZE * 8 + 1024); // 5 extents
  ino = (*fs.files)[fd].inode_->ino_;
  ctx = fs.allocFsOp();
  truncateCtx = new (ctx->private_data) TruncateContext();
  fileName = "/" + fileName;
  truncateCtx->ino = ino;
  truncateCtx->size = BLOCK_SIZE * 4 + 1024; // 3 extents
  ctx->callback = truncate_complete;
  ctx->cb_args = ctx;
  fs.truncate(*ctx);
  fs.journal->pollEditOp();
  inode = fs.status(fileName);
  CU_ASSERT(inode->extents_->size() == 3);
  CU_ASSERT(inode->size_ == truncateCtx->size);
  // size is 0
  fileName = "test_file3";
  fd = createTestFile(fs, fileName, BLOCK_SIZE * 8 + 1024); // 5 extents
  ino = (*fs.files)[fd].inode_->ino_;
  ctx = fs.allocFsOp();
  truncateCtx = new (ctx->private_data) TruncateContext();
  fileName = "/" + fileName;
  truncateCtx->ino = ino;
  truncateCtx->size = 0;
  ctx->callback = truncate_complete;
  ctx->cb_args = ctx;
  fs.truncate(*ctx);
  fs.journal->pollEditOp();
  inode = fs.status(fileName);
  CU_ASSERT(inode->extents_->size() == 1);
  CU_ASSERT(inode->size_ == 0);
}

static void writeFile(FastFS& fs, int fd, read_write_task& t) {
  fs_op_context* ctx = fs.allocFsOp();
  WriteContext* writeCtx = new (ctx->private_data) WriteContext();
  writeCtx->fd = fd;
  writeCtx->pwrite = true;
  writeCtx->offset = t.offset;
  writeCtx->write_buff = t.data;
  writeCtx->count = t.count;
  ctx->callback = write_complete;
  ctx->cb_args = ctx;
  fs.write(*ctx);
  fs.freeFsOp(ctx);
}

static void writeDirect(FastFS& fs, int fd, read_write_task& t) {
  fs_op_context* ctx = fs.allocFsOp();
  WriteContext* writeCtx = new (ctx->private_data) WriteContext();
  writeCtx->dirctWrite(&fs, fd, t.offset, t.count, t.data);
  ctx->callback = write_complete;
  ctx->cb_args = ctx;
  fs.write(*ctx);
  fs.freeFsOp(ctx);
  fs.freeBuffer(writeCtx->direct_buff);
}

static void readFile(FastFS& fs, int fd, read_write_task& t) {
  fs_op_context* ctx = fs.allocFsOp();
  ReadContext* readCtx = new (ctx->private_data) ReadContext();
  readCtx->fd = fd;
  readCtx->pread = true;
  readCtx->offset = t.offset;
  readCtx->count = t.count;
  readCtx->read_buff = t.data;
  ctx->callback = read_complete;
  ctx->cb_args = ctx;
  FILE_READ = true;
  fs.read(*ctx);
  bool correct = true;
  for (uint32_t i = 0; i < readCtx->count; i++) {
    if (readCtx->read_buff[i] != (char) i) {
      correct = false;
      break;
    }
  }
  CU_ASSERT(correct);
  fs.freeFsOp(ctx);
  FILE_READ = false;
}

static void readDirect(FastFS& fs, int fd, read_write_task& t) {
  fs_op_context* ctx = fs.allocFsOp();
  ReadContext* readCtx = new (ctx->private_data) ReadContext();
  readCtx->dirctRead(&fs, fd, t.offset, t.count);
  ctx->callback = read_complete;
  ctx->cb_args = ctx;
  FILE_READ = true;
  fs.read(*ctx);
  bool correct = true;
  readCtx->direct_buff->position(readCtx->direct_cursor);
  char val;
  int count = 0;
  for (uint32_t i = 0; i < readCtx->count; i++) {
    if (!readCtx->direct_buff->getByte(val) || val != (char) i) {
      correct = false;
      break;
    }
    count++;
  }
  CU_ASSERT(correct);
  fs.freeFsOp(ctx);
  fs.freeBuffer(readCtx->direct_buff);
  FILE_READ = false;
}

static void test_read_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", task.offset);
  writeFile(fs, fd, task);
  readFile(fs, fd, task);
  fs.close(fd);
}

static void test_direct_read_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", task.offset);
  writeDirect(fs, fd, task);
  readDirect(fs, fd, task);
  fs.close(fd);
}

static void test_read_write_small(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", smallTask.offset);
  SMALL_WRITE = true;
  writeFile(fs, fd, smallTask);
  SMALL_WRITE = false;
  readFile(fs, fd, smallTask);
  fs.close(fd);
}

static void test_direct_read_write_small(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", smallTask.offset);
  SMALL_WRITE = true;
  writeDirect(fs, fd, smallTask);
  SMALL_WRITE = false;
  readDirect(fs, fd, smallTask);
  fs.close(fd);
}

static void test_read_write_large(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", largeTask.offset);
  writeFile(fs, fd, largeTask);
  readFile(fs, fd, largeTask);
  fs.close(fd);
}

static void test_direct_read_write_large(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", largeTask.offset);
  writeDirect(fs, fd, largeTask);
  readDirect(fs, fd, largeTask);
  fs.close(fd);
}

static void test_align_read_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", alignTask.offset);
  writeFile(fs, fd, alignTask);
  readFile(fs, fd, alignTask);
  fs.close(fd);
}

static void test_align_small_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", alignSmallTask.offset);
  writeFile(fs, fd, alignSmallTask);
  readFile(fs, fd, alignSmallTask);
  fs.close(fd);
}

static void test_direct_align_read_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", alignTask.offset);
  writeDirect(fs, fd, alignTask);
  readDirect(fs, fd, alignTask);
  fs.close(fd);
}

static void test_direct_align_small_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", alignSmallTask.offset);
  writeDirect(fs, fd, alignSmallTask);
  readDirect(fs, fd, alignSmallTask);
  fs.close(fd);
}

static void test_multi_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  read_write_task task(4192, 12096);
  read_write_task task2(16288, 12288);
  int fd = createTestFile(fs, "file", 4192);
  writeFile(fs, fd, task);
  writeFile(fs, fd, task2);
  fs.close(fd);
}

static void test_direct_multi_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  read_write_task task(4192, 12096);
  read_write_task task2(16288, 12288);
  int fd = createTestFile(fs, "file", 4192);
  writeDirect(fs, fd, task);
  writeDirect(fs, fd, task2);
  fs.close(fd);
}

static void test_write_error(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", task.offset);
  WRITE_ERROR = true;
  writeFile(fs, fd, task);
  WRITE_ERROR = false;
  fs.close(fd);
}

static void test_fs_op_pool(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 2);
  fs_op_context* opCtx = nullptr;
  ByteBuffer* buffer = nullptr;
  for (int i = 0; i < DEFAULT_POOL_SIZE; i++) {
    opCtx = fs.allocFsOp();
    buffer = fs.allocBuffer();
    if (!opCtx || !buffer) {
      break;
    }
  }
  CU_ASSERT(opCtx != nullptr);
  CU_ASSERT(buffer != nullptr);

  fs_op_context* opCtx2 = fs.allocFsOp();
  ByteBuffer* buffer2 = fs.allocBuffer();
  CU_ASSERT(!opCtx2);
  CU_ASSERT(!buffer2);

  fs.freeFsOp(opCtx);
  fs.freeBuffer(buffer);
  opCtx2 = fs.allocFsOp();
  buffer2 = fs.allocBuffer();
  CU_ASSERT(opCtx2 != nullptr);
  CU_ASSERT(buffer2 != nullptr);
}

static void random_write_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  WriteContext* ctx = reinterpret_cast<WriteContext*>(opCtx->private_data);
  CU_ASSERT(code == 0);
  CU_ASSERT(ctx->file->inode_->size_ > ctx->offset + ctx->count);
  auto* extents = ctx->file->inode_->extents_;
  for (auto& extent : ctx->writeExtents) {
    CU_ASSERT(extent.index < extents->size());
  }
  ExtentMap* dirtyExtents = ctx->file->inode_->dirtyExtents;
  CU_ASSERT(dirtyExtents != nullptr && dirtyExtents->size() == 3);
  for (auto& [index, extentInfo] : *dirtyExtents) {
    CU_ASSERT(extentInfo.first != extentInfo.second);
  }
}

static void fsync_complete(void* cb_args, int code) {
  CU_ASSERT(code == 0);
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FSyncContext* ctx = reinterpret_cast<FSyncContext*>(opCtx->private_data);
  ExtentMap* dirtyExtents = ctx->file->inode_->dirtyExtents;
  CU_ASSERT(dirtyExtents != nullptr && dirtyExtents->size() == 0);
}

static void test_random_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fileSize = largeTask.offset + largeTask.count + BLOCK_SIZE;
  int fd = createTestFile(fs, "file", fileSize);

  fs_op_context* ctx = fs.allocFsOp();
  WriteContext* writeCtx = new (ctx->private_data) WriteContext();
  writeCtx->dirctWrite(&fs, fd, largeTask.offset, largeTask.count, largeTask.data);
  ctx->callback = random_write_complete;
  ctx->cb_args = ctx;
  RANDOM_WRITE = true;
  fs.write(*ctx);
  RANDOM_WRITE = false;
  fs.freeBuffer(writeCtx->direct_buff);

  FSyncContext* fsyncCtx = new (ctx->private_data) FSyncContext();
  fsyncCtx->fd = fd;
  ctx->callback = fsync_complete;
  fs.fsync(*ctx);
  fs.freeFsOp(ctx);
  fs.close(fd);
}

static void sparse_write_complete(void* cb_args, int code) {
  CU_ASSERT(code == 0);
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  WriteContext* ctx = reinterpret_cast<WriteContext*>(opCtx->private_data);
  CU_ASSERT(ctx->file->inode_->size_ == ctx->offset + ctx->count);
  auto* extents = ctx->file->inode_->extents_;
  CU_ASSERT(extents->size() == 2);
  CU_ASSERT((*extents)[0] == UINT32_MAX);
  ExtentMap* dirtyExtents = ctx->file->inode_->dirtyExtents;
  CU_ASSERT(dirtyExtents != nullptr && dirtyExtents->size() == 1);
  auto it = dirtyExtents->find(1/*indxe*/);
  CU_ASSERT(it != dirtyExtents->end());
  auto& extentInfo = it->second;
  CU_ASSERT(extentInfo.second == (*extents)[1]);
}

static void test_sparse_read_write(void) {
  FastFS fs("Malloc0");
  do_mount(fs, BLOCK_SIZE * 8);
  int fd = createTestFile(fs, "file", 0); // empty file
  fs.seek(fd, smallTask.offset, SEEK_SET);

  fs_op_context* ctx = fs.allocFsOp();
  WriteContext* writeCtx = new (ctx->private_data) WriteContext();
  writeCtx->fd = fd;
  writeCtx->direct = true;
  writeCtx->count = smallTask.count;
  writeCtx->direct_buff = fs.allocWriteBuffer(smallTask.count);
  writeCtx->direct_buff->putBytes(0, smallTask.data, smallTask.count);
  writeCtx->write_buff = writeCtx->direct_buff->p_buffer_;

  ctx->callback = sparse_write_complete;
  ctx->cb_args = ctx;
  RANDOM_WRITE = true;
  fs.write(*ctx);
  RANDOM_WRITE = false;
  fs.freeBuffer(writeCtx->direct_buff);
  fs.freeFsOp(ctx);
  fs.close(fd);
}

int main() {
  if (CUE_SUCCESS != CU_initialize_registry()) {
    return CU_get_error();
  }

  CU_pSuite suite = CU_add_suite("FastFS Tests", NULL, NULL);
  if (suite == NULL) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  if (
      CU_add_test(suite, "mkdir", test_mkdir) == NULL ||
      CU_add_test(suite, "delete", test_delete) == NULL ||
      CU_add_test(suite, "truncate", test_truncate) == NULL ||
      CU_add_test(suite, "objs poll", test_fs_op_pool) == NULL ||
      CU_add_test(suite, "open close", test_open_close) == NULL ||
      CU_add_test(suite, "alloc buffer", test_alloc_buffer) == NULL ||
      CU_add_test(suite, "fd overflow", test_fd_overflow) == NULL ||
      CU_add_test(suite, "inode overflow", test_inode_overflow) == NULL ||
      CU_add_test(suite, "file read write", test_read_write) == NULL ||
      CU_add_test(suite, "test write error", test_write_error) == NULL ||
      CU_add_test(suite, "small read write", test_read_write_small) == NULL ||
      CU_add_test(suite, "large read write", test_read_write_large) == NULL ||
      CU_add_test(suite, "align read write", test_align_read_write) == NULL ||
      CU_add_test(suite, "align small write", test_align_small_write) == NULL ||
      CU_add_test(suite, "with multi write", test_multi_write) == NULL ||
      CU_add_test(suite, "random read write", test_random_write) == NULL ||
      CU_add_test(suite, "sparse read write", test_sparse_read_write) == NULL ||
      CU_add_test(suite, "direct read write", test_direct_read_write) == NULL ||
      CU_add_test(suite, "direct multi write", test_direct_multi_write) == NULL ||
      CU_add_test(suite, "direct small read write", test_direct_read_write_small) == NULL ||
      CU_add_test(suite, "direct large read write", test_direct_read_write_large) == NULL ||
      CU_add_test(suite, "direct align read write", test_direct_align_read_write) == NULL ||
      CU_add_test(suite, "direct align small write", test_direct_align_small_write) == NULL
  ) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  CU_basic_set_mode(CU_BRM_VERBOSE);
  CU_basic_run_tests();

  CU_cleanup_registry();
  return CU_get_number_of_failures();
}
