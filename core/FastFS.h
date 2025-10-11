/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTFS_H_
#define FASTFS_H_

#include "FastJournal.h"
#include "Serialization.h"

#define F_READ 1
#define F_WRITE 2
#define F_MULTI_WRITE 32

// forward declare
class FastFS;
class FastInode;
class FastFile;
class FastCkpt;

using FileCache = std::vector<FastFile, MemAllocator<FastFile>>;
using HashSlots = std::vector<uint32_t, MemAllocator<uint32_t>>;
using INodeCache = std::vector<FastInode, MemAllocator<FastInode>>;
// index -> (old, new)
using ExtentMap = std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>;

struct fs_context_t {
  const char *bdev_name;
  struct spdk_bdev *bdev = nullptr;
  struct spdk_bdev_desc *bdev_desc = nullptr;
  struct spdk_io_channel *bdev_io_channel = nullptr;
  BlockAllocator *allocator = nullptr;
  BitsAllocator *fdAllocator = nullptr;
  BitsAllocator *inodeAllocator = nullptr;
  FastFS *fastfs;
  uint64_t blocks;
  uint32_t blockSize;
  uint32_t blockBits;
  uint32_t blockMask;
  uint32_t extentSize;
  uint32_t extentBits;
  uint32_t extentMask;
  uint32_t bufAlign;
  uint32_t maxInodes;
  uint32_t inodesMask;
  uint32_t maxFiles;
  uint32_t localCore;
  uint32_t localNuma;
  bool skipJournal;
  SuperBlock superBlock;
  fs_cb callback;
};

struct fs_op_context {
  fs_op_context *next;
  op_cb callback;
  void *cb_args;
  FastFS *fastfs;
  char private_data[96]; // align 128 bytes
};

class WriteExtent {
  public:
  uint64_t offset; // bdev offset
  uint32_t len;
  uint32_t bufOff;
  uint32_t bufLen;
  uint32_t index;
  uint32_t extentId;
  uint32_t extentOff;
  uint32_t newId;
  fs_op_context *op_ctx;
};

class WriteContext {
  public:
  uint32_t fd;
  uint32_t count;
  uint64_t offset;
  bool pwrite;
  bool append;
  bool direct;
  bool success;
  FastFile *file;
  ByteBuffer *direct_buff;
  const char *write_buff;
  // internal use
  uint32_t writingSize;
  uint32_t writedExtents;
  // clear carefully to avoid memory leak
  std::list<WriteExtent> writeExtents;

  public:
  WriteContext() : pwrite(false), append(false), direct(false), success(true) {}
  void reset(FastFile *f, uint64_t off);
  void dirctWrite(FastFS *fs, int handle, uint64_t off, uint32_t len, const char *data);
  uint32_t remainingSize() { return count - writingSize; }
};

class FSyncContext {
  public:
  uint32_t fd;
  FastFile *file;
  ExtentMap *dirtyExtents;

  public:
  void serialize(ByteBuffer *buf);
};

class ReadContext {
  public:
  uint32_t fd;
  uint32_t count;
  uint64_t offset;
  bool pread;
  bool direct;
  bool success;
  FastFile *file;
  char *read_buff;
  ByteBuffer *direct_buff;
  uint32_t direct_cursor;
  // internal use
  uint32_t readingSize;
  uint32_t extentsToRead;
  uint32_t extentsReaded;

  public:
  void reset(FastFile *f);
  void dirctRead(FastFS *fs, int handle, uint64_t off, uint32_t len);
  uint32_t remainingSize() { return count - readingSize; }
};

class FastFile {
  public:
  uint32_t flags_;
  uint64_t pos_;
  FastInode *inode_;
  ByteBuffer *tail_block;

  public:
  FastFile() : flags_(0), pos_(0), inode_(nullptr), tail_block(nullptr) {}

  void open(uint32_t flags, FastInode *inode);
  void close();
  ByteBuffer *getTailBlock();

  void clearTailBlock() {
    if (tail_block) {
      tail_block->clear();
    }
  }
};

class FastFS {
  public:
  static fs_context_t fs_context;
  bool ready = false;
  HashSlots *slots = nullptr;
  INodeCache *inodes = nullptr;
  FileCache *files = nullptr;
  std::vector<fs_op_context, MemAllocator<fs_op_context>> *fs_ops = nullptr;
  std::vector<ByteBuffer, MemAllocator<ByteBuffer>> *buffers = nullptr;
  fs_op_context *op_head = nullptr;
  ByteBuffer *buf_head = nullptr;
  FastJournal *journal = nullptr;
  FastCkpt *checkpoint = nullptr;
  FastInode *root = nullptr;

  public:
  FastFS(const char *bdev);

  void format(uint32_t extentSize, fs_cb callback, bool skipJournal = false);
  void mount(fs_cb callback, uint32_t maxInodes = 2097152, uint32_t maxFiles = 65536);
  void unmount();

  FastInode *lookup(uint32_t parentId, std::string_view name) const;
  /**
   * lookup for write
   * pre, target inode's previous PTR, when hash conflict happen
   * ino, target inode's ino
   * return true when pre is HEAD
   */
  bool lookup(uint32_t parentId, std::string_view name, uint32_t &pre, uint32_t &ino) const;
  FastInode *status(const std::string &path) const;

  int open(uint32_t ino, uint32_t flags);
  int open(const std::string &path, uint32_t flags);
  int close(uint32_t fd);

  int applyCreate(CreateContext *createCtx);
  void create(fs_op_context &ctx);
  // mkdir -p
  void createRecursive(const std::string &path, op_cb callback, void *args);

  int applyTruncate(TruncateContext *truncateCtx);
  void truncate(fs_op_context &ctx);

  int applyRemove(DeleteContext *delCtx);
  void remove(fs_op_context &ctx);

  int applyRename(RenameContext *renameCtx);
  void rename(fs_op_context &ctx);

  void writeComplete(fs_op_context *ctx);
  void write(fs_op_context &ctx);
  void fsync(fs_op_context &ctx);

  int64_t seek(uint32_t fd, uint64_t offset, int whence);
  void read(fs_op_context &ctx);

  void initObjPool(int poolSize);

  fs_op_context *allocFsOp();
  void freeFsOp(fs_op_context *fs_op);

  ByteBuffer *allocBuffer();
  ByteBuffer *allocReadBuffer(uint64_t offset, uint32_t len);
  ByteBuffer *allocWriteBuffer(uint32_t len);
  void freeBuffer(ByteBuffer *buffer);

  void dumpInfo();

  private:
  void removeRecursive(FastInode *target);
  inline uint32_t hashSlot(uint32_t parentId, std::string_view name) const;
};

class FastInode {
  public:
  uint32_t parentId_;
  uint32_t next_;
  uint32_t ino_;
  uint32_t refCnts_;
  uint32_t mode_;
  uint8_t status_; // release(0), use(1), delete(2)
  FileType type_;
  uint64_t size_;
  union {
    std::vector<uint32_t> *extents_;
    std::set<uint32_t> *children_;
  };
  ExtentMap *dirtyExtents;
  std::string name_;

  public:
  FastInode() : next_(0), refCnts_(0), status_(0), dirtyExtents(nullptr) {}

  void create(uint32_t ino, uint32_t parentId, std::string_view name, FileType type);

  void unlink();

  bool getExtent(uint64_t offset, uint32_t &index, uint32_t &extentId);
};

class FastCkpt {
  public:
  const fs_context_t &fs_context;
  INodeCache::const_iterator cusor;
  std::set<uint32_t>::const_iterator dentryCusor;
  std::vector<uint32_t>::const_iterator extentCusor;
  bool extentEnd = true;
  bool dentryEnd = true;
  uint32_t inodesLocation;
  uint32_t dentryLocation;
  uint32_t inodeExtents;
  uint32_t dentryExtents;
  uint32_t curExtent;
  uint32_t nextExtent;
  std::forward_list<uint32_t> extents_;
  INodeFile inodeProto;
  fs_cb ckpt_cb = nullptr;

  public:
  FastCkpt(const fs_context_t &context) : fs_context(context) {}
  ~FastCkpt() {}
  void loadImage();
  void checkpoint(fs_cb callback);
  bool parseExtent(ByteBuffer *extentBuf, uint32_t &nextExtent, INodeCache &inodes);
  inline int releaseExtents() {
    int count = 0;
    for (auto &extentId : extents_) {
      count++;
      fs_context.allocator->release(extentId);
    }
    return count;
  }
};

#endif /* FASTFS_H_ */
