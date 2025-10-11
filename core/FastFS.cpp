/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "FastFS.h"

fs_context_t FastFS::fs_context;

FastFS::FastFS(const char *bdev) { fs_context.bdev_name = bdev; }

void FSyncContext::serialize(ByteBuffer *buf) {
  buf->write<uint32_t>(file->inode_->ino_);
  buf->write<uint64_t>(file->inode_->size_);
  if (dirtyExtents) {
    buf->write<uint16_t>(dirtyExtents->size());
    for (auto &[index, extentInfo] : *dirtyExtents) {
      buf->write<uint32_t>(index);
      buf->write<uint32_t>(extentInfo.second);
    }
  } else {
    buf->write<uint16_t>(0);
  }
}

static void writeSuperBlockComplete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *buffer = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(buffer->private_data);
  auto &fs_context = FastFS::fs_context;
  spdk_dma_free(buffer->p_buffer_);
  delete buffer;
  SPDK_NOTICELOG("format FastFS successfully, current epoch %d, extentSize %d\n",
                 fs_context.superBlock.epoch,
                 fs_context.superBlock.extentSize);
  fs_context.callback(fastfs, success ? 0 : -1);
}

static void writeSuperBlock(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }

  ByteBuffer *buffer = reinterpret_cast<ByteBuffer *>(cb_arg);
  fs_context_t &ctx = FastFS::fs_context;
  if (success && ctx.superBlock.deserialize(buffer)) {
    ctx.superBlock.epoch += 1;
  } else {
    ctx.superBlock.epoch = 0;
  }
  ctx.superBlock.ckptInodesLoc = 0;
  ctx.superBlock.ckptDentryLoc = 0;
  ctx.superBlock.lastTxid = 0;
  ctx.superBlock.journalLoc = 1; // use second extent by default
  ctx.superBlock.journalSkipBlocks = 0;
  ctx.superBlock.journalSkipOps = 0;
  ctx.superBlock.extentSize = ctx.extentSize;
  ctx.superBlock.flags = 0;
  ctx.superBlock.version = 0;
  buffer->clear();
  ctx.superBlock.serialize(buffer);
  spdk_bdev_write(
    ctx.bdev_desc, ctx.bdev_io_channel, buffer->p_buffer_, 0, ctx.blockSize, writeSuperBlockComplete, buffer);
}

void FastFS::format(uint32_t extentSize, fs_cb callback, bool skipJournal) {
  if (extentSize & (extentSize - 1)) {
    SPDK_WARNLOG("extentSize needs to be a power of 2.\n");
    callback(this, -1);
    return;
  }
  fs_context.callback = callback;
  // read super block to determine fs epoch
  uint32_t ioUnitSize = spdk_bdev_get_write_unit_size(fs_context.bdev);
  fs_context.bufAlign = spdk_bdev_get_buf_align(fs_context.bdev);
  fs_context.blockSize = spdk_bdev_get_block_size(fs_context.bdev) * ioUnitSize;
  fs_context.extentSize = extentSize;
  fs_context.skipJournal = skipJournal;
  fs_context.localCore = spdk_env_get_current_core();
  fs_context.localNuma = spdk_env_get_numa_id(fs_context.localCore);

  char *addr = (char *)spdk_dma_zmalloc_socket(fs_context.blockSize, fs_context.bufAlign, NULL, fs_context.localNuma);
  ByteBuffer *buffer = new ByteBuffer(addr, fs_context.blockSize);
  buffer->private_data = this;
  spdk_bdev_read(fs_context.bdev_desc,
                 fs_context.bdev_io_channel,
                 buffer->p_buffer_,
                 0,
                 fs_context.blockSize,
                 writeSuperBlock,
                 buffer);
}

static void loadCheckpoint(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *buffer = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(buffer->private_data);
  fs_context_t &ctx = FastFS::fs_context;
  success = success && ctx.superBlock.deserialize(buffer);
  spdk_dma_free(buffer->p_buffer_);
  delete buffer;
  if (success) {
    ctx.extentSize = ctx.superBlock.extentSize;
    ctx.extentBits = spdk_u32log2(ctx.extentSize);
    ctx.extentMask = ctx.extentSize - 1;
    ctx.allocator = new BlockAllocator(ctx.blocks, ctx.blockSize, ctx.extentSize);
    fastfs->initObjPool(DEFAULT_POOL_SIZE);
    fastfs->checkpoint = new FastCkpt(ctx);
    fastfs->checkpoint->loadImage();
  } else {
    SPDK_WARNLOG("parse superblock failed, should format first.\n");
    ctx.callback(fastfs, -2);
  }
}

void FastFS::mount(fs_cb callback, uint32_t maxInodes, uint32_t maxFiles) {
  if (maxInodes & (maxInodes - 1)) {
    SPDK_WARNLOG("maxInodes needs to be a power of 2.\n");
    callback(this, -1);
    return;
  }
  fs_context.fastfs = this;
  fs_context.callback = callback;
  fs_context.maxInodes = maxInodes;
  fs_context.inodesMask = maxInodes * 4 - 1;
  fs_context.maxFiles = maxFiles;

  uint32_t ioUnitSize = spdk_bdev_get_write_unit_size(fs_context.bdev);
  fs_context.bufAlign = spdk_bdev_get_buf_align(fs_context.bdev);
  fs_context.blocks = spdk_bdev_get_num_blocks(fs_context.bdev) / ioUnitSize;
  fs_context.blockSize = spdk_bdev_get_block_size(fs_context.bdev) * ioUnitSize;
  fs_context.blockBits = spdk_u32log2(fs_context.blockSize);
  fs_context.blockMask = (1 << fs_context.blockBits) - 1;
  fs_context.localCore = spdk_env_get_current_core();
  fs_context.localNuma = spdk_env_get_numa_id(fs_context.localCore);

  fs_context.fdAllocator = new BitsAllocator(fs_context.maxFiles);
  fs_context.fdAllocator->reserve(0); // stdin
  fs_context.fdAllocator->reserve(1); // stdout
  fs_context.fdAllocator->reserve(2); // stderr
  files = new FileCache(fs_context.maxFiles, MemAllocator<FastFile>(fs_context.localNuma, 64));
  fs_context.inodeAllocator = new BitsAllocator(fs_context.maxInodes);
  fs_context.inodeAllocator->reserve(0); // root
  slots = new HashSlots(fs_context.inodesMask + 1, MemAllocator<uint32_t>(fs_context.localNuma, 64));
  inodes = new INodeCache(fs_context.maxInodes, MemAllocator<FastInode>(fs_context.localNuma, 64));
  root = &(*inodes)[0];
  root->create(0, 0, "root", FASTFS_DIR);

  // read super block
  char *addr = (char *)spdk_dma_zmalloc_socket(fs_context.blockSize, fs_context.bufAlign, NULL, fs_context.localNuma);
  ByteBuffer *buffer = new ByteBuffer(addr, fs_context.blockSize);
  buffer->private_data = this;
  spdk_bdev_read(fs_context.bdev_desc,
                 fs_context.bdev_io_channel,
                 buffer->p_buffer_,
                 0,
                 fs_context.blockSize,
                 loadCheckpoint,
                 buffer);
}

void FastFS::dumpInfo() {
  journal->dumpInfo();
  SPDK_NOTICELOG("inodes %d, free extents %d, first slot %d\n",
                 fs_context.inodeAllocator->getAllocated(),
                 fs_context.allocator->getFree(),
                 fs_context.allocator->getLowestFreeIndex());
}

void FastFS::unmount() {
  dumpInfo();

  for (auto &file : *files) {
    file.close();
  }
  for (auto &inode : *inodes) {
    inode.unlink();
  }

  if (slots) {
    delete slots;
  }
  if (inodes) {
    delete inodes;
  }
  if (files) {
    delete files;
  }
  if (fs_ops) {
    delete fs_ops;
  }
  if (buffers) {
    delete buffers;
  }

  if (fs_context.allocator) {
    delete fs_context.allocator;
  }
  if (fs_context.fdAllocator) {
    delete fs_context.fdAllocator;
  }

  if (journal) {
    delete journal;
  }
  if (fs_context.bdev_io_channel) {
    spdk_put_io_channel(fs_context.bdev_io_channel);
  }
  if (fs_context.bdev_desc) {
    spdk_bdev_close(fs_context.bdev_desc);
  }
}

void FastFS::initObjPool(int poolSize) {
  fs_ops =
    new std::vector<fs_op_context, MemAllocator<fs_op_context>>(MemAllocator<fs_op_context>(fs_context.localNuma, 64));
  buffers = new std::vector<ByteBuffer, MemAllocator<ByteBuffer>>(MemAllocator<ByteBuffer>(fs_context.localNuma, 64));
  fs_ops->reserve(poolSize);
  buffers->reserve(poolSize);
  for (int i = 0; i < poolSize; i++) {
    char *buffer =
      (char *)spdk_dma_zmalloc_socket(fs_context.extentSize, fs_context.bufAlign, NULL, fs_context.localNuma);
    buffers->emplace_back(buffer, fs_context.extentSize);
  }
  for (int i = 0; i < poolSize - 1; i++) {
    (*fs_ops)[i].fastfs = this;
    (*fs_ops)[i].next = &(*fs_ops)[i + 1];
    (*buffers)[i].next = &(*buffers)[i + 1];
  }
  (*fs_ops)[poolSize - 1].fastfs = this;
  (*fs_ops)[poolSize - 1].next = nullptr;
  (*buffers)[poolSize - 1].next = nullptr;
  op_head = &(*fs_ops)[0];
  buf_head = &(*buffers)[0];
}

fs_op_context *FastFS::allocFsOp() {
  fs_op_context *res = op_head;
  if (op_head) {
    op_head = op_head->next;
  }
  return res;
}

void FastFS::freeFsOp(fs_op_context *fs_op) {
  fs_op->next = op_head;
  op_head = fs_op;
}

// TODO chenxu14 consider NULL
ByteBuffer *FastFS::allocBuffer() {
  ByteBuffer *res = buf_head;
  if (buf_head) {
    buf_head = buf_head->next;
  }
  return res;
}

ByteBuffer *FastFS::allocReadBuffer(uint64_t offset, uint32_t len) {
  uint64_t extentOffset = offset & fs_context.extentMask;
  uint64_t blockOffset = extentOffset & fs_context.blockMask;
  uint64_t startOffset = extentOffset - blockOffset;
  uint64_t endOffset = (extentOffset + len + fs_context.blockMask) & ~(fs_context.blockMask);
  uint32_t capacity = endOffset - startOffset;
  ByteBuffer *buff = nullptr;
  if (capacity <= fs_context.extentSize) {
    buff = allocBuffer();
  } else {
    buff = new ByteBuffer(capacity, true, fs_context.localNuma, fs_context.bufAlign);
  }
  buff->position(blockOffset);
  return buff;
}

ByteBuffer *FastFS::allocWriteBuffer(uint32_t len) {
  uint32_t capacity = ((len + fs_context.blockMask) & ~(fs_context.blockMask));
  if (capacity <= fs_context.extentSize) {
    return allocBuffer();
  } else {
    return new ByteBuffer(capacity, true, fs_context.localNuma, fs_context.bufAlign);
  }
}

void FastFS::freeBuffer(ByteBuffer *buffer) {
  if (buffer->alloc_) {
    delete buffer;
    return;
  }
  buffer->clear();
  buffer->private_data = nullptr;
  buffer->next = buf_head;
  buf_head = buffer;
}
