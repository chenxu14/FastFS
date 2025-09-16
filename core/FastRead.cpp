/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "FastFS.h"

void ReadContext::reset(FastFile* f) {
  file = f;
  extentsToRead = 0;
  extentsReaded = 0;
  readingSize = 0;
  success = true;
}

void ReadContext::dirctRead(
    FastFS* fs, int handle, uint64_t off, uint32_t len) {
  fd = handle;
  pread = true;
  direct = true;
  offset = off;
  count = len;
  direct_buff = fs->allocReadBuffer(offset, count);
  direct_cursor = direct_buff->position();
}

static void readExtentComplete(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }

  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  fs_op_context* ctx = reinterpret_cast<fs_op_context*>(buffer->private_data);
  ReadContext* readCtx = reinterpret_cast<ReadContext*>(ctx->private_data);
  readCtx->extentsReaded++;
  readCtx->success = (readCtx->success & success);
  if (!readCtx->direct) {
    if (success) {
      // use mark_ to decision target cursor
      char* buf = readCtx->read_buff + buffer->mark_;
      memcpy(buf, buffer->getBuffer(), buffer->remaining());
    }
    // [NOTICE] can't do this before memcpy
    ctx->fastfs->freeBuffer(buffer);
  }

  // all extent commit and complete
  if ((readCtx->readingSize == readCtx->count) &&
      (readCtx->extentsReaded == readCtx->extentsToRead)) {
    if (!readCtx->pread) {
      readCtx->file->pos_ += readCtx->count;
    }
    int code = readCtx->success ? 0 : -4/*ReadDataFail*/;
    ctx->callback(ctx->cb_args, code);
  }
}

void FastFS::read(fs_op_context& ctx) {
  ReadContext* readCtx = reinterpret_cast<ReadContext*>(ctx.private_data);
  if (readCtx->fd > fs_context.maxFiles) {
    return ctx.callback(ctx.cb_args, -1);
  }
  FastFile& file = (*files)[readCtx->fd];
  uint64_t offset = readCtx->pread ? readCtx->offset : file.pos_;
  if (offset > file.inode_->size_) {
    return ctx.callback(ctx.cb_args, -2/*EOF*/);
  }

  readCtx->reset(&file);
  uint32_t remaining = file.inode_->size_ - offset;
  readCtx->count = std::min(readCtx->count, remaining);
  if (readCtx->count == 0) {
    return ctx.callback(ctx.cb_args, 0);
  }

  uint64_t bdevOffset;
  uint32_t nbytes;

  // read first extent
  uint32_t index;
  uint32_t extentId;
  file.inode_->getExtent(offset, index, extentId);
  uint64_t extentOffset = offset & fs_context.extentMask;
  if (extentOffset != 0) {
    // plus first extent's data
    readCtx->readingSize = (fs_context.extentSize - extentOffset);
    uint64_t blockOffset = extentOffset & fs_context.blockMask;
    // align extentOffset with blockSize
    extentOffset -= blockOffset;

    readCtx->extentsToRead++;
    ByteBuffer* extentBuf = nullptr;
    if (readCtx->direct) {
      extentBuf = readCtx->direct_buff;
    } else {
      extentBuf = allocBuffer();
      extentBuf->position(blockOffset);
    }
    extentBuf->private_data = &ctx;
    if (readCtx->readingSize > readCtx->count) {
      // most cases, one extent is enough
      readCtx->readingSize = readCtx->count;
      extentBuf->limit(blockOffset + readCtx->count);
      nbytes = (extentBuf->limit() + fs_context.blockMask)
          & ~(fs_context.blockMask);
    } else {
      nbytes = fs_context.extentSize - extentOffset;
      extentBuf->limit(nbytes);
    }

    if (extentId == UINT32_MAX) { // sparse file
      memset(extentBuf->getBuffer(), 0, extentBuf->remaining());
      readExtentComplete(nullptr, true, extentBuf);
    } else {
      // read extent data
      bdevOffset = (static_cast<uint64_t>(extentId) << fs_context.extentBits)
          + extentOffset;
      spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
          extentBuf->p_buffer_, bdevOffset, nbytes, readExtentComplete, extentBuf);
    }
    index++;
  }

  // read other extents
  while (readCtx->readingSize < readCtx->count) {
    readCtx->extentsToRead++;
    ByteBuffer* extentBuf = nullptr;
    if (readCtx->direct) {
      extentBuf = readCtx->direct_buff;
      if (readCtx->extentsToRead > 1) {
        extentBuf->position(extentBuf->limit_);
      } else {
        extentBuf->private_data = &ctx;
      }
    } else {
      extentBuf = allocBuffer();
      extentBuf->private_data = &ctx;
    }
    extentBuf->mark_ = readCtx->readingSize; // used for memcpy and UT

    nbytes = fs_context.extentSize;
    uint32_t remaining = readCtx->remainingSize();
    if (remaining < fs_context.extentSize) {
      // the last extent
      extentBuf->limit(extentBuf->position_ + remaining);
      // align block size
      nbytes = (remaining + fs_context.blockMask) & ~(fs_context.blockMask);
      readCtx->readingSize = readCtx->count;
    } else {
      extentBuf->limit(extentBuf->position_ + fs_context.extentSize);
      readCtx->readingSize += fs_context.extentSize;
    }

    extentId = (*file.inode_->extents_)[index];
    if (extentId == UINT32_MAX) { // sparse file
      memset(extentBuf->getBuffer(), 0, extentBuf->remaining());
      readExtentComplete(nullptr, true, extentBuf);
    } else {
      bdevOffset = (static_cast<uint64_t>(extentId) << fs_context.extentBits);
      spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
          extentBuf->getBuffer(), bdevOffset, nbytes, readExtentComplete, extentBuf);
    }
    index++;
  }
}
