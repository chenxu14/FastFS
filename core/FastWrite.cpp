/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "FastFS.h"

void WriteContext::reset(FastFile* f, uint64_t off) {
  offset = off;
  writingSize = 0;
  writedExtents = 0;
  file = f;
  writeExtents.clear();
  append = (offset == file->inode_->size_);
  if (!append || file->getTailBlock()->mark_ != offset) {
    file->getTailBlock()->clear();
  }
  if (!file->inode_->dirtyExtents) {
    file->inode_->dirtyExtents = new ExtentMap();
  }
}

void WriteContext::dirctWrite(
    FastFS* fs, int handle, uint64_t off, uint32_t len, const char* data) {
  fd = handle;
  pwrite = true;
  direct = true;
  offset = off;
  count = len;
  direct_buff = fs->allocWriteBuffer(count);
  direct_buff->putBytes(0, data, count);
  write_buff = direct_buff->p_buffer_;
}

ByteBuffer* FastFile::getTailBlock() {
  if (!tail_block) {
    fs_context_t& fs_context = FastFS::fs_context;
    char* tail_block_buffer = (char*) spdk_dma_zmalloc_socket(
        fs_context.blockSize, fs_context.bufAlign, NULL, fs_context.localNuma);
    tail_block = new ByteBuffer(tail_block_buffer, fs_context.blockSize);
  }
  return tail_block;
}

void FastFS::writeComplete(fs_op_context* ctx) {
  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  FastFile* file = writeCtx->file;
  FastInode* inode = file->inode_;
  // if inode has been deleted, no need to update
  if (inode->status_ == 1) {
    uint64_t size = writeCtx->offset + writeCtx->writingSize;
    if (size > inode->size_) {
      inode->size_ = size;
    }
    // update file position
    if (!writeCtx->pwrite) {
      file->pos_ += writeCtx->count;
    }
    // reset tail block
    if (writeCtx->append && (file->flags_ & F_MULTI_WRITE)) {
      uint64_t endoff = writeCtx->offset + writeCtx->count;
      uint32_t tailSize = endoff & fs_context.blockMask;
      if (tailSize > 0) {
        ByteBuffer* tailBlock = file->getTailBlock();
        tailBlock->mark_ = inode->size_;
        if (tailSize < writeCtx->count) {
          // write span blocks
          const char* buf = writeCtx->write_buff + (writeCtx->count - tailSize);
          tailBlock->clear().putBytes(buf, tailSize);
        } else if ((writeCtx->offset & fs_context.blockMask) == 0) {
          // align small write case
          tailBlock->clear().putBytes(writeCtx->write_buff, writeCtx->count);
        }
      }
    }
    // modify extents info
    ExtentMap* dirtyExtents = file->inode_->dirtyExtents;
    for (auto& extent : writeCtx->writeExtents) {
      if (extent.newId != 0) { // newAdd or copyOnWrite
        if (extent.index >= inode->extents_->size()) {
          // append case or sparse file
          for (uint32_t i = inode->extents_->size(); i < extent.index; i++) {
            inode->extents_->emplace_back(UINT32_MAX); // sparse extents
          }
          inode->extents_->emplace_back(extent.newId);
        } else {
          // random write case
          (*inode->extents_)[extent.index] = extent.newId;
        }
        auto iter = dirtyExtents->find(extent.index);
        if (iter == dirtyExtents->end()) { // first write after fsync
          uint32_t original = UINT32_MAX;
          if (extent.newId != extent.extentId) {
            original = extent.extentId;
          }
          dirtyExtents->emplace(extent.index,
              std::pair<uint32_t, uint32_t>(original, extent.newId));
        } else { // extent written multi times after fsync
          auto& extentInfo = iter->second;
          fs_context.allocator->release(extentInfo.second);
          extentInfo.second = extent.newId;
        }
      }
    }
    // judge whether fsync needs to be called
    if (file->flags_ & O_SYNC) {
      writeCtx->writeExtents.clear();
      int fd = writeCtx->fd;
      FSyncContext* fsyncCtx = new (ctx->private_data) FSyncContext();
      fsyncCtx->fd = fd;
      return fsync(*ctx);
    }
  } else {
    // free allocated extents
    for (WriteExtent& extentInfo : writeCtx->writeExtents) {
      if (extentInfo.newId) {
        fs_context.allocator->release(extentInfo.newId);
      }
    }
  }
  writeCtx->writeExtents.clear();
  ctx->callback(ctx->cb_args, 0);
}

static void writeExtentComplete(
    struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }

  fs_op_context* ctx;
  WriteContext* writeCtx;
  if (!success && !bdev_io) { // read tail block or extent fail
    ctx = reinterpret_cast<fs_op_context*>(cb_arg);
    writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  } else {
    ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
    ctx = reinterpret_cast<fs_op_context*>(buffer->private_data);
    writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
    if (!writeCtx->direct ||
        buffer->p_buffer_ != writeCtx->direct_buff->p_buffer_) {
      ctx->fastfs->freeBuffer(buffer);
    }
  }

  writeCtx->success = (writeCtx->success & success);
  writeCtx->writedExtents++;
  // all WriteExtent commit and complete
  if ((writeCtx->writingSize == writeCtx->count) &&
      (writeCtx->writedExtents == writeCtx->writeExtents.size())) {
    if (writeCtx->success) {
      ctx->fastfs->writeComplete(ctx);
    } else {
      // remove tail block since it maybe dirty
      writeCtx->file->clearTailBlock();
      // free new allocated extents
      for (auto& extent : writeCtx->writeExtents) {
        if (extent.newId) {
          FastFS::fs_context.allocator->release(extent.newId);
        }
      }
      writeCtx->writeExtents.clear();
      ctx->callback(ctx->cb_args, -5/*WriteDataFail*/);
    }
  }
}

static void copyOnWrite(struct spdk_bdev_io* bdev_io, bool success, void* cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer* extentBuf = reinterpret_cast<ByteBuffer*>(cb_arg);
  WriteExtent* extent = reinterpret_cast<WriteExtent*>(extentBuf->private_data);
  fs_op_context* ctx = extent->op_ctx;
  if (!success) { // read extent fail
    ctx->fastfs->freeBuffer(extentBuf);
    return writeExtentComplete(nullptr, false, ctx);
  }

  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  char* data = writeCtx->direct_buff->p_buffer_ + extent->bufOff;
  extentBuf->putBytes(extent->extentOff, data, extent->bufLen);
  extentBuf->private_data = ctx;
  // allocate new extent for write
  extent->newId = FastFS::fs_context.allocator->allocate();
  if (extent->newId == UINT32_MAX) {
    SPDK_WARNLOG("no free extents.\n");
    ctx->fastfs->freeBuffer(extentBuf);
    return writeExtentComplete(nullptr, false, ctx);
  }
  uint64_t bdevOff = (extent->newId << FastFS::fs_context.extentBits);
  spdk_bdev_write(FastFS::fs_context.bdev_desc, FastFS::fs_context.bdev_io_channel,
      extentBuf->p_buffer_, bdevOff, FastFS::fs_context.extentSize,
      writeExtentComplete, extentBuf);
}

static void writeExtent(struct spdk_bdev_io* bdev_io, bool success, void* cb_arg) {
  if (bdev_io) { // bdev_io used for read tail block
    spdk_bdev_free_io(bdev_io);
  }
  WriteExtent* extent = reinterpret_cast<WriteExtent*>(cb_arg);
  fs_op_context* ctx = extent->op_ctx;
  if (!success) { // read tail block fail
    return writeExtentComplete(nullptr, false, ctx);
  }

  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  if (writeCtx->direct) {
    // skip non-required data
    extent->bufOff += writeCtx->direct_buff->position_;
  }
  auto& fs_context = FastFS::fs_context;

  if (writeCtx->append) {
    struct iovec iov[2];
    int iovcnt = 0;
    // write tail block
    if (!extent->newId && (writeCtx->offset & fs_context.blockMask)) {
      ByteBuffer* tailBlock = writeCtx->file->tail_block;
      uint32_t tailSize = std::min(tailBlock->remaining(), writeCtx->count);
      tailBlock->putBytes(writeCtx->write_buff, tailSize);
      extent->bufOff += tailSize;
      extent->bufLen -= tailSize;
      iov[iovcnt].iov_base = tailBlock->p_buffer_;
      iov[iovcnt].iov_len = fs_context.blockSize;
      iovcnt++;
    }
    // write remaining
    ByteBuffer* buffer = writeCtx->direct ? writeCtx->direct_buff : ctx->fastfs->allocBuffer();
    buffer->private_data = ctx;
    if (extent->bufLen > 0) {
      if (writeCtx->direct) {
        iov[iovcnt].iov_base = buffer->p_buffer_ + extent->bufOff;
      } else {
        const char* targetBuf = writeCtx->write_buff + extent->bufOff;
        buffer->putBytes(targetBuf, extent->bufLen);
        iov[iovcnt].iov_base = buffer->p_buffer_;
      }
      iov[iovcnt].iov_len =
          ((extent->bufLen + fs_context.blockMask) & ~(fs_context.blockMask));
      iovcnt++;
      spdk_bdev_writev(fs_context.bdev_desc, fs_context.bdev_io_channel,
          iov, iovcnt, extent->offset, extent->len, writeExtentComplete, buffer);
    } else {
      // small write case, write size less than one block
      spdk_bdev_write(fs_context.bdev_desc, fs_context.bdev_io_channel,
          iov[0].iov_base, extent->offset, extent->len, writeExtentComplete, buffer);
    }
  } else if (extent->newId) {
    // sparse file
    ByteBuffer* buffer = ctx->fastfs->allocBuffer();
    memset(buffer->p_buffer_, 0, fs_context.extentSize);
    // TODO chenxu14 consider no direct case
    char* data = writeCtx->direct_buff->p_buffer_ + extent->bufOff;
    buffer->putBytes(extent->extentOff, data, extent->bufLen);
    buffer->private_data = ctx;
    spdk_bdev_write(fs_context.bdev_desc, fs_context.bdev_io_channel,
        buffer->p_buffer_, extent->offset, extent->len, writeExtentComplete, buffer);
  } else { // random write case
    if (extent->bufLen == fs_context.extentSize) {
      // whole extent write, no need to copy first
      ByteBuffer* buffer = writeCtx->direct_buff;
      // TODO chenxu14 consider no direct case
      char* addr = buffer->p_buffer_ + extent->bufOff;
      buffer->private_data = ctx;
      // allocate new extent for write
      extent->newId = fs_context.allocator->allocate();
      if (extent->newId == UINT32_MAX) {
        SPDK_WARNLOG("no free extents.\n");
        return writeExtentComplete(nullptr, false, ctx);
      }
      uint64_t bdevOff = (extent->newId << fs_context.extentBits);
      spdk_bdev_write(fs_context.bdev_desc, fs_context.bdev_io_channel,
          addr, bdevOff, fs_context.extentSize, writeExtentComplete, buffer);
    } else {
      // should do copy on write
      ByteBuffer* buffer = ctx->fastfs->allocBuffer();
      buffer->private_data = extent;
      spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
          buffer->p_buffer_, extent->offset, extent->len, copyOnWrite, buffer);
    }
  }
}

static void writeRange(struct spdk_bdev_io* bdev_io, bool success, void* cb_arg) {
  fs_op_context* ctx = nullptr;
  if (bdev_io) { // used by paddingZero
    spdk_bdev_free_io(bdev_io);
    ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
    ctx = reinterpret_cast<fs_op_context*>(buffer->private_data);
    ctx->fastfs->freeBuffer(buffer);
    if (!success) {
      return ctx->callback(ctx->cb_args, -2/*PaddingZeroFailed*/);
    }
  } else {
    ctx = reinterpret_cast<fs_op_context*>(cb_arg);
  }

  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  auto& fs_context = FastFS::fs_context;
  auto& file = *(writeCtx->file);
  uint64_t offset = writeCtx->offset;
  WriteExtent extent;
  uint64_t extentOffset = offset & fs_context.extentMask;
  if (extentOffset != 0) { // first extent not align
    extent.newId = 0;
    if (!file.inode_->getExtent(offset, extent.index, extent.extentId)) {
      extent.extentId = fs_context.allocator->allocate();
      if (extent.extentId == UINT32_MAX) {
        SPDK_WARNLOG("no free extents.\n");
        return ctx->callback(ctx->cb_args, -3/*NoFreeExtents*/);
      }
      extent.newId = extent.extentId;
    }
    writeCtx->writingSize = (fs_context.extentSize - extentOffset);
    uint64_t blockOffset = extentOffset & fs_context.blockMask;
    // compute bdev's offset & length
    extent.bufOff = 0;
    extent.extentOff = extentOffset;
    WriteExtent* target = nullptr;
    if (writeCtx->append) {
      // align extentOffset with blockSize
      extentOffset -= blockOffset;
      extent.offset = (extent.extentId << fs_context.extentBits) + extentOffset;
      // most cases, one extent is enough
      if (writeCtx->writingSize >= writeCtx->count) {
        writeCtx->writingSize = writeCtx->count;
        extent.len = (writeCtx->count + blockOffset + fs_context.blockMask)
            & ~(fs_context.blockMask);
      } else {
        extent.len = fs_context.extentSize - extentOffset;
      }
      extent.bufLen = writeCtx->writingSize;
      target = &writeCtx->writeExtents.emplace_back(extent);
      target->op_ctx = ctx;
      // tail block's data maybe need in append case
      if (blockOffset != 0 &&
          (!file.tail_block || file.tail_block->position() == 0)) {
        ByteBuffer* tailBlockBuf = file.getTailBlock();
        // advance position to block's write cursor
        tailBlockBuf->position(blockOffset);
        spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
            tailBlockBuf->p_buffer_, extent.offset, fs_context.blockSize,
            writeExtent, target);
      } else {
        writeExtent(nullptr, true, target);
      }
    } else { // random write
      extent.offset = (extent.extentId << fs_context.extentBits);
      extent.len = fs_context.extentSize;
      if (writeCtx->writingSize >= writeCtx->count) {
        writeCtx->writingSize = writeCtx->count;
      }
      extent.bufLen = writeCtx->writingSize;
      target = &writeCtx->writeExtents.emplace_back(extent);
      target->op_ctx = ctx;
      writeExtent(nullptr, true, target);
    }
  }

  // write other extents
  while (writeCtx->writingSize < writeCtx->count) {
    extent.extentOff = 0;
    extent.newId = 0;
    if (!file.inode_->getExtent(
        offset + writeCtx->writingSize, extent.index, extent.extentId)) {
      extent.extentId = fs_context.allocator->allocate();
      if (extent.extentId == UINT32_MAX) {
        SPDK_WARNLOG("no free extents.\n");
        // free allocated extents
        for (auto& e : writeCtx->writeExtents) {
          if (e.newId) {
            fs_context.allocator->release(e.newId);
          }
        }
        writeCtx->writeExtents.clear();
        return ctx->callback(ctx->cb_args, -4/*NoFreeExtents*/);
      }
      extent.newId = extent.extentId;
    }
    extent.offset = (extent.extentId << fs_context.extentBits);
    extent.bufOff = writeCtx->writingSize;
    uint32_t remaining = writeCtx->remainingSize();
    if (remaining < fs_context.extentSize) { // the last extent
      if (writeCtx->append) {
        extent.len = (remaining + fs_context.blockMask) & ~(fs_context.blockMask);
      } else {
        extent.len = fs_context.extentSize;
      }
      extent.bufLen = remaining;
      writeCtx->writingSize = writeCtx->count;
    } else {
      extent.len = fs_context.extentSize;
      extent.bufLen = fs_context.extentSize;
      writeCtx->writingSize += fs_context.extentSize;
    }
    auto& target = writeCtx->writeExtents.emplace_back(extent);
    target.op_ctx = ctx;
    writeExtent(nullptr, true, &target);
  }
}

static void paddingZero(struct spdk_bdev_io* bdev_io, bool success, void* cb_arg) {
  spdk_bdev_free_io(bdev_io);
  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  fs_op_context* ctx = reinterpret_cast<fs_op_context*>(buffer->private_data);
  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx->private_data);
  auto& fs_context = FastFS::fs_context;
  if (!success) {
    ctx->fastfs->freeBuffer(buffer);
    return ctx->callback(ctx->cb_args, -2/*PaddingZeroFailed*/);
  }
  uint64_t extentOffset =
      writeCtx->file->inode_->size_ & fs_context.extentMask;
  uint64_t len = fs_context.extentSize - extentOffset;
  memset(buffer->p_buffer_ + extentOffset, 0, len);
  spdk_bdev_write(fs_context.bdev_desc, fs_context.bdev_io_channel,
      buffer->p_buffer_, buffer->mark_, fs_context.extentSize, writeRange, buffer);
}

void FastFS::write(fs_op_context& ctx) {
  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(ctx.private_data);
  if (writeCtx->fd > fs_context.maxFiles) {
    return ctx.callback(ctx.cb_args, -1);
  }
  FastFile& file = (*files)[writeCtx->fd];

  // determine write offset
  uint64_t offset = file.pos_;
  if (writeCtx->append) {
    offset = file.inode_->size_;
  } else if (writeCtx->pwrite) {
    offset = writeCtx->offset;
  }
  writeCtx->reset(&file, offset);

  if (offset > file.inode_->size_) {
    auto& extents = *(file.inode_->extents_);
    if (extents.size() > 0) {
      // padding zero with last extent
      uint32_t extentId = extents[extents.size() - 1];
      if (extentId != UINT32_MAX) { // truncate case
        uint64_t offset = (extentId << fs_context.extentBits);
        ByteBuffer* buffer = allocBuffer();
        buffer->private_data = &ctx;
        buffer->mark_ = offset;
        spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
            buffer->p_buffer_, offset, fs_context.extentSize, paddingZero, buffer);
        return;
      }
    }
  }
  writeRange(nullptr, true, &ctx);
}

static void fsyncComplete(void* cb_args, int code) {
  fs_op_context* ctx = reinterpret_cast<fs_op_context*>(cb_args);
  FSyncContext* syncCtx = reinterpret_cast<FSyncContext*>(ctx->private_data);
  // release dirty extents
  if (syncCtx->dirtyExtents) {
    for (auto& [index, extentInfo] : *syncCtx->dirtyExtents) {
      if (extentInfo.first != UINT32_MAX) {
        FastFS::fs_context.allocator->release(extentInfo.first);
      }
    }
    delete syncCtx->dirtyExtents;
  }
  ctx->fastfs->journal->freeEditOp();
  ctx->callback(ctx->cb_args, code);
}

void FastFS::fsync(fs_op_context& ctx) {
  FSyncContext* syncCtx = reinterpret_cast<FSyncContext*>(ctx.private_data);
  if (syncCtx->fd > fs_context.maxFiles) {
    return ctx.callback(ctx.cb_args, -1);
  }

  int32_t size = 14; /*ino(4) + size(8) + extentsCnt(2)*/
  FastFile& file = (*files)[syncCtx->fd];
  syncCtx->file = &file;
  syncCtx->dirtyExtents = nullptr;
  if (file.inode_->dirtyExtents && file.inode_->dirtyExtents->size() > 0) {
    syncCtx->dirtyExtents = file.inode_->dirtyExtents;
    file.inode_->dirtyExtents = new ExtentMap();
    size += syncCtx->dirtyExtents->size() * 8;
  }

  if (fs_context.skipJournal) {
    return fsyncComplete(&ctx, 0);
  }

  EditOp* editOp = journal->allocEditOp();
  editOp->opctx = syncCtx;
  editOp->type = 4;
  editOp->size = size;
  editOp->callback = fsyncComplete;
  editOp->cb_args = &ctx;
  editOp->phrase = !editOp->phrase;
}
