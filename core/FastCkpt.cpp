/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "FastFS.h"

static constexpr int32_t kMinExtentSize = 21;
static void ckptINode(ByteBuffer *extentBuf);
static void ckptDentryAndExtents(ByteBuffer *extentBuf);

static void loadSuccess(ByteBuffer *extentBuf) {
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  fs_context_t &ctx = FastFS::fs_context;
  fastfs->freeBuffer(extentBuf);
  SPDK_NOTICELOG("load checkpoint success, do replay journal now.\n");
  // replay journal
  fastfs->journal = new FastJournal(ctx);
  fastfs->journal->logReplay();
}

bool FastCkpt::parseExtent(ByteBuffer *extentBuf, uint32_t &nextExtent, INodeCache &inodes) {
  uint32_t numOps = 0;
  extentBuf->read<uint32_t>(nextExtent);
  extentBuf->read<uint32_t>(numOps);
  char opType = -1;
  for (uint32_t i = 0; i < numOps; i++) {
    extentBuf->getByte(opType);
    if (opType == FASTFS_REGULAR_FILE) {
      uint32_t ino = UINT32_MAX;
      uint32_t extentCount = 0;
      uint32_t extentId = 0;
      if (extentBuf->read<uint32_t>(ino) && ino < fs_context.maxInodes) {
        extentBuf->read<uint32_t>(extentCount);
        for (uint32_t j = 0; j < extentCount; j++) {
          extentBuf->read<uint32_t>(extentId);
          inodes[ino].extents_->emplace_back(extentId);
          fs_context.allocator->reserve(extentId);
        }
      } else {
        SPDK_WARNLOG("file's inodeId overflow\n");
        return false;
      }
    } else if (opType == FASTFS_DIR) {
      uint32_t ino = UINT32_MAX;
      uint32_t childCount = 0;
      uint32_t childIno = 0;
      if (extentBuf->read<uint32_t>(ino) && ino < fs_context.maxInodes) {
        extentBuf->read<uint32_t>(childCount);
        for (uint32_t j = 0; j < childCount; j++) {
          if (extentBuf->read<uint32_t>(childIno) && childIno < fs_context.maxInodes) {
            inodes[ino].children_->insert(childIno);
          } else {
            SPDK_WARNLOG("child's inodeId overflow\n");
            return false;
          }
        }
      } else {
        SPDK_WARNLOG("dir's inodeId overflow\n");
        return false;
      }
    } else {
      SPDK_WARNLOG("Unknown operation type %d\n", opType);
      return false;
    }
  }
  return true;
}

static void loadDentryAndExtents(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *extentBuf = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  fs_context_t &ctx = FastFS::fs_context;
  if (!success) {
    fastfs->freeBuffer(extentBuf);
    ctx.callback(fastfs, -7 /*load dentry failed*/);
    return;
  }

  uint32_t nextExtent = 0;
  if (!fastfs->checkpoint->parseExtent(extentBuf, nextExtent, *fastfs->inodes)) {
    SPDK_WARNLOG("extent %d's OP record can't parse successfully!\n", fastfs->checkpoint->curExtent);
    fastfs->freeBuffer(extentBuf);
    FastFS::fs_context.callback(fastfs, -8);
    return;
  }

  if (nextExtent > 0) {
    ctx.allocator->reserve(nextExtent);
    fastfs->checkpoint->extents_.push_front(nextExtent);
    uint64_t bdevOffset = static_cast<uint64_t>(nextExtent) << ctx.extentBits;
    extentBuf->clear();
    spdk_bdev_read(ctx.bdev_desc,
                   ctx.bdev_io_channel,
                   extentBuf->p_buffer_,
                   bdevOffset,
                   ctx.extentSize,
                   loadDentryAndExtents,
                   extentBuf);
    return;
  }

  loadSuccess(extentBuf);
}

static void loadInodes(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *extentBuf = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  fs_context_t &ctx = FastFS::fs_context;
  if (!success) {
    fastfs->freeBuffer(extentBuf);
    ctx.callback(fastfs, -3 /*read extent failed*/);
    return;
  }

  uint32_t nextExtent = 0;
  uint32_t numOps = 0;
  extentBuf->read<uint32_t>(nextExtent);
  extentBuf->read<uint32_t>(numOps);
  int32_t opSize = -1;
  uint32_t pre = 0;
  uint32_t ino = 0;
  bool head = false;
  for (uint32_t i = 0; i < numOps; i++) {
    extentBuf->read<int32_t>(opSize);
    auto &inodeProto = fastfs->checkpoint->inodeProto;
    if (inodeProto.deserialize(extentBuf)) {
      if (inodeProto.ino >= FastFS::fs_context.maxInodes) {
        SPDK_WARNLOG("No available inodes");
        fastfs->freeBuffer(extentBuf);
        ctx.callback(fastfs, -5);
        return;
      }

      // recover HashSlots
      head = fastfs->lookup(inodeProto.parent_id, inodeProto.name, pre, ino);
      if (head) {
        (*fastfs->slots)[pre] = inodeProto.ino;
      } else {
        (*fastfs->inodes)[pre].next_ = inodeProto.ino;
      }

      FastFS::fs_context.inodeAllocator->reserve(inodeProto.ino);
      FastInode &inode = (*fastfs->inodes)[inodeProto.ino];
      inode.create(inodeProto.ino, inodeProto.parent_id, inodeProto.name, inodeProto.type);
      inode.size_ = inodeProto.size;
      inode.mode_ = inodeProto.mode;
    } else {
      SPDK_WARNLOG("extent %d's OP record can't parse successfully!\n", fastfs->checkpoint->curExtent);
      fastfs->freeBuffer(extentBuf);
      ctx.callback(fastfs, -6);
      return;
    }
  }

  if (nextExtent > 0) {
    ctx.allocator->reserve(nextExtent);
    fastfs->checkpoint->extents_.push_front(nextExtent);
    uint64_t bdevOffset = static_cast<uint64_t>(nextExtent) << ctx.extentBits;
    extentBuf->clear();
    spdk_bdev_read(
      ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_, bdevOffset, ctx.extentSize, loadInodes, extentBuf);
    return;
  }

  uint32_t dentryLocation = ctx.superBlock.ckptDentryLoc;
  if (dentryLocation > 0) {
    ctx.allocator->reserve(dentryLocation);
    fastfs->checkpoint->dentryLocation = dentryLocation;
    fastfs->checkpoint->extents_.push_front(dentryLocation);
    uint64_t bdevOffset = static_cast<uint64_t>(dentryLocation) << ctx.extentBits;
    extentBuf->clear();
    spdk_bdev_read(ctx.bdev_desc,
                   ctx.bdev_io_channel,
                   extentBuf->p_buffer_,
                   bdevOffset,
                   ctx.extentSize,
                   loadDentryAndExtents,
                   extentBuf);
  } else {
    loadSuccess(extentBuf);
  }
}

void FastCkpt::loadImage() {
  ByteBuffer *extentBuf = fs_context.fastfs->allocBuffer();
  extentBuf->private_data = fs_context.fastfs;
  inodesLocation = fs_context.superBlock.ckptInodesLoc;
  if (inodesLocation > 0) {
    fs_context.allocator->reserve(inodesLocation);
    extents_.push_front(inodesLocation);
    uint64_t bdevOffset = static_cast<uint64_t>(inodesLocation) << fs_context.extentBits;
    spdk_bdev_read(fs_context.bdev_desc,
                   fs_context.bdev_io_channel,
                   extentBuf->p_buffer_,
                   bdevOffset,
                   fs_context.extentSize,
                   loadInodes,
                   extentBuf);
  } else {
    loadSuccess(extentBuf);
  }
}

static void writeSuperBlockComplete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *extentBuf = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  FastCkpt &ckpt = *fastfs->checkpoint;

  if (success) {
    SPDK_NOTICELOG("checkpoint finished, inodes begin at %d, "
                   "dentry begin at %d, inodes extents %d, dentry extents %d\n",
                   ckpt.inodesLocation,
                   ckpt.dentryLocation,
                   ckpt.inodeExtents,
                   ckpt.dentryExtents);
    // release Journal's old extents
    auto &journalExtents = fastfs->journal->extents_;
    int count = 0;
    for (auto it = fastfs->journal->cusor; it != journalExtents.end(); it++) {
      count++;
    }
    journalExtents.erase_after(fastfs->journal->cusor, journalExtents.end());
    SPDK_NOTICELOG("release %d useless extents of journal.\n", count - 1);
  } else {
    SPDK_ERRLOG("do checkpoint failed, can't save super block.\n");
  }

  fastfs->freeBuffer(extentBuf);
  int count = ckpt.releaseExtents(); // release old checkpoint's extents
  SPDK_NOTICELOG("release %d useless extents of checkpoint.\n", count);
  ckpt.ckpt_cb(fastfs, success ? 0 : -4);
}

static void writeSuperBlock(ByteBuffer *buffer) {
  FastFS *fastfs = reinterpret_cast<FastFS *>(buffer->private_data);
  FastCkpt &ckpt = *fastfs->checkpoint;
  fs_context_t &ctx = FastFS::fs_context;
  ctx.superBlock.ckptInodesLoc = ckpt.inodesLocation;
  ctx.superBlock.ckptDentryLoc = ckpt.dentryLocation;
  ctx.superBlock.lastTxid = fastfs->journal->txid;
  // reset journal location
  ctx.superBlock.journalLoc = fastfs->journal->extents_.front();
  ctx.superBlock.journalSkipBlocks = fastfs->journal->curBlock;
  ctx.superBlock.journalSkipOps = fastfs->journal->num_ops;
  buffer->clear();
  ctx.superBlock.serialize(buffer);
  spdk_bdev_write(
    ctx.bdev_desc, ctx.bdev_io_channel, buffer->p_buffer_, 0, ctx.blockSize, writeSuperBlockComplete, buffer);
}

static void ckptDentryComplete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *extentBuf = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  FastCkpt &ckpt = *fastfs->checkpoint;

  if (success) {
    ckpt.curExtent = ckpt.nextExtent;
    ckptDentryAndExtents(extentBuf);
  } else {
    SPDK_ERRLOG("checkpoint dentry and extents failed\n");
    fastfs->freeBuffer(extentBuf);
    ckpt.releaseExtents();
    ckpt.ckpt_cb(fastfs, -3);
  }
}

static void ckptDentryAndExtents(ByteBuffer *extentBuf) {
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  FastCkpt &ckpt = *fastfs->checkpoint;

  // reset extent buffer
  extentBuf->clear();
  extentBuf->write<uint32_t>(0); // next extentId
  extentBuf->write<uint32_t>(0); // num_ops in current extent
  ckpt.nextExtent = 0;
  int count = 0;
  while (ckpt.cusor != fastfs->inodes->end()) {
    auto &inode = *ckpt.cusor;
    if (inode.status_ == 1 && inode.type_ == FASTFS_REGULAR_FILE && inode.extents_->size() > 0) {
      if (ckpt.extentEnd) {
        ckpt.extentCusor = inode.extents_->begin();
        ckpt.extentEnd = false;
      }
      int opSize = 9; // type(1), ino(4), num(4)
      bool metaWrite = false;
      uint32_t childCount = 0;
      while (ckpt.extentCusor != inode.extents_->end()) {
        opSize += 4; // extentId
        if (extentBuf->writable(opSize)) {
          if (!metaWrite) {
            extentBuf->putByte(inode.type_);
            extentBuf->write<uint32_t>(inode.ino_);
            extentBuf->mark().write<uint32_t>(0); // child count
            metaWrite = true;
          }
          extentBuf->write<uint32_t>(*ckpt.extentCusor);
          childCount++;
          ckpt.extentCusor++;
          opSize = 0;
        } else { // extent full
          if (metaWrite) {
            extentBuf->pwrite<uint32_t>(extentBuf->mark_, childCount);
            count++;
          }
          ckpt.nextExtent = FastFS::fs_context.allocator->allocate();
          extentBuf->pwrite<uint32_t>(0, ckpt.nextExtent);
          ckpt.dentryExtents++;
          goto endLoop;
        }
      }
      extentBuf->pwrite<uint32_t>(extentBuf->mark_, childCount);
      ckpt.extentEnd = true;
    } else if (inode.status_ == 1 && inode.type_ == FASTFS_DIR && inode.children_->size() > 0) {
      if (ckpt.dentryEnd) {
        ckpt.dentryCusor = inode.children_->begin();
        ckpt.dentryEnd = false;
      }

      int opSize = 9; // type(1), ino(4), num(4)
      bool metaWrite = false;
      uint32_t childCount = 0;
      while (ckpt.dentryCusor != inode.children_->end()) {
        opSize += 4; // child inodeId
        if (extentBuf->writable(opSize)) {
          if (!metaWrite) {
            extentBuf->putByte(inode.type_);
            extentBuf->write<uint32_t>(inode.ino_);
            extentBuf->mark().write<uint32_t>(0); // child count
            metaWrite = true;
          }
          extentBuf->write<uint32_t>(*ckpt.dentryCusor);
          childCount++;
          ckpt.dentryCusor++;
          opSize = 0;
        } else { // extent full
          if (metaWrite) {
            extentBuf->pwrite<uint32_t>(extentBuf->mark_, childCount);
            count++;
          }
          ckpt.nextExtent = FastFS::fs_context.allocator->allocate();
          extentBuf->pwrite<uint32_t>(0, ckpt.nextExtent);
          ckpt.dentryExtents++;
          goto endLoop;
        }
      }
      extentBuf->pwrite<uint32_t>(extentBuf->mark_, childCount);
      ckpt.dentryEnd = true;
    } else {
      ckpt.cusor++;
      continue;
    }
    count++;
    ckpt.cusor++;
  }

endLoop:
  fs_context_t &ctx = FastFS::fs_context;
  if (count > 0) {
    extentBuf->pwrite<uint32_t>(4, count); // update count
    uint64_t offset = static_cast<uint64_t>(ckpt.curExtent) << ctx.extentBits;
    spdk_bdev_write(
      ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_, offset, ctx.extentSize, ckptDentryComplete, extentBuf);
  } else {
    writeSuperBlock(extentBuf);
  }
}

static void ckptINodeComplete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer *extentBuf = reinterpret_cast<ByteBuffer *>(cb_arg);
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  FastCkpt &ckpt = *fastfs->checkpoint;
  if (success) {
    ckpt.curExtent = ckpt.nextExtent;
    ckptINode(extentBuf);
  } else {
    SPDK_ERRLOG("checkpoint inodes failed\n");
    fastfs->freeBuffer(extentBuf);
    ckpt.releaseExtents();
    ckpt.ckpt_cb(fastfs, -2);
  }
}

static void ckptINode(ByteBuffer *extentBuf) {
  FastFS *fastfs = reinterpret_cast<FastFS *>(extentBuf->private_data);
  FastCkpt &ckpt = *fastfs->checkpoint;
  // reset extent buffer
  extentBuf->clear();
  extentBuf->write<uint32_t>(0); // next extentId
  extentBuf->write<uint32_t>(0); // num_ops in current extent
  ckpt.nextExtent = 0;

  // serialize INodes
  int count = 0;
  while (ckpt.cusor != fastfs->inodes->end()) {
    auto &inode = *ckpt.cusor;
    if (inode.status_ != 1) {
      ckpt.cusor++;
      continue;
    }
    ckpt.inodeProto.ino = inode.ino_;
    ckpt.inodeProto.parent_id = inode.parentId_;
    ckpt.inodeProto.name = inode.name_;
    ckpt.inodeProto.size = inode.size_;
    ckpt.inodeProto.type = inode.type_;
    ckpt.inodeProto.mode = inode.mode_;

    int size = INodeFile::kFixSize + ckpt.inodeProto.name.size();
    if (extentBuf->writable(size + 5 /*1 type and 4 size*/)) {
      extentBuf->write<int32_t>(size);
      ckpt.inodeProto.serialize(extentBuf);
    } else { // extents full
      ckpt.nextExtent = FastFS::fs_context.allocator->allocate();
      extentBuf->pwrite<uint32_t>(0, ckpt.nextExtent);
      ckpt.inodeExtents++;
      break;
    }
    count++;
    ckpt.cusor++;
  }

  if (count > 0) {
    if (ckpt.curExtent == 0) {
      SPDK_ERRLOG("checkpoint has wrong extentId.\n");
      ckpt.releaseExtents();
      return ckpt.ckpt_cb(fastfs, -1);
    }
    extentBuf->pwrite<uint32_t>(4, count); // update count
    // write extent
    fs_context_t &ctx = FastFS::fs_context;
    uint64_t offset = static_cast<uint64_t>(ckpt.curExtent) << ctx.extentBits;
    spdk_bdev_write(
      ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_, offset, ctx.extentSize, ckptINodeComplete, extentBuf);
  } else {
    ckpt.cusor = fastfs->inodes->begin();
    ckpt.dentryLocation = FastFS::fs_context.allocator->allocate();
    ckpt.dentryExtents = 1;
    ckpt.curExtent = ckpt.dentryLocation;
    ckptDentryAndExtents(extentBuf);
  }
}

void FastCkpt::checkpoint(fs_cb callback) {
  uint32_t count = fs_context.inodeAllocator->getAllocated();
  SPDK_NOTICELOG("do checkpoint now, total inodes %d\n", count);
  ckpt_cb = callback;
  ByteBuffer *extentBuf = fs_context.fastfs->allocBuffer();
  extentBuf->private_data = fs_context.fastfs;
  // notify FastJournal first
  fs_context.fastfs->journal->startCheckpoint();

  // if only root, no need to do checkpoint
  if (count <= 1) {
    inodesLocation = 0;
    dentryLocation = 0;
    inodeExtents = 0;
    dentryExtents = 0;
    writeSuperBlock(extentBuf);
    return;
  }

  inodesLocation = fs_context.allocator->allocate();
  inodeExtents = 1;
  cusor = fs_context.fastfs->inodes->begin();
  curExtent = inodesLocation;
  ckptINode(extentBuf);
}
