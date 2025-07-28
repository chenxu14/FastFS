/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "FastFS.h"

static void replayNextExtent(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg);

static int journalPollEditOp(void *arg) {
  FastJournal* journal = reinterpret_cast<FastJournal*>(arg);
  return journal->pollEditOp();
}

FastJournal::FastJournal(const fs_context_t& context) : fs_context(context) {
  extentBlocks = fs_context.allocator->getExtentBlocks();
  epoch = fs_context.superBlock.epoch;
  tail_block_buffer = (char*) spdk_dma_zmalloc_socket(
      fs_context.blockSize, fs_context.bufAlign, NULL, fs_context.localNuma);
  tail_block = new ByteBuffer(tail_block_buffer, fs_context.blockSize);
  resetTailBlock();
  initObjPool();
  op_poller = SPDK_POLLER_REGISTER(journalPollEditOp, this, 0);
}

FastJournal::~FastJournal() {
  if (tail_block) {
    spdk_dma_free(tail_block_buffer);
    delete tail_block;
    tail_block = nullptr;
  }
  if (op_poller) {
    spdk_poller_unregister(&op_poller);
  }
  if (editObjs) {
    delete editObjs;
  }
}

static void parseExtent(ByteBuffer* buffer, uint32_t startOff, uint32_t skipOps) {
  FastJournal* journal = reinterpret_cast<FastJournal*>(buffer->private_data);
  auto& fs_context = journal->fs_context;
  FastFS* fastfs = fs_context.fastfs;

  char flag = 1;
  uint32_t offset = startOff;
  bool blockCorect = true;
  while (flag == 1 && offset < buffer->limit()) {
    buffer->position(offset).getByte(flag);
    if (flag != 0 && flag != 1) {
      SPDK_WARNLOG("block %ld's next flag incorrect!\n",
          (journal->offset + offset) >> fs_context.blockBits);
      blockCorect = false;
      break;
    }
    uint32_t epochNum = 0;
    if (!buffer->read<uint32_t>(epochNum) || epochNum != journal->epoch) {
      SPDK_WARNLOG("block %ld's epoch is %d, expect %d\n",
          (journal->offset + offset) >> fs_context.blockBits,
          epochNum, journal->epoch);
      blockCorect = false;
      break;
    }
    uint64_t txid = 0;
    if (!buffer->read<uint64_t>(txid) || txid != journal->txid) {
      SPDK_WARNLOG("block %ld's txid is %ld, but expect %ld\n",
          (journal->offset + offset) >> fs_context.blockBits,
          txid, journal->txid);
      blockCorect = false;
      break;
    }

    // parse OP record
    buffer->read<uint32_t>(journal->num_ops);
    bool parsed = true;
    for (uint32_t i = 0; i < journal->num_ops; i++) {
      char opType = -1;
      int32_t opSize = -1;
      buffer->getByte(opType);
      buffer->read<int32_t>(opSize);
      if (skipOps > 0) {
        buffer->skip(opSize);
        skipOps--;
        continue;
      }
      // TODO chenxu14 consider disk data corruption
      switch (opType) {
        case 0 : { // createOp
          journal->createCtx.deserialize(buffer);
          if (fastfs->applyCreate(&journal->createCtx) != 0) {
            parsed = false;
          }
          break;
        }
        case 1 : { // truncateOp
          journal->truncateCtx.deserialize(buffer);
          if (fastfs->applyTruncate(&journal->truncateCtx) != 0) {
            parsed = false;
          }
          break;
        }
        case 2 : { // deleteOp
          journal->delCtx.deserialize(buffer);
          if (fastfs->applyRemove(&journal->delCtx) != 0) {
            parsed = false;
          }
          break;
        }
        case 3 : { // allocOp
          buffer->read<uint32_t>(journal->nextExtentId);
          fs_context.allocator->reserve(journal->nextExtentId);
          journal->extents_.push_front(journal->nextExtentId);
          break;
        }
        case 4 : { // fsyncOp
          uint32_t ino = 0;
          buffer->read<uint32_t>(ino);
          FastInode& inode = (*fastfs->inodes)[ino];
          uint64_t size = 0;
          buffer->read<uint64_t>(size);
          inode.size_ = size;
          uint16_t counts = 0;
          buffer->read<uint16_t>(counts);
          auto& extents = *inode.extents_;
          for (int i = 0; i < counts; i++) {
            uint32_t index = 0;
            uint32_t extentId = 0;
            if (!buffer->read<uint32_t>(index) || !buffer->read<uint32_t>(extentId)
                || (size >> fs_context.extentBits) < index) {
              parsed = false;
              break;
            }
            for (uint32_t i = extents.size(); i <= index; i++) {
              extents.emplace_back(UINT32_MAX);
            }
            if (extents[index] != UINT32_MAX) {
              fs_context.allocator->release(extents[index]);
            }
            extents[index] = extentId;
            fs_context.allocator->reserve(extentId);
          }
          break;
        }
        case 5 : { // renameOp
          journal->renameCtx.deserialize(buffer);
          if (fastfs->applyRename(&journal->renameCtx) != 0) {
            parsed = false;
          }
          break;
        }
        default: {
          parsed = false;
          SPDK_WARNLOG("Unknown operation type %d\n", opType);
          break;
        }
      }
      if (!parsed) {
        SPDK_WARNLOG("block %ld's OP record can't parse successfully!\n",
            (journal->offset + offset) >> fs_context.blockBits);
      }
      journal->replayOps++;
    }

    if (flag == 1) { // has next block
      journal->advanceTxid();
      offset += fs_context.blockSize;
    }
  }

  // read next extent
  if (flag == 1 && journal->nextExtentId > 0) {
    buffer->clear();
    journal->offset = (journal->nextExtentId << fs_context.extentBits);
    journal->curBlock = 0;
    spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
        buffer->p_buffer_, journal->offset, fs_context.extentSize,
        replayNextExtent, buffer);
    return;
  }

  journal->offset += offset;
  journal->curBlock = (offset >> fs_context.blockBits);
  if (blockCorect) {
    // reset tail block
    journal->tail_block->putBytes(
        0, buffer->p_buffer_ + offset, fs_context.blockSize);
    journal->tail_block->position(buffer->position() & fs_context.blockMask);
  } else if (journal->offset != fs_context.extentSize) { // exclude format case
    SPDK_WARNLOG(
        "block %d's data not as expected, FastFS does't stop gracefully?\n",
        journal->curBlock >> fs_context.blockBits);
  }
  fastfs->freeBuffer(buffer);
  SPDK_NOTICELOG(
      "replay journal finished, total replay %ld OPs\n", journal->replayOps);
  fastfs->dumpInfo();
  fs_context.callback(fastfs, 0);
}

static void replayNextExtent(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  FastJournal* journal = reinterpret_cast<FastJournal*>(buffer->private_data);
  auto& fs_context = journal->fs_context;
  FastFS* fastfs = fs_context.fastfs;
  journal->nextExtentId = 0; // clear next
  if (!success) {
    fs_context.callback(fastfs, -8/*read extent failed*/);
    return;
  }
  parseExtent(buffer, 0, 0);
}

static void replayFirstExtent(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  if (bdev_io) {
    spdk_bdev_free_io(bdev_io);
  }
  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  FastJournal* journal = reinterpret_cast<FastJournal*>(buffer->private_data);
  auto& fs_context = journal->fs_context;
  journal->txid = fs_context.superBlock.lastTxid;
  uint32_t skipBlocks = fs_context.superBlock.journalSkipBlocks;
  uint32_t skipOps = fs_context.superBlock.journalSkipOps;
  parseExtent(buffer, skipBlocks << fs_context.blockBits, skipOps);
}

void FastJournal::logReplay() {
  uint64_t extentId = fs_context.superBlock.journalLoc;
  offset = extentId << fs_context.extentBits;
  // reserve journal's start extent
  fs_context.allocator->reserve(extentId);
  extents_.push_front(extentId);
  ByteBuffer* extentBuf = fs_context.fastfs->allocBuffer();
  extentBuf->private_data = this;
  replayOps = 0;
  spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
      extentBuf->p_buffer_, offset, fs_context.extentSize,
      replayFirstExtent, extentBuf);
}

void FastJournal::startCheckpoint() {
  cusor = extents_.cbegin();
  SPDK_NOTICELOG("Journal's current extent : %d\n", *cusor);
}

void FastJournal::writeComplete(int code) {
  int preIdx = commitIdx - inflights;
  if (preIdx >= 0) {
    for (int i = preIdx; i < commitIdx; i++) {
      (*editObjs)[i].callback((*editObjs)[i].cb_args, code);
    }
  } else {
    for (int i = (poolSize + commitIdx - inflights); i < poolSize; i++) {
      (*editObjs)[i].callback((*editObjs)[i].cb_args, code);
    }
    for (int i = 0; i < commitIdx; i++) {
      (*editObjs)[i].callback((*editObjs)[i].cb_args, code);
    }
  }
  inflights = 0;

  if (tail_block_full) {
    resetTailBlock();
    tail_block_full = false;
    curBlock++;
    if (curBlock == extentBlocks) {
      offset = nextExtentId << fs_context.extentBits;
      curBlock = 0;
    } else {
      offset += fs_context.blockSize;
      // [NOTICE] extent should at least two block
      if (curBlock == extentBlocks - 1) {
        uint32_t extentId = fs_context.allocator->allocate();
        if (extentId == UINT32_MAX) {
          SPDK_WARNLOG("no free extents.\n");
          waiting = true;
        } else {
          allocNewExtent(extentId);
        }
      }
    }
  }
}

static void journalWriteComplete(
    struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
  FastJournal* journal = reinterpret_cast<FastJournal*>(cb_arg);
  spdk_bdev_free_io(bdev_io);
  journal->writeComplete(success ? 0 : -1);
  // TODO chenxu14 consider exit when journal write failed
}

int FastJournal::pollEditOp() {
  int status = SPDK_POLLER_IDLE;
  if (waiting) {
    uint32_t extentId = fs_context.allocator->allocate();
    if (extentId == UINT32_MAX) {
      return status;
    }
    allocNewExtent(extentId);
    waiting = false;
  }
  if (inflights > 0 || tail_block_full) {
    return status; // previous transaction has not been commit
  }
  EditOp* editOp = &(*editObjs)[commitIdx];
  // iterate committed OP
  while (editOp->phrase ^ phrase) {
    int32_t size = editOp->size;
    if (tail_block->writable(size + 5/*opcode(1) and size(4)*/)) {
      tail_block->pwrite<uint32_t>(kNumOpsIndex, ++num_ops);
      tail_block->putByte(editOp->type);
      tail_block->write<int32_t>(size);
      switch (editOp->type) {
        case 0 : {
          CreateContext* createCtx = reinterpret_cast<CreateContext*>(editOp->opctx);
          createCtx->serialize(tail_block);
          break;
        }
        case 1 : {
          TruncateContext* truncateCtx = reinterpret_cast<TruncateContext*>(editOp->opctx);
          truncateCtx->serialize(tail_block);
          break;
        }
        case 2 : {
          DeleteContext* delCtx = reinterpret_cast<DeleteContext*>(editOp->opctx);
          delCtx->serialize(tail_block);
          break;
        }
        case 4 : {
          FSyncContext* syncCtx = reinterpret_cast<FSyncContext*>(editOp->opctx);
          syncCtx->serialize(tail_block);
          break;
        }
        case 5 : {
          RenameContext* renameCtx = reinterpret_cast<RenameContext*>(editOp->opctx);
          renameCtx->serialize(tail_block);
          break;
        }
        default : {
          break;
        }
      }

      // advance commitIdx
      commitIdx++;
      if (commitIdx == poolSize) {
        commitIdx = 0;
        phrase = !phrase;
      }
      editOp = &(*editObjs)[commitIdx];
      inflights++;
    } else {
      tail_block_full = true;
      tail_block->putByte(0, 1); // change next flag
      if (inflights > 0) {
        status = SPDK_POLLER_BUSY;
      }
      int rc = spdk_bdev_write(
          fs_context.bdev_desc, fs_context.bdev_io_channel, tail_block_buffer,
          offset, fs_context.blockSize, journalWriteComplete, this);
      if (rc == -ENOMEM) {
        // TODO chenxu14 do back off
      }
      return status;
    }
  }

  if (inflights > 0) {
    status = SPDK_POLLER_BUSY;
    int rc = spdk_bdev_write(
        fs_context.bdev_desc, fs_context.bdev_io_channel, tail_block_buffer,
        offset, fs_context.blockSize, journalWriteComplete, this);
    if (rc == -ENOMEM) {
      // TODO chenxu14 do back off
    }
  }
  return status;
}

void FastJournal::initObjPool() {
  allocIdx = 0;
  poolSize = DEFAULT_POOL_SIZE;
  limitIdx = poolSize - 1;
  poolMask = limitIdx;
  editObjs = new std::vector<EditOp, MemAllocator<EditOp>>(
      MemAllocator<EditOp>(fs_context.localNuma, 64));
  editObjs->reserve(poolSize);
  for (int i = 0; i < poolSize; i++) {
    (*editObjs)[i].phrase = false;
  }
  commitIdx = 0;
  phrase = false;
}

void FastJournal::dumpInfo() {
  uint64_t extentId = offset >> fs_context.extentBits;
  std::string nextExtent = "None";
  if (nextExtentId > 0 && nextExtentId != extentId) {
    nextExtent = std::to_string(nextExtentId);
  }
  SPDK_NOTICELOG(
      "epoch %d, txid %ld, extentId %ld, nextExtent %s, blockId %d, num_ops %d\n",
      epoch, txid, extentId, nextExtent.c_str(), curBlock, num_ops);
}
