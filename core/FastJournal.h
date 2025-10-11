/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FAST_JOURNAL_H_
#define FAST_JOURNAL_H_

#include <forward_list>
#include <list>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "Allocator.h"
#include "ByteBuffer.h"
#include "Serialization.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#define DEFAULT_POOL_SIZE 256
static constexpr int32_t kNumOpsIndex = 13;
static constexpr int32_t kTxidIndex = 5;
static constexpr char kDelimiter = '/';

struct fs_context_t;
class FastJournal;
class FastFS;

typedef void (*fs_cb)(FastFS *fastfs, int code);

struct EditOp {
  op_cb callback;
  void *cb_args;
  void *opctx;
  int32_t size;
  char type;
  bool phrase;
  char padding[2]; // align 32 bytes
};

class FastJournal {
  private:
  struct spdk_poller *op_poller = nullptr;
  char *tail_block_buffer = nullptr;
  bool tail_block_full = false;
  uint32_t extentBlocks;
  int allocIdx;
  int limitIdx;
  int poolSize;
  int poolMask;
  int commitIdx;
  std::vector<EditOp, MemAllocator<EditOp>> *editObjs = nullptr;
  int inflights = 0;
  bool phrase = false;
  bool waiting = false;

  public:
  const fs_context_t &fs_context;
  DeleteContext delCtx;
  CreateContext createCtx;
  TruncateContext truncateCtx;
  RenameContext renameCtx;
  std::forward_list<uint32_t> extents_;
  std::forward_list<uint32_t>::const_iterator cusor;
  ByteBuffer *tail_block = nullptr;
  uint32_t nextExtentId = 0;
  uint64_t replayOps = 0;
  uint64_t offset = 0;
  uint32_t curBlock = 0;
  uint64_t txid = 0;
  uint32_t num_ops = 0;
  uint32_t epoch;

  public:
  FastJournal(const fs_context_t &context);
  ~FastJournal();

  int pollEditOp();

  void writeComplete(int code);

  /**
   * replay journal to decide tail offset
   */
  void logReplay();

  void startCheckpoint();

  EditOp *allocEditOp() {
    EditOp *res = nullptr;
    if (allocIdx != limitIdx) {
      res = &(*editObjs)[allocIdx++];
      allocIdx &= poolMask;
    }
    return res;
  }

  void freeEditOp() {
    limitIdx++;
    limitIdx &= poolMask;
  }

  void advanceTxid() {
    txid += num_ops;
    num_ops = 0;
  }

  void dumpInfo();

  private:
  inline void initObjPool();

  /**
   * should call this after resetTailBlock
   */
  void allocNewExtent(uint32_t extentId) {
    nextExtentId = extentId;
    tail_block->pwrite<uint32_t>(kNumOpsIndex, ++num_ops);
    tail_block->putByte(3 /*opType*/);
    tail_block->write<int32_t>(4 /*opSize*/);
    tail_block->write<int32_t>(nextExtentId);
    extents_.push_front(extentId);
  }

  inline void resetTailBlock() {
    advanceTxid();
    tail_block->clear();
    tail_block->putByte(0); // flag to indicate if there has next block
    tail_block->write<uint32_t>(epoch);
    tail_block->write<uint64_t>(txid);
    tail_block->write<uint32_t>(0); // num_ops in current block
  }
};

#endif /* FAST_JOURNAL_H_ */
