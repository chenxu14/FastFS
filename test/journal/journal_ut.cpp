/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

extern "C" {
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
}
#include "core/FastFS.h"

size_t spdk_bdev_get_buf_align(const struct spdk_bdev*) {
  return 1;
}
void* spdk_mempool_get(struct spdk_mempool*) {
  return malloc(sizeof(FastInode));
}
void spdk_mempool_put(struct spdk_mempool*, void *ele) {
  free(ele);
}
void *spdk_malloc(size_t size, size_t, uint64_t*, int, uint32_t) {
  return malloc(size);
}
void spdk_free(void *buf) {
  free(buf);
}
void* spdk_realloc(void *buf, size_t size, size_t) {
  return realloc(buf, size);
}
void* spdk_dma_zmalloc_socket(size_t size, size_t, uint64_t*, int) {
  return malloc(size);
}
void spdk_dma_free(void *buf) {
  free(buf);
}
struct spdk_poller* spdk_poller_register_named(
    spdk_poller_fn, void*, uint64_t, const char*) {
  return nullptr;
}
int spdk_bdev_write(struct spdk_bdev_desc*, struct spdk_io_channel*,
    void*, uint64_t, uint64_t, spdk_bdev_io_completion_cb, void *cb_arg) {
  FastJournal* journal = reinterpret_cast<FastJournal*>(cb_arg);
  journal->writeComplete(0);
  return 0;
}

static void init_fs_context(fs_context_t& ctx,
    uint32_t blockSize, uint32_t extentSize, uint32_t blocks) {
  ctx.blockSize = blockSize;
  ctx.extentSize = extentSize;
  ctx.bufAlign = 1;
  ctx.allocator = new BlockAllocator(blocks, blockSize, extentSize);
  ctx.superBlock.epoch = 0;
}

static void recordCreate(ByteBuffer* buffer, EditOp&, uint32_t& num_ops,
    FileType type, uint32_t pid, const std::string& name) {
  CreateContext createCtx;
  createCtx.parentId = pid;
  createCtx.name = name;
  createCtx.type = type;
  createCtx.mode = 493; // 755
  buffer->pwrite<uint32_t>(kNumOpsIndex, ++num_ops);
  buffer->putByte(0); // type
  buffer->write<int32_t>(10 + createCtx.name.size());
  createCtx.serialize(buffer);
}

static void recordTruncate(ByteBuffer* buffer, EditOp&,
    uint32_t& num_ops, uint32_t ino, uint64_t file_size) {
  TruncateContext truncateCtx;
  truncateCtx.ino = ino;
  truncateCtx.size = file_size;
  buffer->pwrite<uint32_t>(kNumOpsIndex, ++num_ops);
  buffer->putByte(1);
  buffer->write<int32_t>(12);
  truncateCtx.serialize(buffer);
}

static void recordDelete(ByteBuffer* buffer, EditOp&,
    uint32_t& num_ops, uint32_t pid, const std::string& name) {
  DeleteContext deleteCtx;
  deleteCtx.parentId = pid;
  deleteCtx.name = name;
  deleteCtx.recursive = false;
  buffer->pwrite<uint32_t>(kNumOpsIndex, ++num_ops);
  buffer->putByte(2);
  buffer->write<int32_t>(6 + deleteCtx.name.size());
  deleteCtx.serialize(buffer);
}

static void recordRename(ByteBuffer* buffer, EditOp&, uint32_t& num_ops,
    uint32_t olddir, uint32_t newdir,
    const std::string& oldname, const std::string& newname) {
  RenameContext renameCtx;
  renameCtx.olddir = olddir;
  renameCtx.newdir = newdir;
  renameCtx.oldname = oldname;
  renameCtx.newname = newname;
  buffer->pwrite<uint32_t>(kNumOpsIndex, ++num_ops);
  buffer->putByte(5); // type
  buffer->write<int32_t>(10 + oldname.size() + newname.size());
  renameCtx.serialize(buffer);
}

static void test_log_replay(void) {
  fs_context_t fs_context;
  init_fs_context(fs_context, 4096, 8192, 10);
  FastJournal journal(fs_context);
  auto buffer = journal.tail_block;

  EditOp editOp;
  uint32_t num_ops = 0;
  recordCreate(buffer, editOp, num_ops, FASTFS_DIR, 0, "dir_1");
  recordCreate(buffer, editOp, num_ops, FASTFS_REGULAR_FILE, 0, "file_1");
  recordTruncate(buffer, editOp, num_ops, 2, 1024);
  recordDelete(buffer, editOp, num_ops, 0, "dir_1");
  recordRename(buffer, editOp, num_ops, 10, 20, "oldname", "newname");

  CreateContext createCtx;
  TruncateContext truncateCtx;
  DeleteContext deleteCtx;
  RenameContext renameCtx;

  char flag;
  bool res = buffer->flip().getByte(flag);
  CU_ASSERT(res && flag == 0);

  uint32_t epoch;
  res = buffer->read<uint32_t>(epoch);
  CU_ASSERT(res && epoch == fs_context.superBlock.epoch);

  uint64_t txid;
  res = buffer->read<uint64_t>(txid);
  CU_ASSERT(res && txid == 0);

  res = buffer->read<uint32_t>(num_ops);
  CU_ASSERT(num_ops == 5);
  buffer->getByte(editOp.type);
  CU_ASSERT(editOp.type == 0);

  int32_t size = 0;
  buffer->read<int32_t>(size);
  createCtx.deserialize(buffer);
  CU_ASSERT(createCtx.name == "dir_1");

  buffer->getByte(editOp.type);
  CU_ASSERT(editOp.type == 0);
  buffer->read<int32_t>(size);
  createCtx.deserialize(buffer);
  CU_ASSERT(createCtx.name == "file_1");

  buffer->getByte(editOp.type);
  CU_ASSERT(editOp.type == 1);
  buffer->read<int32_t>(size);
  truncateCtx.deserialize(buffer);
  CU_ASSERT(truncateCtx.size == 1024);

  buffer->getByte(editOp.type);
  CU_ASSERT(editOp.type == 2);
  buffer->read<int32_t>(size);
  deleteCtx.deserialize(buffer);
  CU_ASSERT(deleteCtx.name == "dir_1");

  buffer->getByte(editOp.type);
  CU_ASSERT(editOp.type == 5);
  buffer->read<int32_t>(size);
  renameCtx.deserialize(buffer);
  CU_ASSERT(renameCtx.newname == "newname");
}

static void test_block_has_padding(void) {
  fs_context_t fs_context;
  init_fs_context(fs_context, 4096, 8192, 10);
  FastJournal journal(fs_context);
  auto buffer = journal.tail_block;
  EditOp* editOp = journal.allocEditOp();
  CU_ASSERT_PTR_NOT_NULL(editOp);
  uint32_t num_ops = 0;
  recordCreate(buffer, *editOp, num_ops, FASTFS_DIR, 0, "dir");
  recordTruncate(buffer, *editOp, num_ops, 2, 1024);
  recordDelete(buffer, *editOp, num_ops, 0, "dir");
  CU_ASSERT(buffer->remaining() > 0);

  buffer->flip().skip(kNumOpsIndex).read<uint32_t>(num_ops);
  CU_ASSERT(num_ops == 3);
  int32_t size = 0;
  CreateContext createCtx;
  TruncateContext truncateCtx;
  DeleteContext deleteCtx;
  for (uint32_t i = 0; i < num_ops; i++) {
    buffer->getByte(editOp->type);
    buffer->read<int32_t>(size);
    if (editOp->type == 0) {
      CU_ASSERT(i == 0);
      createCtx.deserialize(buffer);
      CU_ASSERT(createCtx.name == "dir");
    } else if (editOp->type == 1) {
      CU_ASSERT(i == 1);
      truncateCtx.deserialize(buffer);
      CU_ASSERT(truncateCtx.size == 1024);
    } else if (editOp->type == 2) {
      CU_ASSERT(i == 2);
      deleteCtx.deserialize(buffer);
      CU_ASSERT(deleteCtx.name == "dir");
    }
  }
}

static void test_editop_alloc(void) {
  fs_context_t fs_context;
  init_fs_context(fs_context, 4096, 8192, 10);
  FastJournal journal(fs_context);
  EditOp* editOp = nullptr;
  uint32_t num = DEFAULT_POOL_SIZE - 1;
  for (uint32_t i = 0; i < num; i++) {
    editOp = journal.allocEditOp();
    if (!editOp) {
      break;
    }
  }
  CU_ASSERT_PTR_NOT_NULL(editOp);

  editOp = journal.allocEditOp();
  CU_ASSERT_PTR_NULL(editOp);
  journal.freeEditOp();
  editOp = journal.allocEditOp();
  CU_ASSERT_PTR_NOT_NULL(editOp);

  journal.freeEditOp();
  for (uint32_t i = 0; i < num * 2; i++) {
    editOp = journal.allocEditOp();
    if (!editOp) {
      break;
    }
    journal.freeEditOp();
  }
  CU_ASSERT_PTR_NOT_NULL(editOp);
}

static void write_complete(void* cb_args, int) {
  FastJournal* journal = reinterpret_cast<FastJournal*>(cb_args);
  journal->freeEditOp();
}

static void test_op_poller(void) {
  fs_context_t fs_context;
  init_fs_context(fs_context, 4096, 8192, 10);
  uint32_t freeBefore = fs_context.allocator->getFree();
  FastJournal journal(fs_context);

  bool allocated = true;
  uint32_t num = DEFAULT_POOL_SIZE - 1;
  for (uint32_t i = 0; i < num; i++) {
    EditOp* editOp = journal.allocEditOp();
    if (!editOp) {
      allocated = false;
      break;
    }
    DeleteContext* deleteCtx = new DeleteContext();
    deleteCtx->parentId = 0;
    deleteCtx->name = "dir";
    deleteCtx->recursive = false;
    editOp->opctx = deleteCtx;
    editOp->type = 2;
    editOp->size = 6 + deleteCtx->name.size();
    editOp->callback = write_complete;
    editOp->cb_args = &journal;
    editOp->phrase = !editOp->phrase;
  }
  CU_ASSERT(allocated);

  int status = journal.pollEditOp();
  while (status != SPDK_POLLER_IDLE) {
    status = journal.pollEditOp();
  }
  // phrase bit revert case
  for (uint32_t i = 0; i < num; i++) {
    EditOp* editOp = journal.allocEditOp();
    if (!editOp) {
      allocated = false;
      break;
    }
    TruncateContext* truncateCtx = new TruncateContext();
    truncateCtx->ino = i;
    truncateCtx->size = 1024;
    editOp->opctx = truncateCtx;
    editOp->type = 1;
    editOp->size = 12;
    editOp->callback = write_complete;
    editOp->cb_args = &journal;
    editOp->phrase = !editOp->phrase;
  }
  CU_ASSERT(allocated);

  uint32_t freeAfter = fs_context.allocator->getFree();
  CU_ASSERT(freeAfter <= freeBefore);
}

static void test_no_space(void) {
  uint32_t blockSize = 4096;
  fs_context_t fs_context;
  init_fs_context(fs_context, blockSize, 8192, 4/*two extents*/);
  FastJournal journal(fs_context);
  auto buffer = journal.tail_block;

  uint32_t extentId = fs_context.allocator->allocate();
  CU_ASSERT(extentId != UINT32_MAX);
  // first extent is reserved
  CU_ASSERT(fs_context.allocator->getFree() == 0);

  EditOp* editOp = journal.allocEditOp();
  DeleteContext* deleteCtx = new DeleteContext();
  deleteCtx->parentId = 0;
  deleteCtx->name = "file";
  deleteCtx->recursive = false;
  editOp->opctx = deleteCtx;
  editOp->type = 2;
  editOp->size = 6 + deleteCtx->name.size();
  editOp->callback = write_complete;
  editOp->cb_args = &journal;
  editOp->phrase = !editOp->phrase;

  // make tail block full
  buffer->skip(blockSize - 1);
  int count = 0;
  for (int i = 0; i < 10; i++) {
    count = journal.pollEditOp();
    if (count > 0) {
      break;
    }
  }
  CU_ASSERT(count == 0);

  // free extent to make space
  fs_context.allocator->release(extentId);
  CU_ASSERT(fs_context.allocator->getFree() > 0);
  count = journal.pollEditOp();
  CU_ASSERT(count > 0);
}

static void test_release_extents(void) {
  std::forward_list<uint32_t> extents;
  for (uint32_t i = 0; i < 10; i++) {
    extents.push_front(i);
  }
  // start checkpoint, cusor point to '9'
  std::forward_list<uint32_t>::const_iterator cusor = extents.cbegin();
  for (uint32_t i = 10; i < 20; i++) {
    extents.push_front(i);
  }
  // release old extents [0 ~ 9)
  extents.erase_after(cusor, extents.end());
  uint32_t target = 19;
  bool correct = true;
  for (auto& extentId : extents) {
    if (extentId != target--) {
      correct = false;
    }
  }
  CU_ASSERT(correct);
}

int main() {
  if (CUE_SUCCESS != CU_initialize_registry()) {
    return CU_get_error();
  }

  CU_pSuite suite = CU_add_suite("Journal Tests", NULL, NULL);
  if (suite == NULL) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  if (
      CU_add_test(suite, "Poller", test_op_poller) == NULL ||
      CU_add_test(suite, "log replay", test_log_replay) == NULL ||
      CU_add_test(suite, "EditOp alloc", test_editop_alloc) == NULL ||
      CU_add_test(suite, "no space left", test_no_space) == NULL ||
      CU_add_test(suite, "release extents", test_release_extents) == NULL ||
      CU_add_test(suite, "block has padding", test_block_has_padding) == NULL
  ) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  CU_basic_set_mode(CU_BRM_VERBOSE);
  CU_basic_run_tests();

  CU_cleanup_registry();
  return CU_get_number_of_failures();
}
