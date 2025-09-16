/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "core/FastFS.h"
#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/bdev_zone.h"

static const char* bdevName = NULL;
static bool format = false;
static bool dump = false;
static bool checkpoint = false;
static int extentSize = 1048576; // 1M

static void tools_usage(void) {
  printf(" -b <bdev>                 name of the bdev to use\n");
  printf(" -f                        format FastFS\n");
  printf(" -S <size>                 extent size\n");
  printf(" -D                        dump FastFS info\n");
  printf(" -C                        checkpoint meta\n");
}

static int tools_parse_arg(int ch, char *arg) {
  switch (ch) {
  case 'b':
    bdevName = arg;
    break;
  case 'f':
    format = true;
    break;
  case 'S':
    extentSize = (int) std::stoi(arg);
    break;
  case 'D':
    dump = true;
    break;
  case 'C':
    checkpoint = true;
    break;
  default:
    return -EINVAL;
  }
  return 0;
}

static void operate_complete(FastFS* fastfs, int code) {
  if (code != 0) {
    SPDK_ERRLOG("operate failed: %d\n", code);
  } else {
    SPDK_NOTICELOG("operate successfuly.\n");
  }
  fs_context_t& fs_context = FastFS::fs_context;
  if (fs_context.bdev_io_channel) {
    spdk_put_io_channel(fs_context.bdev_io_channel);
  }
  if (fs_context.bdev_desc) {
    spdk_bdev_close(fs_context.bdev_desc);
  }
  spdk_app_stop(code);
}

static void tools_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
        void *event_ctx) {
  SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static void ckpt_complete(FastFS* fastfs, int code) {
//  if (code == 0) {
//    for (auto& inode : *fastfs->inodes) {
//      if (inode.type_ == FASTFS_DIR) {
//        printf("dentrys : %ld\n", inode.children_->size());
//        for (auto& childIno : *inode.children_) {
//          printf("  name : %s\n", (*fastfs->inodes)[childIno].name_.c_str());
//        }
//      }
//    }
//  }
  fastfs->unmount();
  spdk_app_stop(code);
}

static void mount_complete(FastFS* fastfs, int code) {
  if (code != 0) {
    SPDK_ERRLOG("mount fastfs failed: %d\n", code);
    return operate_complete(fastfs, code);
  }
  fastfs->checkpoint->checkpoint(ckpt_complete);
}

static void parseJournal(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  spdk_bdev_free_io(bdev_io);
  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  FastFS* fastfs = reinterpret_cast<FastFS*>(buffer->private_data);
  fs_context_t& ctx = FastFS::fs_context;
  uint32_t nextExtentId = 0;
  char flag = 1;
  uint32_t offset = 0;
  uint64_t txid = 0;
  uint32_t total_ops = 0;
  uint32_t create_ops = 0;
  uint32_t fsync_ops = 0;
  uint32_t truncate_ops = 0;
  uint32_t delete_ops = 0;
  uint32_t rename_ops = 0;
  uint32_t unkown_ops = 0;
  success = success && buffer->pread<uint64_t>(kTxidIndex, txid);
  if (success) {
    printf("txid %ld, ", txid);
  } else {
    printf("]\n");
    return operate_complete(fastfs, -2);
  }

  while (flag == 1 && offset < buffer->limit()) {
    buffer->position(offset).getByte(flag);
    if (flag != 0 && flag != 1) {
      break;
    }
    uint32_t epochNum = 0;
    if (!buffer->read<uint32_t>(epochNum) ||
        epochNum != ctx.superBlock.epoch) {
      break;
    }
    uint32_t num_ops = 0;
    if (!buffer->skip(8).read<uint32_t>(num_ops)) {
      break;
    }
    total_ops += num_ops;
    for (uint32_t i = 0; i < num_ops; i++) {
      char opType = -1;
      int32_t opSize = -1;
      buffer->getByte(opType);
      buffer->read<int32_t>(opSize);
      switch (opType) {
        case 0 : { // createOp
          create_ops++;
          break;
        }
        case 1 : { // truncateOp
          truncate_ops++;
          break;
        }
        case 2 : { // deleteOp
          delete_ops++;
          break;
        }
        case 3 : { // allocOp
          buffer->read<uint32_t>(nextExtentId);
          break;
        }
        case 4 : { // fsyncOp
          fsync_ops++;
          break;
        }
        case 5 : { // renameOp
          rename_ops++;
          break;
        }
        default: {
          unkown_ops++;
          break;
        }
      }
      if (opType != 3) { // allocOp
        buffer->skip(opSize);
      }
    }
    if (flag == 1) { // has next block
      offset += ctx.blockSize;
    }
  }
  printf("creates %d, truncates %d, fsyncs %d, deletes %d, renames %d, unkown %d, total %d]\n",
      create_ops, truncate_ops, fsync_ops, delete_ops, rename_ops, unkown_ops, total_ops);

  if (flag == 1 && nextExtentId > 0) {
    buffer->clear();
    SPDK_NOTICELOG("JOURNAL %d [", nextExtentId);
    uint64_t nextOffset = static_cast<uint64_t>(nextExtentId) << ctx.extentBits;
    spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, buffer->p_buffer_,
        nextOffset, ctx.extentSize, parseJournal, buffer);
    return;
  }
  return operate_complete(fastfs, 0);
}

static void parseDentry(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  spdk_bdev_free_io(bdev_io);
  ByteBuffer* extentBuf = reinterpret_cast<ByteBuffer*>(cb_arg);
  fs_context_t& ctx = FastFS::fs_context;
  uint32_t nextExtent = 0;
  uint32_t numOps = 0;
  uint32_t dentryOps = 0;
  uint32_t extentOps = 0;
  uint32_t unkownOps = 0;
  extentBuf->read<uint32_t>(nextExtent);
  extentBuf->read<uint32_t>(numOps);
  for (uint32_t i = 0; i < numOps; i++) {
    char opType = -1;
    extentBuf->getByte(opType);
    switch (opType) {
      case FASTFS_REGULAR_FILE : {
        extentOps++;
        extentBuf->skip(4); // inodeId
        uint32_t extentCount = 0;
        extentBuf->read<uint32_t>(extentCount);
        extentBuf->skip(extentCount * 4);
        break;
      }
      case FASTFS_DIR : {
        dentryOps++;
        extentBuf->skip(4); // inodeId
        uint32_t childCount = 0;
        extentBuf->read<uint32_t>(childCount);
        extentBuf->skip(childCount * 4/*childInode*/);
        break;
      }
      default: {
        unkownOps++;
        break;
      }
    }
  }
  printf("numOps %d, dentryOps %d, extentOps %d, unkownOps %d]\n",
      numOps, dentryOps, extentOps, unkownOps);

  if (nextExtent > 0) {
    SPDK_NOTICELOG("DENTRY %d [", nextExtent);
    extentBuf->clear();
    uint64_t offset = static_cast<uint64_t>(nextExtent) << ctx.extentBits;
    spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_,
        offset, ctx.extentSize, parseDentry, extentBuf);
    return;
  }

  uint64_t journalStart = ctx.superBlock.journalLoc;
  SPDK_NOTICELOG("JOURNAL %ld [", journalStart);
  extentBuf->clear();
  spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_,
      journalStart << ctx.extentBits, ctx.extentSize, parseJournal, extentBuf);
}

static void parseInodes(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  spdk_bdev_free_io(bdev_io);
  ByteBuffer* extentBuf = reinterpret_cast<ByteBuffer*>(cb_arg);
  fs_context_t& ctx = FastFS::fs_context;
  uint32_t nextExtent = 0;
  uint32_t numOps = 0;
  extentBuf->read<uint32_t>(nextExtent);
  extentBuf->read<uint32_t>(numOps);
  printf("numOps %d]\n", numOps);
  if (nextExtent > 0) {
    SPDK_NOTICELOG("INODES %d [", nextExtent);
    extentBuf->clear();
    uint64_t offset = static_cast<uint64_t>(nextExtent) << ctx.extentBits;
    spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_,
        offset, ctx.extentSize, parseInodes, extentBuf);
    return;
  }

  uint64_t ckptDentryLoc = ctx.superBlock.ckptDentryLoc;
  SPDK_NOTICELOG("DENTRY %ld [", ckptDentryLoc);
  extentBuf->clear();
  spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_,
      ckptDentryLoc << ctx.extentBits, ctx.extentSize, parseDentry, extentBuf);
}

static void readSuperBlockComplete(
    struct spdk_bdev_io* bdev_io, bool success, void *cb_arg) {
  spdk_bdev_free_io(bdev_io);
  ByteBuffer* buffer = reinterpret_cast<ByteBuffer*>(cb_arg);
  FastFS* fastfs = reinterpret_cast<FastFS*>(buffer->private_data);
  fs_context_t& ctx = FastFS::fs_context;
  success = success && ctx.superBlock.deserialize(buffer);
  spdk_dma_free(buffer->p_buffer_);
  delete buffer;
  if (success) {
    ctx.extentSize = ctx.superBlock.extentSize;
    ctx.extentBits = spdk_u32log2(ctx.extentSize);
    ctx.extentMask = ctx.extentSize - 1;
    uint64_t journalStart = ctx.superBlock.journalLoc;
    uint32_t skipBlocks = ctx.superBlock.journalSkipBlocks;
    uint32_t skipOps = ctx.superBlock.journalSkipOps;
    uint64_t ckptInodesLoc = ctx.superBlock.ckptInodesLoc;
    uint64_t ckptDentryLoc = ctx.superBlock.ckptDentryLoc;
    uint64_t lastTxid = ctx.superBlock.lastTxid;
    SPDK_NOTICELOG("SuperBlock [epoch %d, extentSize %d, journal_loc %ld, "
        "skip_blocks %d, skip_ops %d, ckpt_inodes_loc %ld, ckpt_dentry_loc %ld, "
        "last_txid %ld]\n",
        ctx.superBlock.epoch, ctx.extentSize, journalStart, skipBlocks, skipOps,
        ckptInodesLoc, ckptDentryLoc, lastTxid);

    uint32_t buf_align = spdk_bdev_get_buf_align(ctx.bdev);
    char* addr = (char*) spdk_dma_zmalloc(ctx.extentSize, buf_align, NULL);
    ByteBuffer* extentBuf = new ByteBuffer(addr, ctx.extentSize);
    extentBuf->private_data = fastfs;

    if (ckptInodesLoc > 0) {
      SPDK_NOTICELOG("INODES %ld [", ckptInodesLoc);
      spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_,
          ckptInodesLoc << ctx.extentBits, ctx.extentSize, parseInodes, extentBuf);
    } else {
      SPDK_NOTICELOG("JOURNAL %ld [", journalStart);
      spdk_bdev_read(ctx.bdev_desc, ctx.bdev_io_channel, extentBuf->p_buffer_,
          journalStart << ctx.extentBits, ctx.extentSize, parseJournal, extentBuf);
    }
  } else {
    SPDK_WARNLOG("can't read super block, FastFS not format?\n");
    operate_complete(fastfs, -1);
  }
}

static void dumpFS(FastFS* fastfs) {
  auto& fs_context = FastFS::fs_context;
  uint32_t ioUnitSize = spdk_bdev_get_write_unit_size(fs_context.bdev);
  uint32_t buf_align = spdk_bdev_get_buf_align(fs_context.bdev);
  fs_context.blocks = spdk_bdev_get_num_blocks(fs_context.bdev) / ioUnitSize;
  fs_context.blockSize = spdk_bdev_get_block_size(fs_context.bdev) * ioUnitSize;
  fs_context.blockBits = spdk_u32log2(fs_context.blockSize);
  fs_context.blockMask = (1 << fs_context.blockBits) - 1;
  char* addr = (char*) spdk_dma_zmalloc(fs_context.blockSize, buf_align, NULL);
  ByteBuffer* buffer = new ByteBuffer(addr, fs_context.blockSize);
  buffer->private_data = fastfs;
  // read super block
  spdk_bdev_read(fs_context.bdev_desc, fs_context.bdev_io_channel,
      buffer->p_buffer_, 0, fs_context.blockSize, readSuperBlockComplete, buffer);

}

static void tools_start(void *arg) {
  FastFS* fastfs = (FastFS*) arg;
  fs_context_t* fs_context = &FastFS::fs_context;
  fs_context->fastfs = fastfs;

  int rc = 0;
  fs_context->bdev = NULL;
  fs_context->bdev_desc = NULL;

  SPDK_NOTICELOG("Opening the bdev %s\n", fs_context->bdev_name);
  rc = spdk_bdev_open_ext(fs_context->bdev_name, true, tools_event_cb, NULL,
        &fs_context->bdev_desc);
  if (rc) {
    SPDK_ERRLOG("Could not open bdev: %s\n", fs_context->bdev_name);
    spdk_app_stop(-1);
    return;
  }
  fs_context->bdev = spdk_bdev_desc_get_bdev(fs_context->bdev_desc);

  SPDK_NOTICELOG("Opening io channel\n");
  fs_context->bdev_io_channel = spdk_bdev_get_io_channel(fs_context->bdev_desc);
  if (fs_context->bdev_io_channel == NULL) {
    SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
    spdk_bdev_close(fs_context->bdev_desc);
    spdk_app_stop(-1);
    return;
  }
  if (format) {
    fastfs->format(extentSize, operate_complete);
  } else if (dump) {
    dumpFS(fastfs);
  } else if (checkpoint) {
    fastfs->mount(mount_complete);
  } else {
    operate_complete(fastfs, 0);
  }
}

int main(int argc, char **argv) {
  struct spdk_app_opts opts = {};
  spdk_app_opts_init(&opts, sizeof(opts));
  opts.name = "fastfs_tools";
  opts.rpc_addr = NULL;

  int rc = spdk_app_parse_args(
      argc, argv, &opts, "b:fS:DC", NULL, tools_parse_arg, tools_usage);
  if (rc != SPDK_APP_PARSE_ARGS_SUCCESS) {
    exit(rc);
  }

  if (!bdevName) {
    SPDK_ERRLOG("ERROR should specify bdevName with -b\n");
  } else {
    FastFS fast_fs(bdevName);
    rc = spdk_app_start(&opts, tools_start, &fast_fs);
    if (rc) {
      SPDK_ERRLOG("ERROR Start FastFS Tools\n");
    }
    spdk_app_fini();
  }
  return rc;
}
