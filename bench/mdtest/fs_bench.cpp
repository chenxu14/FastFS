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

static const char* bdevName = "Malloc0";
static bool verify = false;
static bool format = true;
static bool direct = false;
static bool readWrite = false;
static int extentSize = 8192;
static int parallelism = 1;
static int subdirs = 1024;
static int files_per_dir = 64;

struct fs_bench_context {
  struct spdk_poller* poller;
  struct timespec time_start;
  std::string path;
  int subdir_index;
  int subfile_index;
  int stage; /* mkdir(0), create(1), stats(2), write(3), read(4), remove(5)*/
  int inflights = 0;
  uint32_t size = 4096;
  char data[4096];
  bool wait = false;
  fs_bench_context() {
    for (uint32_t i = 0; i < size; i++) {
      data[i] = i;
    }
  }
};
static fs_bench_context ctx;

static void print_time_cost(int stage) {
  struct timespec time_end;
  struct timespec d;
  clock_gettime(CLOCK_MONOTONIC, &time_end);
  if ((time_end.tv_nsec - ctx.time_start.tv_nsec) < 0) {
    d.tv_sec = time_end.tv_sec - ctx.time_start.tv_sec - 1;
    d.tv_nsec = 1000000000 + time_end.tv_nsec - ctx.time_start.tv_nsec;
  } else {
    d.tv_sec = time_end.tv_sec - ctx.time_start.tv_sec;
    d.tv_nsec = time_end.tv_nsec - ctx.time_start.tv_nsec;
  }
  std::string msg;
  long counts = 0;
  switch (stage) {
  case 0 :
    msg = "[MKDIR] create %ld dirs use %ld ms, ops is %s.\n";
    counts = subdirs;
    break;
  case 1 :
    msg = "[Create] create %ld files use %ld ms, ops is %s.\n";
    counts = subdirs * files_per_dir;
    break;
  case 2 :
    msg = "[Stats] stats %ld files use %ld ms, ops is %s.\n";
    counts = subdirs * files_per_dir;
    break;
  case 3 :
    msg = "[Write] write %ld files use %ld ms, ops is %s.\n";
    counts = subdirs * files_per_dir;
    break;
  case 4 :
    msg = "[Read] read %ld files use %ld ms, ops is %s.\n";
    counts = subdirs * files_per_dir;
    break;
  case 5 :
    msg = "[Remove] delete %ld files use %ld ms, ops is %s.\n";
    counts = subdirs * files_per_dir;
    break;
  }
  printf(msg.c_str(), counts, (long)(d.tv_sec * 1000 + d.tv_nsec / 1000000.0),
      std::to_string(counts / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)).c_str());
}

static void fsbench_usage(void) {
  printf(" -b <bdev>                 name of the bdev to use\n");
  printf(" -V                        do verify\n");
  printf(" -M                        don't format FastFS\n");
  printf(" -S <size>                 extent size\n");
  printf(" -P <num>                  parallelism\n");
  printf(" -D                        don't use direct IO\n");
  printf(" -N                        number of dirs\n");
  printf(" -F                        number of files per dir\n");
  printf(" -w                        Whether do read and write\n");
}

static int fsbench_parse_arg(int ch, char *arg) {
  switch (ch) {
  case 'b':
    bdevName = arg;
    break;
  case 'V':
    verify = true;
    break;
  case 'M':
    format = false;
    break;
  case 'S':
    extentSize = (int) std::stoi(arg);
    break;
  case 'P':
    parallelism = (int) std::stoi(arg);
    break;
  case 'D':
    direct = true;
    break;
  case 'N':
    subdirs = (int) std::stoi(arg);
    break;
  case 'F':
    files_per_dir = (int) std::stoi(arg);
    break;
  case 'w':
    readWrite = true;
    break;
  default:
    return -EINVAL;
  }
  return 0;
}

static void delete_complete(void* cb_args, int code) {
  spdk_poller_unregister(&ctx.poller);
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->freeFsOp(opCtx);
  fastfs->unmount();
  spdk_app_stop(code);
}

static void delete_file_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    SPDK_ERRLOG("delete file failed: %d\n", code);
    spdk_poller_unregister(&ctx.poller);
    fastfs->unmount();
    spdk_app_stop(-1);
  }
  ctx.inflights--;
}

static void delete_file(FastFS* fastfs) {
  ctx.path = "/mdtest/mdtest_tree." + std::to_string(ctx.subdir_index);
  FastInode* parent = fastfs->status(ctx.path);
  ctx.path = "file." + std::to_string(ctx.subfile_index);
  fs_op_context* opCtx = fastfs->allocFsOp();
  DeleteContext* delCtx = new (opCtx->private_data) DeleteContext();
  delCtx->parentId = parent->ino_;
  delCtx->name = ctx.path.c_str();
  opCtx->callback = delete_file_complete;
  opCtx->cb_args = opCtx;
  fastfs->remove(*opCtx);
  ctx.subfile_index++;
}

static void read_file_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  ReadContext* readCtx = reinterpret_cast<ReadContext*>(opCtx->private_data);
  FastFS* fastfs = opCtx->fastfs;
  if (verify) {
    if (direct) {
      readCtx->direct_buff->position(readCtx->direct_cursor);
      readCtx->read_buff = readCtx->direct_buff->getBuffer();
    }
    code = memcmp(readCtx->read_buff, ctx.data, ctx.size);
  }
  if (direct) {
    fastfs->freeBuffer(readCtx->direct_buff);
  } else {
    delete readCtx->read_buff;
  }
  fastfs->close(readCtx->fd);
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    SPDK_ERRLOG("read file failed: %d\n", code);
    spdk_poller_unregister(&ctx.poller);
    fastfs->unmount();
    spdk_app_stop(-1);
  }
  ctx.inflights--;
}

static void read_file(FastFS* fastfs) {
  ctx.path = "/mdtest/mdtest_tree." + std::to_string(ctx.subdir_index)
      + "/file." + std::to_string(ctx.subfile_index);
  int fd = fastfs->open(ctx.path, O_RDONLY);
  if (readWrite) {
    fs_op_context* opCtx = fastfs->allocFsOp();
    ReadContext* readCtx = new (opCtx->private_data) ReadContext();
    if (direct) {
      readCtx->dirctRead(fastfs, fd, 0, ctx.size);
    } else {
      readCtx->fd = fd;
      readCtx->pread = true;
      readCtx->offset = 0;
      readCtx->count = ctx.size;
      readCtx->read_buff = new char[ctx.size];
    }
    opCtx->callback = read_file_complete;
    opCtx->cb_args = opCtx;
    fastfs->read(*opCtx);
  } else {
    fastfs->close(fd);
    ctx.inflights--;
  }
  ctx.subfile_index++;
}

static void write_file_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(opCtx->private_data);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->close(writeCtx->fd);
  if (direct) {
    fastfs->freeBuffer(writeCtx->direct_buff);
  }
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    SPDK_ERRLOG("write file failed: %d\n", code);
    spdk_poller_unregister(&ctx.poller);
    fastfs->unmount();
    spdk_app_stop(-1);
  }
  ctx.inflights--;
}

static void write_file(FastFS* fastfs) {
  ctx.path = "/mdtest/mdtest_tree." + std::to_string(ctx.subdir_index)
      + "/file." + std::to_string(ctx.subfile_index);
  int fd = fastfs->open(ctx.path, O_RDWR); // F_MULTI_WRITE
  if (readWrite) {
    fs_op_context* opCtx = fastfs->allocFsOp();
    WriteContext* writeCtx = new (opCtx->private_data) WriteContext();
    if (direct) {
      writeCtx->dirctWrite(fastfs, fd, 0, ctx.size, ctx.data);
    } else {
      writeCtx->fd = fd;
      writeCtx->pwrite = false;
      writeCtx->write_buff = ctx.data;
      writeCtx->count = ctx.size;
    }
    opCtx->callback = write_file_complete;
    opCtx->cb_args = opCtx;
    fastfs->write(*opCtx);
  } else {
    fastfs->close(fd);
    ctx.inflights--;
  }
  ctx.subfile_index++;
}

static void stat_file(FastFS* fastfs) {
  ctx.path = "/mdtest/mdtest_tree." + std::to_string(ctx.subdir_index)
      + "/file." + std::to_string(ctx.subfile_index);
  FastInode* inode = fastfs->status(ctx.path);
  if (!inode) {
    SPDK_ERRLOG("stat file failed: %s\n", ctx.path.c_str());
    spdk_poller_unregister(&ctx.poller);
    fastfs->unmount();
    spdk_app_stop(-1);
  }
  ctx.inflights--;
  ctx.subfile_index++;
}

static void create_file_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    SPDK_ERRLOG("create file failed: %d\n", code);
    spdk_poller_unregister(&ctx.poller);
    fastfs->unmount();
    spdk_app_stop(-1);
  }
  ctx.inflights--;
}

static void create_file(FastFS* fastfs) {
  ctx.path = "/mdtest/mdtest_tree." + std::to_string(ctx.subdir_index);
  FastInode* parent = fastfs->status(ctx.path);
  ctx.path = "file." + std::to_string(ctx.subfile_index);
  fs_op_context* opCtx = fastfs->allocFsOp();
  CreateContext* createCtx = new (opCtx->private_data) CreateContext();
  createCtx->parentId = parent->ino_;
  createCtx->name = ctx.path.c_str();
  createCtx->mode = 493;
  createCtx->type = FASTFS_REGULAR_FILE;
  opCtx->callback = create_file_complete;
  opCtx->cb_args = opCtx;
  fastfs->create(*opCtx);
  ctx.subfile_index++;
}

static void create_subdir_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    SPDK_ERRLOG("create sub dir failed: %d\n", code);
    spdk_poller_unregister(&ctx.poller);
    fastfs->unmount();
    spdk_app_stop(-1);
  }
  ctx.inflights--;
}

static void create_subdir(FastFS* fastfs) {
  ctx.path = "/mdtest";
  FastInode* parent = fastfs->status(ctx.path);
  ctx.path = "mdtest_tree." + std::to_string(ctx.subdir_index);
  fs_op_context* opCtx = fastfs->allocFsOp();
  CreateContext* createCtx = new (opCtx->private_data) CreateContext();
  createCtx->parentId = parent->ino_;
  createCtx->name = ctx.path.c_str();
  createCtx->mode = 493;
  createCtx->type = FASTFS_DIR;
  opCtx->callback = create_subdir_complete;
  opCtx->cb_args = opCtx;
  fastfs->create(*opCtx);
  ctx.subdir_index++;
}

static bool advanceProgress() {
  if (ctx.subfile_index == files_per_dir) {
    if (++ctx.subdir_index == subdirs) {
      ctx.wait = true;
      return false;
    }
    ctx.subfile_index = 0;
  }
  ctx.inflights++;
  return true;
}

/**
 * Use poller to avoid stack too large
 * because Malloc bdev's read is a sync call
 */
static int do_bench(void *arg) {
  if (ctx.wait) {
    if (ctx.inflights > 0) {
      // wait for previous stage finish
      return SPDK_POLLER_IDLE;
    } else {
      print_time_cost(ctx.stage);
      ctx.wait = false;
      ctx.stage++;
      ctx.subfile_index = 0;
      ctx.subdir_index = 0;
      clock_gettime(CLOCK_MONOTONIC, &ctx.time_start);
    }
  }
  if (ctx.inflights >= parallelism) {
    return SPDK_POLLER_IDLE;
  }
  FastFS* fastfs = reinterpret_cast<FastFS*>(arg);
  switch (ctx.stage) {
    case 0 : { // mkdir
      if (ctx.subdir_index == subdirs) {
        ctx.wait = true;
        return SPDK_POLLER_IDLE;
      }
      ctx.inflights++;
      create_subdir(fastfs);
      return SPDK_POLLER_BUSY;
    }
    case 1 : { // create
      if (!advanceProgress()) {
        return SPDK_POLLER_IDLE;
      }
      create_file(fastfs);
      return SPDK_POLLER_BUSY;
    }
    case 2 : { // stats
      if (!advanceProgress()) {
        return SPDK_POLLER_IDLE;
      }
      stat_file(fastfs);
      return SPDK_POLLER_BUSY;
    }
    case 3 : { // write
      if (!advanceProgress()) {
        return SPDK_POLLER_IDLE;
      }
      write_file(fastfs);
      return SPDK_POLLER_BUSY;
    }
    case 4 : { // read
      if (!advanceProgress()) {
        return SPDK_POLLER_IDLE;
      }
      read_file(fastfs);
      return SPDK_POLLER_BUSY;
    }
    case 5 : { // read
      if (!advanceProgress()) {
        return SPDK_POLLER_IDLE;
      }
      delete_file(fastfs);
      return SPDK_POLLER_BUSY;
    }
    case 6 : { // remove test dir
      ctx.path = "mdtest";
      fs_op_context* opCtx = fastfs->allocFsOp();
      DeleteContext* delCtx = new (opCtx->private_data) DeleteContext();
      delCtx->parentId = 0;
      delCtx->name = ctx.path.c_str();
      delCtx->recursive = true;
      opCtx->callback = delete_complete;
      opCtx->cb_args = opCtx;
      fastfs->remove(*opCtx);
      ctx.stage++;
      return SPDK_POLLER_BUSY;
    }
  }
  return SPDK_POLLER_IDLE;
}

static void create_dir_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    SPDK_ERRLOG("create test dir failed: %d\n", code);
    fastfs->unmount();
    spdk_app_stop(-1);
    return;
  }

  // prepare for mkdir
  ctx.stage = 0;
  ctx.subdir_index = 0;
  ctx.subfile_index = 0;
  clock_gettime(CLOCK_MONOTONIC, &ctx.time_start);
  ctx.poller = SPDK_POLLER_REGISTER(do_bench, fastfs, 0);
}


static void mount_complete(FastFS* fastfs, int code) {
  if (code != 0) {
    SPDK_ERRLOG("mount fastfs failed: %d\n", code);
    return;
  }
  ctx.path = "mdtest";
  fs_op_context* opCtx = fastfs->allocFsOp();
  CreateContext* createCtx = new (opCtx->private_data) CreateContext();
  createCtx->parentId = 0; // root
  createCtx->name = ctx.path.c_str();
  createCtx->mode = 493;
  createCtx->type = FASTFS_DIR;
  opCtx->callback = create_dir_complete;
  opCtx->cb_args = opCtx;
  SPDK_NOTICELOG("mount fastfs successfuly, create test dir now.\n");
  fastfs->create(*opCtx);
}

static void format_complete(FastFS* fastfs, int code) {
  if (code != 0) {
    SPDK_ERRLOG("format fastfs failed: %d\n", code);
    return;
  }
  SPDK_NOTICELOG("format fastfs successfuly, do mount now.\n");
  fastfs->mount(mount_complete);
}

static void fsbench_event_cb(
    enum spdk_bdev_event_type type, struct spdk_bdev*, void*) {
  SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static void fsbench_start(void *arg) {
  SPDK_NOTICELOG("Successfully started the application\n");
  FastFS* fastfs = (FastFS*) arg;
  fs_context_t* fs_context = &FastFS::fs_context;

  int rc = 0;
  fs_context->bdev = NULL;
  fs_context->bdev_desc = NULL;

  SPDK_NOTICELOG("Opening the bdev %s\n", fs_context->bdev_name);
  rc = spdk_bdev_open_ext(fs_context->bdev_name, true, fsbench_event_cb, NULL,
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
    fastfs->format(extentSize, format_complete, false);
  } else {
    fastfs->mount(mount_complete);
  }
}

int main(int argc, char **argv) {
  struct spdk_app_opts opts = {};
  spdk_app_opts_init(&opts, sizeof(opts));
  int rc = spdk_app_parse_args(
        argc, argv, &opts, "b:VMS:P:DN:F:w", NULL, fsbench_parse_arg, fsbench_usage);
  if (rc != SPDK_APP_PARSE_ARGS_SUCCESS) {
    exit(rc);
  }
  opts.name = "fs_bench";
  opts.rpc_addr = NULL;
  opts.mem_size = 1024;
  opts.hugepage_single_segments = 1;

  FastFS fast_fs(bdevName);
  rc = spdk_app_start(&opts, fsbench_start, &fast_fs);
  if (rc) {
    SPDK_ERRLOG("ERROR running application\n");
  }
  spdk_app_fini();
  return rc;
}
