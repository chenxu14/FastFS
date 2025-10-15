/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "core/FastFS.h"
#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/bdev_zone.h"
#include "spdk/accel.h"
#include "spdk/env.h"
#include "spdk/file.h"
#include "spdk/init.h"
#include "spdk/thread.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/queue.h"
#include "spdk/util.h"
#include "spdk/rpc.h"
#include "spdk/event.h"
#include "fio.h"

extern "C" {
#include "optgroup.h"
void get_ioengine(struct ioengine_ops **ioengine_ptr);
}

static const char* g_config_file = nullptr;
static void *g_json_data;
static size_t g_config_file_size;
static const char* g_bdev_name = nullptr;
static bool g_do_format = true;
static const char* g_test_file = "/test.txt";
static uint64_t g_test_file_size = 0;

struct fastfs_fio_options {
  int __pad;
  char* conf = NULL;
  char* bdev = NULL;
  char* file = NULL;
  char* format = NULL;
};

// fio-3.39
static struct fio_option options[] = {
    {
        "spdk_conf",
        "SPDK configuration file",
        NULL,
        FIO_OPT_STR_STORE,
        offsetof(struct fastfs_fio_options, conf),
    },
    {
        "spdk_bdev",
        "SPDK bdev name",
        NULL,
        FIO_OPT_STR_STORE,
        offsetof(struct fastfs_fio_options, bdev),
    },
    {
        "test_file",
        "target test file",
        NULL,
        FIO_OPT_STR_STORE,
        offsetof(struct fastfs_fio_options, file),
    },
    {
        "do_format",
        "format fastfs before mount",
        NULL,
        FIO_OPT_STR_STORE,
        offsetof(struct fastfs_fio_options, format),
    },
    {
        NULL, // end flag
    },
};

struct fastfs_fio_thread {
  FastFS* fastfs = nullptr;
  ByteBuffer* buff = nullptr;
  struct thread_data* td;
  struct spdk_thread* thread;
  struct io_u** iocq;
  ByteBuffer** buffers;
  int reqs = 0;
  int count = 0;
  bool writing = false;
};

static void bdev_fini_done(void *cb_arg) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(cb_arg);
  fio_thread->fastfs->ready = false;
}

static void write_file_complete(void* cb_args, int code) {
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  WriteContext* writeCtx = reinterpret_cast<WriteContext*>(opCtx->private_data);
  if (code != 0) {
    SPDK_ERRLOG("write failed: %d\n", code);
    exit(code);
  }
  FastFS* fastfs = opCtx->fastfs;
  FastFile& file = (*fastfs->files)[writeCtx->fd];
  if (file.pos_ < g_test_file_size) {
    writeCtx->direct_buff->clear();
    fastfs->write(*opCtx);
  } else {
    fastfs->close(writeCtx->fd);
    fastfs->freeBuffer(writeCtx->direct_buff);
    fastfs->freeFsOp(opCtx);
    fastfs->ready = true;
  }
}

static void create_file_complete(void* cb_args, int code) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(
          spdk_thread_get_ctx(spdk_get_thread()));
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(cb_args);
  FastFS* fastfs = opCtx->fastfs;
  fastfs->freeFsOp(opCtx);
  if (code != 0) {
    printf("create test file failed : %d\n", code);
    exit(code);
  }
  if (fio_thread->td->o.td_ddir & TD_DDIR_READ) {
    // mock file's data
    opCtx = fastfs->allocFsOp();
    WriteContext* writeCtx = new (opCtx->private_data) WriteContext();
    writeCtx->fd = fio_thread->fastfs->open(g_test_file, F_MULTI_WRITE);
    writeCtx->count = FastFS::fs_context.extentSize;
    writeCtx->append = true;
    writeCtx->direct = true;
    writeCtx->direct_buff = fastfs->allocWriteBuffer(writeCtx->count);
    memset(writeCtx->direct_buff->p_buffer_, 'A', writeCtx->count);
    writeCtx->write_buff = writeCtx->direct_buff->p_buffer_;
    opCtx->callback = write_file_complete;
    opCtx->cb_args = opCtx;
    fastfs->write(*opCtx);
  } else {
    fastfs->ready = true;
  }
}

static void mount_complete(FastFS* fastfs, int code) {
  if (code != 0) {
    printf("mount fastfs failed: %d\n", code);
    return;
  }
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(
          spdk_thread_get_ctx(spdk_get_thread()));
  FastInode* inode = fastfs->status(g_test_file);
  fs_op_context* opCtx = fastfs->allocFsOp();
  if (!inode) {
    // create test file
    CreateContext* createCtx = new (opCtx->private_data) CreateContext();
    createCtx->parentId = 0;
    createCtx->name = g_test_file + 1;
    createCtx->mode = 493;
    createCtx->type = FASTFS_REGULAR_FILE;
    opCtx->callback = create_file_complete;
    opCtx->cb_args = opCtx;
    fastfs->create(*opCtx);
  } else { // file already exist
    if (fio_thread->td->o.td_ddir & TD_DDIR_WRITE) {
      TruncateContext* truncateCtx = new (opCtx->private_data) TruncateContext();
      truncateCtx->ino = inode->ino_;
      truncateCtx->size = 0;
      opCtx->callback = create_file_complete;
      opCtx->cb_args = opCtx;
      fastfs->truncate(*opCtx);
    } else {
      fastfs->ready = true;
    }
  }
}

static void format_complete(FastFS* fastfs, int code) {
  if (code != 0) {
    printf("format fastfs failed: %d\n", code);
    exit(code);
  }
  fastfs->mount(mount_complete, 128, 128);
}

static void fsbench_event_cb(
    enum spdk_bdev_event_type type, struct spdk_bdev*, void*) {
  printf("Unsupported bdev event: type %d\n", type);
}

static void bdev_init_done(int rc, void *cb_arg) {
  if (rc) {
    SPDK_ERRLOG("RUNTIME RPCs failed\n");
    exit(-1);
  }
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(cb_arg);
  FastFS* fastfs = fio_thread->fastfs;
  fs_context_t* fs_context = &FastFS::fs_context;
  fs_context->bdev = NULL;
  fs_context->bdev_desc = NULL;
  rc = spdk_bdev_open_ext(fs_context->bdev_name, true, fsbench_event_cb, NULL,
      &fs_context->bdev_desc);
  if (rc) {
    printf("Could not open bdev: %s\n", fs_context->bdev_name);
    exit(-1);
  }
  fs_context->bdev = spdk_bdev_desc_get_bdev(fs_context->bdev_desc);
  fs_context->bdev_io_channel = spdk_bdev_get_io_channel(fs_context->bdev_desc);
  if (fs_context->bdev_io_channel == NULL) {
    printf("Could not create bdev I/O channel!!\n");
    spdk_bdev_close(fs_context->bdev_desc);
    exit(-1);
  }
  if (g_do_format) {
    fastfs->format(1024 * 1024, format_complete, false);
  } else {
    fastfs->mount(mount_complete, 128, 128);
  }
}

static void bdev_subsystem_init_done(int rc, void *cb_arg) {
  if (rc) {
    SPDK_ERRLOG("subsystem init failed\n");
    exit(-1);
  }
  spdk_rpc_set_state(SPDK_RPC_RUNTIME);
  spdk_subsystem_load_config(
      g_json_data, g_config_file_size, bdev_init_done, cb_arg, true);
}

static void bdev_startup_done(int rc, void *cb_arg) {
  if (rc) {
    SPDK_ERRLOG("STARTUP RPCs failed\n");
    exit(-1);
  }
  spdk_subsystem_init(bdev_subsystem_init_done, cb_arg);
}

static void bdev_init_start(void *arg) {
  g_json_data = spdk_posix_file_load_from_name(
      g_config_file, &g_config_file_size);
  spdk_subsystem_load_config(
      g_json_data, g_config_file_size, bdev_startup_done, arg, true);
}

static int start_reactor(thread_data* td) {
  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  opts.name = "fastfs-fio";
  opts.opts_size = sizeof(opts);
  if (spdk_env_init(&opts) < 0) {
    printf("Unable to initialize SPDK env\n");
    return -1;
  }
  spdk_thread_lib_init(NULL, sizeof(struct fastfs_fio_thread));
  struct spdk_thread* thread = spdk_thread_create("fio_thread", NULL);
  if (!thread) {
    printf("failed to allocate thread\n");
    return -2;
  }

  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(spdk_thread_get_ctx(thread));
  fio_thread->td = td;
  fio_thread->thread = thread;
  g_test_file_size = td->o.size;
  fio_thread->iocq =
      (struct io_u**) calloc(td->o.iodepth, sizeof(struct io_u*));
  fio_thread->buffers =
      (struct ByteBuffer**) calloc(td->o.iodepth, sizeof(struct ByteBuffer*));
  fio_thread->count = 0;
  td->io_ops_data = fio_thread;
  spdk_set_thread(thread);

  FastFS* fastfs = new FastFS(g_bdev_name);
  fio_thread->fastfs = fastfs;

  spdk_thread_send_msg(fio_thread->thread, bdev_init_start, fio_thread);
  // polling until fastfs ready
  do {
    spdk_thread_poll(fio_thread->thread, 0, 0);
  } while (!fio_thread->fastfs->ready);
  while (spdk_thread_poll(fio_thread->thread, 0, 0) > 0) {};
  return 0;
}

static int fastfs_init(struct thread_data *td) {
  struct fastfs_fio_options* fio_options =
      reinterpret_cast<struct fastfs_fio_options*>(td->eo);
  g_config_file = fio_options->conf;
  g_bdev_name = fio_options->bdev;
  g_test_file = fio_options->file;
  if (fio_options->format != NULL &&
      strcmp(fio_options->format, "true") == 0) {
    g_do_format = true;
  } else {
    g_do_format = false;
  }
  printf("spark conf file %s, bdev name %s, test file %s, do format %s\n",
      g_config_file, g_bdev_name, g_test_file, fio_options->format);

  return start_reactor(td);
}

static void fastfs_cleanup(struct thread_data *td) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  if (FastFS::fs_context.bdev_io_channel) {
    spdk_put_io_channel(FastFS::fs_context.bdev_io_channel);
  }
  if (FastFS::fs_context.bdev_desc) {
    spdk_bdev_close(FastFS::fs_context.bdev_desc);
  }
  spdk_subsystem_fini(bdev_fini_done, fio_thread);
  do {
    spdk_thread_poll(fio_thread->thread, 0, 0);
  } while (fio_thread->fastfs->ready);
  spdk_thread_lib_fini();
  spdk_env_fini();
}

static int fastfs_invalidate(struct thread_data*, struct fio_file*) {
  return 0; // nothing to do
}

static int fastfs_open(struct thread_data *td, struct fio_file *f) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  int fd = fio_thread->fastfs->open(g_test_file, 0);
  if (fd < 0) {
    printf("Failed to open file\n");
    return 1;
  }
  f->fd = fd;
  return 0;
}

static void fsync_complete(void* cb_args, int) {
  int* rc = reinterpret_cast<int*>(cb_args);
  *rc = 1;
}

static int fastfs_close(struct thread_data *td, struct fio_file *f) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  int rc = 0;
  // do fsync first
  fs_op_context* opCtx = fio_thread->fastfs->allocFsOp();
  FSyncContext* fsyncCtx = new (opCtx->private_data) FSyncContext();
  fsyncCtx->fd = f->fd;
  opCtx->callback = fsync_complete;
  opCtx->cb_args = &rc;
  fio_thread->fastfs->fsync(*opCtx);
  while (!rc) {
    spdk_thread_poll(fio_thread->thread, 0, 0);
  }
  opCtx->fastfs->freeFsOp(opCtx);

  rc = fio_thread->fastfs->close(f->fd);
  f->fd = -1;
  return rc;
}

static void read_write_complete(void* arg, int code) {
  struct io_u* io_u = reinterpret_cast<struct io_u*>(arg);
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(
          spdk_thread_get_ctx(spdk_get_thread()));
  if (code != 0) {
    if (io_u->ddir == DDIR_WRITE) {
      printf("write file failed : %d\n", code);
    } else {
      printf("read file failed : %d\n", code);
    }
    exit(code);
  }
  fio_thread->iocq[fio_thread->count++] = io_u;
}

static void write_file(struct thread_data *td, struct io_u *io_u) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  FastFS* fastfs = fio_thread->fastfs;
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(io_u->engine_data);
  WriteContext* writeCtx = new (opCtx->private_data) WriteContext();

  writeCtx->fd = io_u->file->fd;
  writeCtx->pwrite = true;
  writeCtx->offset = io_u->offset;
  writeCtx->count = io_u->xfer_buflen;
  writeCtx->direct = true;
  writeCtx->direct_buff = fio_thread->buffers[io_u->index];
  writeCtx->write_buff = writeCtx->direct_buff->p_buffer_;

  opCtx->callback = read_write_complete;
  opCtx->cb_args = io_u;
  fastfs->write(*opCtx);
}

static void read_file(struct thread_data *td, struct io_u *io_u) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  FastFS* fastfs = fio_thread->fastfs;
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(io_u->engine_data);
  ReadContext* readCtx = new (opCtx->private_data) ReadContext();

  readCtx->fd = io_u->file->fd;
  readCtx->pread = true;
  readCtx->offset = io_u->offset;
  readCtx->count = io_u->xfer_buflen;
  readCtx->direct = true;
  readCtx->direct_buff = &(fio_thread->buffers[io_u->index]->clear());

  opCtx->callback = read_write_complete;
  opCtx->cb_args = io_u;
  fastfs->read(*opCtx);
}

static enum fio_q_status fastfs_queue(struct thread_data *td, struct io_u *io_u) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  if (fio_thread->writing) {
    return FIO_Q_BUSY;
  }
  if (io_u->ddir == DDIR_WRITE) {
    fio_thread->writing = true;
    fio_thread->reqs++;
    write_file(td, io_u);
    return FIO_Q_QUEUED;
  } else if (io_u->ddir == DDIR_READ) {
    fio_thread->reqs++;
    read_file(td, io_u);
    return FIO_Q_QUEUED;
  } else {
    io_u->error = errno;
    return FIO_Q_COMPLETED;
  }
}

static int fastfs_getevents(struct thread_data *td,
    unsigned int, unsigned int, const struct timespec*) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  while (fio_thread->count < fio_thread->reqs) {
    spdk_thread_poll(fio_thread->thread, 0, 0);
  }
  int res = fio_thread->count;
  fio_thread->count = 0;
  fio_thread->reqs = 0;
  fio_thread->writing = false;
  return res;
}

static struct io_u* fastfs_event(struct thread_data *td, int event) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  return fio_thread->iocq[event];
}

static int fastfs_io_u_init(struct thread_data* td, struct io_u *io_u) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  io_u->engine_data = fio_thread->fastfs->allocFsOp();
  return 0;
}

static void fastfs_io_u_free(struct thread_data *td, struct io_u *io_u) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  fs_op_context* opCtx = reinterpret_cast<fs_op_context*>(io_u->engine_data);
  if (opCtx) {
    fio_thread->fastfs->freeFsOp(opCtx);
    io_u->engine_data = NULL;
  }
}

static int fastfs_iomem_alloc(struct thread_data *td, size_t total_mem) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  fio_thread->buff = new ByteBuffer(
      total_mem, true, FastFS::fs_context.localNuma, 4096/*page_size*/);
  if (!fio_thread->buff) {
    printf("failed to allocate ByteBuffer\n");
    exit(-1);
  }
  td->orig_buffer = fio_thread->buff->getBuffer();
  uint32_t max_bs = td_max_bs(td);
  for (uint32_t i = 0; i < td->o.iodepth; i++) {
    fio_thread->buffers[i] = new ByteBuffer(
        fio_thread->buff->p_buffer_ + max_bs * i, max_bs);
  }
  return 0;
}

static void fastfs_iomem_free(struct thread_data *td) {
  struct fastfs_fio_thread* fio_thread =
      reinterpret_cast<struct fastfs_fio_thread*>(td->io_ops_data);
  if (fio_thread->buff) {
    delete fio_thread->buff;
    fio_thread->buff = nullptr;
  }
}

static int fastfs_setup(struct thread_data *td) {
  // tell FIO there is no need to generate test file
  td->o.create_on_open = 1;
  return 0;
}

extern "C" {
static struct ioengine_ops ioengine;
void get_ioengine(struct ioengine_ops **ioengine_ptr) {
    *ioengine_ptr = &ioengine;
    ioengine.name = "fastfs",
    ioengine.version = FIO_IOOPS_VERSION;
    ioengine.flags = FIO_NODISKUTIL;
    ioengine.setup = fastfs_setup;
    ioengine.init = fastfs_init;
    ioengine.invalidate = fastfs_invalidate;
    ioengine.open_file = fastfs_open;
    ioengine.queue = fastfs_queue;
    ioengine.getevents = fastfs_getevents;
    ioengine.event = fastfs_event;
    ioengine.close_file = fastfs_close;
    ioengine.cleanup = fastfs_cleanup;
    ioengine.io_u_init = fastfs_io_u_init;
    ioengine.io_u_free = fastfs_io_u_free;
    ioengine.iomem_alloc = fastfs_iomem_alloc;
    ioengine.iomem_free = fastfs_iomem_free;
    ioengine.option_struct_size = sizeof(struct fastfs_fio_options);
    ioengine.options = options;
}
}
