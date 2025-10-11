/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include <getopt.h>

#include "fastfs_ops.h"
#include "spdk/file.h"
#include "spdk/rpc.h"

extern int inflights;
static void *g_json_data;
static size_t g_config_file_size;

struct {
  const char *spdk_conf = NULL;
  const char *spdk_bdev = NULL;
  const char *mountpoint = NULL;
  int format = false;
  int debug = false;
} options;

static void print_usage() {
  printf("fastfs_fuse -c bdev.json -b Malloc0 -f\n"
         "  -c, --spdk_conf    SPDK configuration file\n"
         "  -b, --spdk_bdev    SPDK bdev name\n"
         "  -m, --mountpoint   fastfs mount point\n"
         "  -f, --format       format fastfs before mount\n"
         "  -d, --debug        enable debug\n");
}

static void parse_options(int argc, char *argv[]) {
  static struct option options_config[] = {{"spdk_conf", required_argument, 0, 'c'},
                                           {"spdk_bdev", required_argument, 0, 'b'},
                                           {"mountpoint", required_argument, 0, 'm'},
                                           {"format", no_argument, &options.format, 'f'},
                                           {"debug", no_argument, &options.debug, 'd'},
                                           {0, 0, 0, 0}};

  int c = 0;
  while (c >= 0) {
    int option_index;
    c = getopt_long(argc, argv, "c:b:m:fd", options_config, &option_index);
    switch (c) {
    case 'c':
      options.spdk_conf = optarg;
      break;
    case 'b':
      options.spdk_bdev = optarg;
      break;
    case 'm':
      options.mountpoint = optarg;
      break;
    case 'f':
      options.format = true;
      break;
    case 'd':
      options.debug = true;
      break;
    default:
      break;
    }
  }

  if (!options.spdk_conf || !options.spdk_bdev || !options.mountpoint) {
    print_usage();
    exit(1);
  }
}

static void mount_complete(FastFS *fastfs, int code) {
  if (code != 0) {
    printf("mount fastfs failed: %d\n", code);
    exit(code);
  }
  fastfs->ready = true;
}

static void format_complete(FastFS *fastfs, int code) {
  if (code != 0) {
    printf("format fastfs failed: %d\n", code);
    exit(code);
  }
  fastfs->mount(mount_complete);
}

static void fuse_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *ctx) {
  printf("Unsupported bdev event: type %d\n", type);
}

static void bdev_init_done(int rc, void *cb_arg) {
  FastFS *fastfs = reinterpret_cast<struct FastFS *>(cb_arg);
  fs_context_t &fs_context = FastFS::fs_context;
  fs_context.bdev = NULL;
  fs_context.bdev_desc = NULL;
  rc = spdk_bdev_open_ext(fs_context.bdev_name, true, fuse_event_cb, NULL, &fs_context.bdev_desc);
  if (rc) {
    printf("Could not open bdev: %s\n", fs_context.bdev_name);
    exit(-1);
  }
  fs_context.bdev = spdk_bdev_desc_get_bdev(fs_context.bdev_desc);
  fs_context.bdev_io_channel = spdk_bdev_get_io_channel(fs_context.bdev_desc);
  if (fs_context.bdev_io_channel == NULL) {
    printf("Could not create bdev I/O channel!!\n");
    spdk_bdev_close(fs_context.bdev_desc);
    exit(-1);
  }
  if (options.format) {
    fastfs->format(4096 * 4, format_complete);
  } else {
    fastfs->mount(mount_complete);
  }
}

static void bdev_subsystem_init_done(int rc, void *cb_arg) {
  if (rc) {
    SPDK_ERRLOG("subsystem init failed\n");
    exit(-1);
  }
  spdk_rpc_set_state(SPDK_RPC_RUNTIME);
  spdk_subsystem_load_config(g_json_data, g_config_file_size, bdev_init_done, cb_arg, true);
}

static void bdev_startup_done(int rc, void *cb_arg) {
  if (rc) {
    SPDK_ERRLOG("STARTUP RPCs failed\n");
    exit(-1);
  }
  spdk_subsystem_init(bdev_subsystem_init_done, cb_arg);
}

static void bdev_init_start(void *arg) {
  g_json_data = spdk_posix_file_load_from_name(options.spdk_conf, &g_config_file_size);
  spdk_subsystem_load_config(g_json_data, g_config_file_size, bdev_startup_done, arg, true);
}

static FastFS *mount_fastfs() {
  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  opts.name = "fastfs-fuse";
  opts.mem_size = 1024;
  opts.hugepage_single_segments = 1;
  opts.opts_size = sizeof(opts);
  if (spdk_env_init(&opts) < 0) {
    printf("Unable to initialize SPDK env\n");
    return nullptr;
  }
  spdk_thread_lib_init(NULL, sizeof(struct fastfs_fuse_context));
  struct spdk_thread *thread = spdk_thread_create("fuse_thread", NULL);
  if (!thread) {
    printf("failed to allocate thread\n");
    return nullptr;
  }

  fuseCtx = reinterpret_cast<struct fastfs_fuse_context *>(spdk_thread_get_ctx(thread));
  fuseCtx->thread = thread;
  spdk_set_thread(thread);
  FastFS *fastfs = new FastFS(options.spdk_bdev);
  fuseCtx->fastfs = fastfs;
  spdk_thread_send_msg(thread, bdev_init_start, fastfs);

  // polling until fastfs ready
  do {
    spdk_thread_poll(thread, 0, 0);
  } while (!fastfs->ready);
  while (spdk_thread_poll(thread, 0, 0) > 0) {};
  return fastfs;
}

static void bdev_fini_done(void *cb_arg) {
  FastFS *fastfs = reinterpret_cast<struct FastFS *>(cb_arg);
  fastfs->ready = false;
}

static void umount_fastfs(FastFS *fastfs) {
  if (FastFS::fs_context.bdev_io_channel) {
    spdk_put_io_channel(FastFS::fs_context.bdev_io_channel);
  }
  if (FastFS::fs_context.bdev_desc) {
    spdk_bdev_close(FastFS::fs_context.bdev_desc);
  }
  spdk_subsystem_fini(bdev_fini_done, fastfs);
  struct spdk_thread *thread = spdk_get_thread();
  do {
    spdk_thread_poll(thread, 0, 0);
  } while (fastfs->ready);
  spdk_thread_lib_fini();
  spdk_env_fini();
}

int main(int argc, char **argv) {
  parse_options(argc, argv);
  std::vector<std::string> fuseArgs;
  std::string programName(argv[0]);
  fuseArgs.push_back(programName);
  fuseArgs.push_back("-o");
  fuseArgs.push_back("allow_other");
  fuseArgs.push_back("-o");
  fuseArgs.push_back("default_permissions");
  fuseArgs.push_back("-o");
  fuseArgs.push_back("auto_unmount");
  fuseArgs.push_back("-o");
  fuseArgs.push_back("subtype=fastfs");
  if (options.debug) {
    fuseArgs.push_back("-o");
    fuseArgs.push_back("debug");
  }
  fuseArgs.push_back(options.mountpoint);
  std::vector<char *> fuseArgsPtr;
  for (auto &arg : fuseArgs) {
    fuseArgsPtr.push_back(const_cast<char *>(arg.c_str()));
  }
  struct fuse_args args = FUSE_ARGS_INIT((int)fuseArgsPtr.size(), fuseArgsPtr.data());

  struct fuse_session *se = nullptr;
  struct spdk_thread *thread = nullptr;
  FastFS *fastfs = nullptr;
  struct fuse_cmdline_opts opts;
  struct pollfd fds[1];
  int ret = -1;

  if (fuse_parse_cmdline(&args, &opts) != 0) {
    return 1;
  }

  fastfs = mount_fastfs();
  if (!fastfs) {
    printf("Failed to mount FastFS\n");
    goto err_out1;
  }

  struct fuse_lowlevel_ops fastfs_oper;
  init_fuse_ops(fastfs_oper);
  se = fuse_session_new(&args, &fastfs_oper, sizeof(fastfs_oper), NULL);
  if (se == NULL) {
    goto err_out1;
  }

  if (fuse_set_signal_handlers(se) != 0) {
    goto err_out2;
  }
  if (fuse_session_mount(se, opts.mountpoint) != 0) {
    goto err_out3;
  }

  fuse_lowlevel_version();

  fds[0].fd = fuse_session_fd(se);
  fds[0].events = POLLIN;
  thread = spdk_get_thread();
  struct fuse_buf fbuf;

  // below copy from fuse_session_loop
  while (!fuse_session_exited(se)) {
    int timeout = inflights > 0 ? 0 : -1;
    int res = poll(fds, 1, timeout);
    if (res > 0) {
      if (fds[0].revents & POLLIN) {
        fuseCtx->buffer = fastfs->allocBuffer();
        fbuf.mem = fuseCtx->buffer->getBuffer();
        fbuf.size = fuseCtx->buffer->capacity();
        res = fuse_session_receive_buf(se, &fbuf);
        if (res == -EINTR) {
          fastfs->freeBuffer(fuseCtx->buffer);
          continue;
        }
        if (res <= 0) {
          printf("do receive buf failed : %d\n", res);
          fastfs->freeBuffer(fuseCtx->buffer);
          break;
        }
        fuse_session_process_buf(se, &fbuf);
        if (fuseCtx->buffer) { // maybe reset by write
          fastfs->freeBuffer(fuseCtx->buffer);
        }
      }
    } else if (res < 0) {
      printf("exit loop, error code is %d\n", res);
      break;
    }
    spdk_thread_poll(thread, 0, 0);
  }

  fuse_session_reset(se);
  fuse_session_unmount(se);
  umount_fastfs(fastfs);

err_out3:
  fuse_remove_signal_handlers(se);
err_out2:
  // fuse_session_destroy(se);
err_out1:
  free(opts.mountpoint);
  fuse_opt_free_args(&args);
  return ret ? 1 : 0;
}
