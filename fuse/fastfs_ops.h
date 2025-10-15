/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTFS_OPS_H_
#define FASTFS_OPS_H_

#include "core/FastFS.h"
#include "fastfs_fuse.h"
#include <vector>
#define FUSE_USE_VERSION 30
#include "fuse3/fuse.h"
#include "fuse3/fuse_lowlevel.h"

static int WRITE_HEADER_SIZE = 80; // fuse_in_header + fuse_write_in
static int TTL_PERIOD = 10;
static bool DIRECT_IO = true;
static int inflights = 0;

struct fastfs_fuse_context {
  FastFS* fastfs = nullptr;
  ByteBuffer* buffer = nullptr;
  struct spdk_thread* thread;
};
static struct fastfs_fuse_context* fuseCtx;

struct FuseOp {
  FuseOp* next = nullptr;
  fuse_req_t req = nullptr;
  struct fuse_file_info* file = nullptr;
  fs_op_context* opCtx = nullptr;
  ByteBuffer* buffer = nullptr;
  fuse_ino_t ino;
};
static FuseOp* op_head = nullptr;
static std::vector<FuseOp> fuseOps;

static FuseOp* allocFuseOp() {
  FuseOp* res = op_head;
  if (op_head) {
    op_head = op_head->next;
  }
  return res;
}

static void freeFuseOp(FuseOp* fuseOp) {
  if (fuseOp->buffer) {
    fuseCtx->fastfs->freeBuffer(fuseOp->buffer);
    fuseOp->buffer = nullptr;
  }
  if (fuseOp->opCtx) {
    fuseCtx->fastfs->freeFsOp(fuseOp->opCtx);
    fuseOp->opCtx = nullptr;
  }
  fuseOp->req = nullptr;
  fuseOp->file = nullptr;
  fuseOp->ino = 0;
  fuseOp->next = op_head;
  op_head = fuseOp;
}

static void fastfs_init(void*, struct fuse_conn_info* conn) {
  conn->max_read = 0; // no limit
  // limit max_write to extentSize
  // in order to use FastFS::allocBuffer to allocate fuse_buf
  conn->max_write = FastFS::fs_context.extentSize - WRITE_HEADER_SIZE;
  conn->max_background = 128;
  conn->want &= ~FUSE_CAP_SPLICE_READ;
  conn->want &= ~FUSE_CAP_READDIRPLUS; // not support yet
  conn->want &= ~FUSE_CAP_AUTO_INVAL_DATA; // TTL disable
  conn->want |= FUSE_CAP_ASYNC_READ;
  conn->want |= FUSE_CAP_ASYNC_DIO;

  // init object pool
  fuseOps.reserve(DEFAULT_POOL_SIZE);
  for (int i = 0; i < DEFAULT_POOL_SIZE - 1; i++) {
    fuseOps[i].buffer = nullptr;
    fuseOps[i].opCtx = nullptr;
    fuseOps[i].next = &fuseOps[i + 1];
  }
  fuseOps[DEFAULT_POOL_SIZE - 1].buffer = nullptr;
  fuseOps[DEFAULT_POOL_SIZE - 1].opCtx = nullptr;
  fuseOps[DEFAULT_POOL_SIZE - 1].next = nullptr;
  op_head = &fuseOps[0];
}

static void fastfs_getattr(
    fuse_req_t req, fuse_ino_t ino, struct fuse_file_info*) {
  FastInode& targetInode = (*fuseCtx->fastfs->inodes)[ino - 1];
  struct stat stbuf;
  memset(&stbuf, 0, sizeof(stbuf));
  stbuf.st_ino = targetInode.ino_;
  if (targetInode.type_ == FASTFS_REGULAR_FILE) {
    stbuf.st_mode = S_IFREG | 0644;
    stbuf.st_nlink = 1;
    stbuf.st_size = targetInode.size_;
    stbuf.st_blksize = FastFS::fs_context.extentSize;
    stbuf.st_blocks = targetInode.extents_->size();
  } else {
    stbuf.st_mode = S_IFDIR | 0755;
    stbuf.st_nlink = targetInode.children_->size() + 1;
  }
  fuse_reply_attr(req, &stbuf, TTL_PERIOD);
}

static void fastfs_setattr(fuse_req_t req, fuse_ino_t ino,
    struct stat*, int, struct fuse_file_info* fi) {
  // TODO chenxu14 support chmod, time and truncate
  fastfs_getattr(req, ino, fi);
}

static void fastfs_lookup(
    fuse_req_t req, fuse_ino_t parentId, const char *name) {
  FastInode* targetInode = fuseCtx->fastfs->lookup(parentId - 1, name);
  if (!targetInode) {
    fuse_reply_err(req, ENOENT);
  } else {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.ino = targetInode->ino_ + 1;
    e.attr_timeout = TTL_PERIOD;
    e.entry_timeout = TTL_PERIOD;
    e.attr.st_ino = targetInode->ino_;
    if (targetInode->type_ == FASTFS_REGULAR_FILE) {
      e.attr.st_mode = S_IFREG | 0644;
      e.attr.st_nlink = 1;
      e.attr.st_size = targetInode->size_;
      e.attr.st_blksize = FastFS::fs_context.extentSize;
      e.attr.st_blocks = targetInode->extents_->size();
    } else {
      e.attr.st_mode = S_IFDIR | 0755;
      e.attr.st_nlink = targetInode->children_->size() + 1;
    }
    fuse_reply_entry(req, &e);
  }
}

static void create_dir_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  CreateContext* createCtx =
      reinterpret_cast<CreateContext*>(fuseOp->opCtx->private_data);
  if (code == 0) {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.ino = createCtx->ino + 1;
    e.attr_timeout = TTL_PERIOD;
    e.entry_timeout = TTL_PERIOD;
    e.attr.st_ino = createCtx->ino;
    e.attr.st_mode = S_IFDIR | 0755;
    e.attr.st_nlink = 2;
    fuse_reply_entry(fuseOp->req, &e);
  } else {
    fuse_reply_err(fuseOp->req, EEXIST);
  }
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_mkdir(
    fuse_req_t req, fuse_ino_t pid, const char *name, mode_t mode) {
  inflights++;
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->req = req;
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  CreateContext* createCtx = new (fuseOp->opCtx->private_data) CreateContext();
  createCtx->parentId = pid - 1;
  createCtx->name = name;
  createCtx->mode = mode;
  createCtx->type = FASTFS_DIR;
  fuseOp->opCtx->callback = create_dir_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->create(*fuseOp->opCtx);
}

static void create_file_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  CreateContext* createCtx =
      reinterpret_cast<CreateContext*>(fuseOp->opCtx->private_data);
  if (code == 0) {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.ino = createCtx->ino + 1;
    e.attr_timeout = TTL_PERIOD;
    e.entry_timeout = TTL_PERIOD;
    e.attr.st_ino = createCtx->ino;
    e.attr.st_mode = S_IFREG | 0644;
    e.attr.st_nlink = 1;
    FastInode& targetInode = (*fuseCtx->fastfs->inodes)[createCtx->ino];
    e.attr.st_size = targetInode.size_;
    e.attr.st_blksize = FastFS::fs_context.extentSize;
    e.attr.st_blocks = targetInode.extents_->size();
    fuseOp->file->fh = fuseCtx->fastfs->open(
        createCtx->ino, fuseOp->file->flags | O_SYNC);
    fuse_reply_create(fuseOp->req, &e, fuseOp->file);
  } else {
    fuse_reply_err(fuseOp->req, EEXIST);
  }
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_create(fuse_req_t req, fuse_ino_t pid,
    const char *name, mode_t mode, struct fuse_file_info *fi) {
  inflights++;
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->req = req;
  fuseOp->file = fi;
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  CreateContext* createCtx = new (fuseOp->opCtx->private_data) CreateContext();
  createCtx->parentId = pid - 1;
  createCtx->name = name;
  createCtx->mode = mode;
  createCtx->type = FASTFS_REGULAR_FILE;
  fuseOp->opCtx->callback = create_file_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->create(*fuseOp->opCtx);
}

static void delete_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  fuse_reply_err(fuseOp->req, code == 0 ? code : ENOENT);
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_unlink(fuse_req_t req, fuse_ino_t pid, const char *name) {
  inflights++;
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->req = req;
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  DeleteContext* delCtx = new (fuseOp->opCtx->private_data) DeleteContext();
  delCtx->parentId = pid - 1;
  delCtx->name = name;
  delCtx->recursive = true;
  fuseOp->opCtx->callback = delete_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->remove(*fuseOp->opCtx);
}

static void fastfs_rmdir(fuse_req_t req, fuse_ino_t pid, const char *name) {
  fastfs_unlink(req, pid, name);
}

static void rename_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  fuse_reply_err(fuseOp->req, code == 0 ? code : ENOENT);
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_rename(fuse_req_t req, fuse_ino_t olddir,
    const char *oldname, fuse_ino_t newdir,
    const char *newname, unsigned int) {
  inflights++;
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->req = req;
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  RenameContext* renameCtx = new (fuseOp->opCtx->private_data) RenameContext();
  renameCtx->olddir = olddir - 1;
  renameCtx->oldname = oldname;
  renameCtx->newdir = newdir - 1;
  renameCtx->newname = newname;
  fuseOp->opCtx->callback = rename_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->rename(*fuseOp->opCtx);
}

static void fastfs_forget(fuse_req_t req, fuse_ino_t, uint64_t) {
  fuse_reply_none(req);
}

static void fastfs_forgetmulti(
    fuse_req_t req, size_t, struct fuse_forget_data*) {
  fuse_reply_none(req);
}

static void fastfs_flush(
    fuse_req_t req, fuse_ino_t, struct fuse_file_info*) {
  fuse_reply_err(req, 0);
}

static void fsync_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  if (code != 0) {
    fuse_reply_err(fuseOp->req, ENOENT);
  } else {
    fuse_reply_err(fuseOp->req, 0);
  }
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_fsync(
    fuse_req_t req, fuse_ino_t, int, struct fuse_file_info* fi) {
  inflights++;
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->req = req;
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  FSyncContext* fsyncCtx = new (fuseOp->opCtx->private_data) FSyncContext();
  fsyncCtx->fd = fi->fh;
  fuseOp->opCtx->callback = fsync_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->fsync(*fuseOp->opCtx);
}

static void truncate_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  if (code != 0) {
    fuse_reply_err(fuseOp->req, ENOENT);
  } else {
    int fd = fuseCtx->fastfs->open(fuseOp->ino - 1, fuseOp->file->flags | O_SYNC);
    if (fd < 0) {
      fuse_reply_err(fuseOp->req, ENOENT);
    } else {
      fuseOp->file->fh = fd;
      if (DIRECT_IO) {
        fuseOp->file->direct_io = 1;
      }
      fuse_reply_open(fuseOp->req, fuseOp->file);
    }
  }
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_open(
    fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
  if (fi->flags & O_TRUNC) {
    inflights++;
    FuseOp* fuseOp = allocFuseOp();
    fuseOp->req = req;
    fuseOp->file = fi;
    fuseOp->ino = ino;
    fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
    TruncateContext* truncateCtx =
        new (fuseOp->opCtx->private_data) TruncateContext();
    truncateCtx->ino = ino - 1;
    truncateCtx->size = 0;
    fuseOp->opCtx->callback = truncate_complete;
    fuseOp->opCtx->cb_args = fuseOp;
    fuseCtx->fastfs->truncate(*fuseOp->opCtx);
  } else {
    int fd = fuseCtx->fastfs->open(ino - 1, fi->flags | O_SYNC);
    if (fd < 0) {
      fuse_reply_err(req, ENOENT);
    } else {
      fi->fh = fd;
      if (DIRECT_IO) {
        fi->direct_io = 1;
      }
      fuse_reply_open(req, fi);
    }
  }
}

static void fastfs_release(
    fuse_req_t req, fuse_ino_t, struct fuse_file_info* fi) {
  uint32_t fd = static_cast<uint32_t>(fi->fh);
  int rc = fuseCtx->fastfs->close(fd);
  fuse_reply_err(req, rc < 0 ? ENOENT : 0);
}

static void fastfs_add_dentry(
    fuse_req_t req, ByteBuffer* buffer, const char *name, fuse_ino_t ino) {
  size_t len = fuse_add_direntry(req, NULL, 0, name, NULL, 0);
  struct stat stbuf;
  memset(&stbuf, 0, sizeof(stbuf));
  stbuf.st_ino = ino;
  fuse_add_direntry(
      req, buffer->getBuffer(), buffer->remaining(),
      name, &stbuf, buffer->position_ + len);
  buffer->skip(len);
}

// TODO chenxu14 consider offset
static void fastfs_readdir(fuse_req_t req, fuse_ino_t ino,
    size_t size, off_t off, struct fuse_file_info*) {
  FastInode& targetInode = (*fuseCtx->fastfs->inodes)[ino - 1];
  if (targetInode.type_ != FASTFS_DIR) {
    fuse_reply_err(req, ENOTDIR);
  } else {
    ByteBuffer* buffer = fuseCtx->fastfs->allocBuffer();
    fastfs_add_dentry(req, buffer, ".", 1);
    fastfs_add_dentry(req, buffer, "..", 1);
    for (auto& ino : *(targetInode.children_)) {
      FastInode& inode = (*fuseCtx->fastfs->inodes)[ino];
      fastfs_add_dentry(req, buffer, inode.name_.c_str(), inode.ino_ + 1);
    }
    if ((uint32_t) off < buffer->position_) {
      size_t len = buffer->position_ - off;
      fuse_reply_buf(req, buffer->p_buffer_ + off, len < size ? len : size);
    } else {
      fuse_reply_buf(req, NULL, 0);
    }

    fuseCtx->fastfs->freeBuffer(buffer);
  }
}

static void fastfs_releasedir(
    fuse_req_t req, fuse_ino_t, struct fuse_file_info*) {
  fuse_reply_err(req, 0);
}

static void fastfs_opendir(
    fuse_req_t req, fuse_ino_t, struct fuse_file_info* fi) {
  fuse_reply_open(req, fi);
}

static void read_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  ReadContext* readCtx =
      reinterpret_cast<ReadContext*>(fuseOp->opCtx->private_data);
  if (code == -2) { // EOF
    fuse_reply_buf(fuseOp->req, NULL, 0);
  } else if (code != 0) {
    fuse_reply_err(fuseOp->req, EIO);
  } else {
    struct fuse_bufvec bufvec;
    bufvec.count = 1;
    bufvec.idx = 0;
    bufvec.off = 0;
    bufvec.buf[0].mem = readCtx->direct_buff->p_buffer_ + readCtx->direct_cursor;
    bufvec.buf[0].size = readCtx->count;
    fuse_reply_data(fuseOp->req, &bufvec, FUSE_BUF_NO_SPLICE);
  }
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_read(fuse_req_t req, fuse_ino_t, size_t size,
    off_t off, struct fuse_file_info* fi) {
  inflights++;
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->buffer = fuseCtx->fastfs->allocReadBuffer(off, size);
  fuseOp->req = req;
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  ReadContext* readCtx = new (fuseOp->opCtx->private_data) ReadContext();
  readCtx->fd = fi->fh;
  readCtx->pread = true;
  readCtx->direct = true;
  readCtx->offset = off;
  readCtx->count = size;
  readCtx->direct_buff = fuseOp->buffer;
  readCtx->direct_cursor = fuseOp->buffer->position();
  fuseOp->opCtx->callback = read_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->read(*fuseOp->opCtx);
}

static void write_complete(void* cb_args, int code) {
  FuseOp* fuseOp = reinterpret_cast<FuseOp*>(cb_args);
  WriteContext* writeCtx =
      reinterpret_cast<WriteContext*>(fuseOp->opCtx->private_data);
  if (code != 0) {
    fuse_reply_err(fuseOp->req, EIO);
  } else {
    fuse_reply_write(fuseOp->req, writeCtx->count);
  }
  freeFuseOp(fuseOp);
  inflights--;
}

static void fastfs_write(fuse_req_t req, fuse_ino_t,
    struct fuse_bufvec* buf, off_t off, struct fuse_file_info* fi) {
  inflights++;
  struct fuse_buf* flatbuf = &buf->buf[0];
  FuseOp* fuseOp = allocFuseOp();
  fuseOp->req = req;
  fuseOp->buffer = fuseCtx->buffer;
  fuseCtx->buffer = nullptr; // avoid release in main loop
  fuseOp->opCtx = fuseCtx->fastfs->allocFsOp();
  WriteContext* writeCtx = new (fuseOp->opCtx->private_data) WriteContext();
  writeCtx->fd = fi->fh;
  writeCtx->pwrite = true;
  writeCtx->offset = off;
  writeCtx->count = flatbuf->size;
  writeCtx->direct = true;
  writeCtx->direct_buff = &fuseOp->buffer->skip(WRITE_HEADER_SIZE);
  writeCtx->write_buff = (char*) flatbuf->mem;
  fuseOp->opCtx->callback = write_complete;
  fuseOp->opCtx->cb_args = fuseOp;
  fuseCtx->fastfs->write(*fuseOp->opCtx);
}

static void fastfs_getxattr(
    fuse_req_t req, fuse_ino_t, const char*, size_t) {
  fuse_reply_err(req, ENOTSUP);
}

static void fastfs_setxattr(
    fuse_req_t req, fuse_ino_t, const char*, const char*, size_t, int) {
  fuse_reply_err(req, ENOTSUP);
}

static void fastfs_removexattr(fuse_req_t req, fuse_ino_t, const char*) {
  fuse_reply_err(req, ENOTSUP);
}

static void fastfs_statfs(fuse_req_t req, fuse_ino_t) {
  auto& ctx = FastFS::fs_context;
  struct statvfs buf;
  memset(&buf, 0, sizeof(buf));
  buf.f_bsize = ctx.blockSize;
  buf.f_blocks = ctx.blocks;
  buf.f_bfree = ctx.allocator->getFree() * (uint64_t) ctx.allocator->getExtentBlocks();
  buf.f_bavail = buf.f_bfree;
  buf.f_namemax = NAME_MAX;
  buf.f_files = ctx.maxInodes;
  buf.f_ffree = ctx.inodeAllocator->getFree();
  buf.f_favail = buf.f_ffree;
  fuse_reply_statfs(req, &buf);
}

static void fastfs_ioctl(fuse_req_t req, fuse_ino_t, int cmd, void*,
    struct fuse_file_info* fi, unsigned flags, const void*,size_t, size_t) {
  if (flags & FUSE_IOCTL_COMPAT) {
    fuse_reply_err(req, ENOSYS);
    return;
  }
  switch ((uint64_t) cmd) {
    case FASTFS_IOCTL_GET_FD : {
      int32_t realFd = fi->fh;
      fuse_reply_ioctl(req, 0, &realFd, sizeof(int32_t));
      break;
    }
    default : {
      fuse_reply_err(req, ENOTTY);
    }
  }
}

static void init_fuse_ops(struct fuse_lowlevel_ops& ops) {
  ops.init = fastfs_init;
  ops.lookup = fastfs_lookup;
  ops.forget = fastfs_forget;
  ops.forget_multi = fastfs_forgetmulti;
  ops.getattr = fastfs_getattr;
  ops.setattr = fastfs_setattr;
  ops.opendir = fastfs_opendir;
  ops.releasedir = fastfs_releasedir;
  ops.readdir = fastfs_readdir;
  ops.mkdir = fastfs_mkdir;
  ops.rmdir = fastfs_rmdir;
  ops.rename = fastfs_rename,
  ops.create = fastfs_create;
  ops.unlink = fastfs_unlink;
  ops.flush = fastfs_flush;
  ops.fsync = fastfs_fsync;
  ops.open = fastfs_open;
  ops.release = fastfs_release;
  ops.read = fastfs_read;
  ops.write_buf = fastfs_write;
  ops.setxattr = fastfs_setxattr;
  ops.getxattr = fastfs_getxattr;
  ops.removexattr = fastfs_removexattr;
  ops.statfs = fastfs_statfs;
  ops.ioctl = fastfs_ioctl;
}

#endif /* FASTFS_OPS_H_ */
