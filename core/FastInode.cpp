/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "FastFS.h"
#include "xxh3.h"

void FastFile::open(uint32_t flags, FastInode* inode) {
  this->flags_ = flags;
  this->inode_ = inode;
  this->tail_block = nullptr;
  this->pos_ = 0;
  this->inode_->refCnts_++;
}

void FastFile::close() {
  if (inode_) {
    inode_->unlink();
    inode_ = nullptr;
  }
  if (tail_block) {
    spdk_dma_free(tail_block->p_buffer_);
    delete tail_block;
    tail_block = nullptr;
  }
}

void FastInode::create(uint32_t ino, uint32_t parentId,
    std::string_view name, FileType type) {
  this->ino_ = ino;
  this->parentId_ = parentId;
  this->next_ = 0;
  this->name_ = name;
  this->size_ = 0;
  this->type_ = type;
  this->refCnts_ = 1;
  this->status_ = 1; // in use
  if (type == FASTFS_REGULAR_FILE) {
    this->extents_ = new std::vector<uint32_t>();
  } else if (type == FASTFS_DIR) {
    this->children_ = new std::set<uint32_t>();
  }
}

void FastInode::unlink() {
  if (--refCnts_ == 0) {
    if (type_ == FASTFS_REGULAR_FILE) {
      for (auto& extentId : *extents_) {
        if (extentId != UINT32_MAX) {
          FastFS::fs_context.allocator->release(extentId);
        }
      }
      delete extents_;

      if (dirtyExtents) {
        // clear dirty extents
        for (auto& [index, extentInfo] : *dirtyExtents) {
          if (extentInfo.first != UINT32_MAX) {
            FastFS::fs_context.allocator->release(extentInfo.first);
          }
        }
        delete dirtyExtents;
        dirtyExtents = nullptr;
      }
    } else if (type_ == FASTFS_DIR) {
      delete children_;
    }
    FastFS::fs_context.inodeAllocator->release(ino_);
    status_ = 0; // released
  }
}

bool FastInode::getExtent(uint64_t offset, uint32_t& index, uint32_t& extentId) {
  index = static_cast<uint32_t>(offset >> FastFS::fs_context.extentBits);
  if (index < extents_->size()) {
    extentId = (*extents_)[index];
    return extentId != UINT32_MAX;
  }
  return false;
}

uint32_t FastFS::hashSlot(uint32_t parentId, std::string_view name) const {
  std::size_t h1 = std::hash<std::uint32_t>{}(parentId);
  // std::size_t h2 = std::hash<std::string_view>{}(name);
  std::size_t h2 = XXH3_64bits(name.data(), name.size());
  return (h1 ^ (h2 << 1)) & fs_context.inodesMask;
}

FastInode* FastFS::lookup(uint32_t parentId, std::string_view name) const {
  uint32_t ino = (*slots)[hashSlot(parentId, name)];
  while (ino) {
    FastInode& inode = (*inodes)[ino];
    if ((parentId == inode.parentId_) && (name == inode.name_)) {
      return &inode;
    }
    ino = inode.next_;
  }
  return nullptr;
}

bool FastFS::lookup(uint32_t parentId, std::string_view name,
    uint32_t& pre, uint32_t& ino) const {
  bool head = true;
  pre = hashSlot(parentId, name);
  uint32_t inodeId = (*slots)[pre];
  FastInode* inode = nullptr;
  while (inodeId) {
    inode = &(*inodes)[inodeId];
    if ((parentId == inode->parentId_) && (name == inode->name_)) {
      ino = inodeId;
      return head;
    }
    pre = inodeId;
    inodeId = inode->next_;
    head = false;
  }
  return head;
}

FastInode* FastFS::status(const std::string& path) const {
  FastInode* cusor = root;
  size_t start = 1;
  size_t i = 1;
  for (; i < path.size(); i++) {
    if (path[i] == kDelimiter) {
      std::string_view name(path.data() + start, i - start);
      start = i + 1;
      cusor = lookup(cusor->ino_, name);
      if (!cusor) {
        return nullptr;
      }
    }
  }
  if (start != i) { // path not end with '/'
    std::string_view name(path.data() + start, i - start);
    cusor = lookup(cusor->ino_, name);
  }
  return cusor;
}

int FastFS::open(uint32_t ino, uint32_t flags) {
  uint32_t fd = fs_context.fdAllocator->allocate();
  if (fd == UINT32_MAX) {
    SPDK_WARNLOG("no free FD.\n");
    return -1;
  }
  (*files)[fd].open(flags, &(*inodes)[ino]);
  return fd;
}

int FastFS::open(const std::string& path, uint32_t flags) {
  FastInode* inode = status(path);
  if (!inode) {
    return -1;
  }

  uint32_t fd = fs_context.fdAllocator->allocate();
  if (fd == UINT32_MAX) {
    SPDK_WARNLOG("no free FD.\n");
    return -2;
  }
  (*files)[fd].open(flags, inode);
  return fd;
}

int FastFS::close(uint32_t fd) {
  int res = -1;
  if (fd < fs_context.maxFiles) {
    (*files)[fd].close();
    fs_context.fdAllocator->release(fd);
    res = 0;
  }
  return res;
}

int64_t FastFS::seek(uint32_t fd, uint64_t offset, int whence) {
  if (fd < fs_context.maxFiles) {
    FastFile& file = (*files)[fd];
    uint64_t pos = offset; // SEEK_SET
    if (whence == SEEK_CUR) {
      pos = file.pos_ + offset;
    } else if (whence == SEEK_END) {
      pos = file.inode_->size_ + offset;
    }
    file.pos_ = pos;
    return pos;
  }
  return -1;
}

static void writeJournalComplete(void* cb_args, int code) {
  fs_op_context* ctx = reinterpret_cast<fs_op_context*>(cb_args);
  ctx->fastfs->journal->freeEditOp();
  ctx->callback(ctx->cb_args, code);
}

int FastFS::applyCreate(CreateContext* createCtx) {
  if (!fs_context.inodeAllocator->getFree()) {
    SPDK_WARNLOG("no free INode.\n");
    return -1;
  }

  uint32_t parentId = createCtx->parentId;
  FastInode& parent = (*inodes)[parentId];
  if (parent.status_ != 1) {
    return -2; // parent not exist
  }

  uint32_t pre = 0;
  uint32_t ino = 0;
  bool head = lookup(parentId, createCtx->name, pre, ino);
  if (ino) {
    return -3; // file already exist
  }

  if (createCtx->ino != UINT32_MAX) { // journal replay case
    ino = createCtx->ino;
    fs_context.inodeAllocator->reserve(ino);
  } else {
    ino = fs_context.inodeAllocator->allocate();
    createCtx->ino = ino;
  }

  if (ino == UINT32_MAX) {
    SPDK_WARNLOG("No available inodes");
    return -4; // no available inodes
  }

  FastInode& target = (*inodes)[ino];
  target.create(ino, parentId, createCtx->name, createCtx->type);
  target.mode_ = createCtx->mode;

  if (head) {
    (*slots)[pre] = ino;
  } else {
    (*inodes)[pre].next_ = ino;
  }
  parent.children_->insert(ino);
  return 0;
}

void FastFS::create(fs_op_context& ctx) {
  CreateContext* createCtx = reinterpret_cast<CreateContext*>(ctx.private_data);
  int code = applyCreate(createCtx);
  if (code != 0) {
    return ctx.callback(ctx.cb_args, code);
  }
  if (fs_context.skipJournal) {
    return ctx.callback(ctx.cb_args, 0);
  }

  EditOp* editOp = journal->allocEditOp();
  editOp->opctx = createCtx;
  editOp->type = 0;
  editOp->size = 14 + createCtx->name.size();
  editOp->callback = writeJournalComplete;
  editOp->cb_args = &ctx;
  editOp->phrase = !editOp->phrase;
}

static void createPath(void* cb_args, int code) {
  fs_op_context* ctx = reinterpret_cast<fs_op_context*>(cb_args);
  CreateContext* createCtx = reinterpret_cast<CreateContext*>(ctx->private_data);
  if (code != 0) {
    createCtx->callback(createCtx->args, code);
    ctx->fastfs->freeFsOp(ctx);
    return;
  }
  if (createCtx->ino != UINT32_MAX) { // new created
    // change parent
    createCtx->parentId = createCtx->ino;
    createCtx->ino = UINT32_MAX;
  }

  size_t i = createCtx->pos;
  for (; i < createCtx->path.size(); i++) {
    if (createCtx->path[i] == kDelimiter) {
      createCtx->name = std::string_view(
          createCtx->path.data() + createCtx->pos, i - createCtx->pos);
      createCtx->pos = i + 1;
      FastInode* inode = ctx->fastfs->lookup(createCtx->parentId, createCtx->name);
      if (!inode) {
        return ctx->fastfs->create(*ctx);
      } else {
        createCtx->parentId = inode->ino_;
      }
    }
  }
  if (createCtx->pos != i) { // path not end with '/'
    createCtx->name = std::string_view(
        createCtx->path.data() + createCtx->pos, i - createCtx->pos);
    createCtx->pos = i; // make position reach end
    FastInode* inode = ctx->fastfs->lookup(createCtx->parentId, createCtx->name);
    if (!inode) { // create last
      return ctx->fastfs->create(*ctx);
    }
  }
  createCtx->callback(createCtx->args, 0);
  ctx->fastfs->freeFsOp(ctx);
}

void FastFS::createRecursive(const std::string& path, op_cb callback, void* args) {
  fs_op_context* ctx = allocFsOp();
  CreateContext* createCtx = new (ctx->private_data) CreateContext();
  createCtx->parentId = 0;
  createCtx->mode = 493;
  createCtx->type = FASTFS_DIR;
  createCtx->path = path;
  createCtx->pos = 1; // skip first '/'
  createCtx->callback = callback;
  createCtx->args = args;
  ctx->callback = createPath;
  ctx->cb_args = ctx;
  return createPath(ctx, 0);
}

int FastFS::applyTruncate(TruncateContext* truncateCtx) {
  FastInode& inode = (*inodes)[truncateCtx->ino];
  if (inode.status_ != 1) {
    return -1; // file not find
  }
  uint64_t targetSize = truncateCtx->size;
  if (targetSize > inode.size_) {
    return -2; // don't support sparse file
  }

  int startId = (targetSize >> fs_context.extentBits);
  if ((targetSize == 0) || (targetSize & fs_context.extentMask)) {
    startId++;
  }

  for (int i = inode.extents_->size() - 1; i >= startId; i--) {
    uint32_t extentId = (*inode.extents_)[i];
    if (extentId != UINT32_MAX) {
      fs_context.allocator->release(extentId);
    }
    inode.extents_->pop_back();
  }

  inode.size_ = targetSize;
  return 0;
}

void FastFS::truncate(fs_op_context& ctx) {
  TruncateContext* truncateCtx =
      reinterpret_cast<TruncateContext*>(ctx.private_data);
  int code = applyTruncate(truncateCtx);
  if (code != 0) {
    return ctx.callback(ctx.cb_args, code);
  }
  if (fs_context.skipJournal) {
    return ctx.callback(ctx.cb_args, 0);
  }

  EditOp* editOp = journal->allocEditOp();
  editOp->opctx = ctx.private_data;
  editOp->type = 1;
  editOp->size = 12; /*ino(4) + size(8)*/
  editOp->callback = writeJournalComplete;
  editOp->cb_args = &ctx;
  editOp->phrase = !editOp->phrase;
}

void FastFS::removeRecursive(FastInode* dir) {
  uint32_t pre = 0;
  uint32_t ino = 0;
  for (auto& inodeId : *(dir->children_)) {
    FastInode& inode = (*inodes)[inodeId];
    bool head = lookup(inode.parentId_, inode.name_, pre, ino);
    if (!ino) {
      SPDK_WARNLOG("inode lookup failed, this should not happen!\n");
    }
    // remove from hash list
    if (head) {
      (*slots)[pre] = inode.next_;
    } else {
      (*inodes)[pre].next_ = inode.next_;
    }
    inode.next_ = 0;

    switch (inode.type_) {
      case FASTFS_REGULAR_FILE: {
        inode.status_ = 2;
        inode.unlink();
        break;
      }
      case FASTFS_DIR: {
        removeRecursive(&inode);
        break;
      }
      default:
        break;
    }
  }
  dir->status_ = 2;
  dir->unlink();
}

int FastFS::applyRemove(DeleteContext* delCtx) {
  FastInode& parent = (*inodes)[delCtx->parentId];
  if (parent.status_ != 1) {
    return -1; // parent not exist
  }

  uint32_t pre = 0;
  uint32_t ino = 0;
  bool head = lookup(delCtx->parentId, delCtx->name, pre, ino);
  if (!ino) {
    return -2; // file not exist
  }

  FastInode& target = (*inodes)[ino];
  if (target.type_ == FASTFS_DIR && !delCtx->recursive
      && target.children_->size() > 0) {
    return -3; // dir not empty
  }

  parent.children_->erase(ino);
  // remove from hash list
  if (head) {
    (*slots)[pre] = target.next_;
  } else {
    (*inodes)[pre].next_ = target.next_;
  }
  target.next_ = 0;

  switch (target.type_) {
    case FASTFS_REGULAR_FILE: {
      target.status_ = 2/*delete*/;
      target.unlink();
      break;
    }
    case FASTFS_DIR: {
      removeRecursive(&target);
	  break;
    }
    default:
	  break;
  }
  return 0;
}

void FastFS::remove(fs_op_context& ctx) {
  DeleteContext* delCtx = reinterpret_cast<DeleteContext*>(ctx.private_data);
  int code = applyRemove(delCtx);
  if (code != 0) {
    return ctx.callback(ctx.cb_args, code);
  }
  if (fs_context.skipJournal) {
    return ctx.callback(ctx.cb_args, 0);
  }

  EditOp* editOp = journal->allocEditOp();
  editOp->opctx = delCtx;
  editOp->type = 2;
  editOp->size = 6 + delCtx->name.size();
  editOp->callback = writeJournalComplete;
  editOp->cb_args = &ctx;
  editOp->phrase = !editOp->phrase;
}

int FastFS::applyRename(RenameContext* renameCtx) {
  FastInode& parentOld = (*inodes)[renameCtx->olddir];
  FastInode& parentNew = (*inodes)[renameCtx->newdir];
  if (parentOld.status_ != 1 || parentNew.status_ != 1) {
    return -1;
  }

  uint32_t srcPre = 0;
  uint32_t srcIno = 0;
  bool sourceHead = lookup(renameCtx->olddir, renameCtx->oldname, srcPre, srcIno);
  if (!srcIno) {
    return -2;
  }

  uint32_t tgtPre = 0;
  uint32_t tgtIno = 0;
  bool tgtHead = lookup(renameCtx->newdir, renameCtx->newname, tgtPre, tgtIno);
  FastInode& source = (*inodes)[srcIno];
  FastInode& target = (*inodes)[tgtIno];
  if (tgtIno && (target.type_ != source.type_)) {
    return -3;
  }

  // remove source from original hash list
  if (sourceHead) {
    (*slots)[srcPre] = source.next_;
  } else {
    (*inodes)[srcPre].next_ = source.next_;
  }

  source.next_ = 0;
  source.parentId_ = renameCtx->newdir;
  source.name_ = renameCtx->newname;

  // add source to new hash list
  if (tgtHead) {
    (*slots)[tgtPre] = srcIno;
  } else {
    (*inodes)[tgtPre].next_ = srcIno;
  }

  if (tgtIno) {
    // target file exist, delete first
    parentNew.children_->erase(tgtIno);
    source.next_ = target.next_;
    target.next_ = 0;
    target.status_ = 2;
    target.unlink();
  }

  if (renameCtx->olddir != renameCtx->newdir) {
    parentOld.children_->erase(srcIno);
    parentNew.children_->insert(srcIno);
  }
  return 0;
}

void FastFS::rename(fs_op_context& ctx) {
  RenameContext* renameCtx = reinterpret_cast<RenameContext*>(ctx.private_data);
  int code = applyRename(renameCtx);
  if (code != 0) {
    return ctx.callback(ctx.cb_args, code);
  }
  if (fs_context.skipJournal) {
    return ctx.callback(ctx.cb_args, 0);
  }

  int32_t size = 10; /*olddir(4) + newdir(4) + oldnameLen(1) + newnameLen(1)*/
  size += renameCtx->oldname.size();
  size += renameCtx->newname.size();
  EditOp* editOp = journal->allocEditOp();
  editOp->opctx = renameCtx;
  editOp->type = 5;
  editOp->size = size;
  editOp->callback = writeJournalComplete;
  editOp->cb_args = &ctx;
  editOp->phrase = !editOp->phrase;
}
