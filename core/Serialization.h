/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTFS_SERDE_H_
#define FASTFS_SERDE_H_

#include <string_view>

#include "ByteBuffer.h"

class FastInode;

enum FileType : int {
  FASTFS_REGULAR_FILE = 0,
  FASTFS_DIR = 1,
  FASTFS_SYMBAL_LINK = 2
};

typedef void (*op_cb)(void *cb_args, int code);

class SuperBlock {
  public:
  uint64_t ckptInodesLoc;
  uint64_t ckptDentryLoc;
  uint64_t lastTxid;
  uint64_t journalLoc;
  uint32_t epoch;
  uint32_t journalSkipBlocks;
  uint32_t journalSkipOps;
  uint32_t extentSize;
  uint32_t flags;
  uint32_t version;

  void serialize(ByteBuffer *buf) {
    buf->putBytes("FAsT", 4);
    buf->write<uint64_t>(ckptInodesLoc);
    buf->write<uint64_t>(ckptDentryLoc);
    buf->write<uint64_t>(lastTxid);
    buf->write<uint64_t>(journalLoc);
    buf->write<uint32_t>(epoch);
    buf->write<uint32_t>(journalSkipBlocks);
    buf->write<uint32_t>(journalSkipOps);
    buf->write<uint32_t>(extentSize);
    buf->write<uint32_t>(flags);
    buf->write<uint32_t>(version);
  }

  bool deserialize(ByteBuffer *buf) {
    char magicWord[4]{0};
    buf->getBytes(magicWord, 4);
    if (memcmp(magicWord, "FAsT", 4) != 0) {
      return false;
    }
    buf->read<uint64_t>(ckptInodesLoc);
    buf->read<uint64_t>(ckptDentryLoc);
    buf->read<uint64_t>(lastTxid);
    buf->read<uint64_t>(journalLoc);
    buf->read<uint32_t>(epoch);
    buf->read<uint32_t>(journalSkipBlocks);
    buf->read<uint32_t>(journalSkipOps);
    buf->read<uint32_t>(extentSize);
    buf->read<uint32_t>(flags);
    buf->read<uint32_t>(version);
    return true;
  }
};

class INodeFile {
  public:
  static constexpr int32_t kFixSize = 22;
  uint32_t ino;
  uint32_t parent_id;
  uint32_t mode;
  uint64_t size;
  std::string_view name;
  FileType type;

  void serialize(ByteBuffer *buf) {
    buf->write<uint32_t>(ino);
    buf->write<uint32_t>(parent_id);
    buf->write<uint32_t>(mode);
    buf->write<uint64_t>(size);
    uint8_t nameLen = name.size();
    buf->write<uint8_t>(nameLen);
    buf->putBytes(name.data(), nameLen);
    buf->putByte((type == FASTFS_DIR ? 1 : 0));
  }

  bool deserialize(ByteBuffer *buf) {
    bool res = true;
    buf->read<uint32_t>(ino);
    buf->read<uint32_t>(parent_id);
    buf->read<uint32_t>(mode);
    buf->read<uint64_t>(size);
    uint8_t nameLen = 0;
    buf->read<uint8_t>(nameLen);
    name = std::string_view(buf->getBuffer(), nameLen);
    buf->skip(nameLen);
    char val = 0;
    res = buf->getByte(val);
    type = (val == 1 ? FASTFS_DIR : FASTFS_REGULAR_FILE);
    return res;
  }
};

class CreateContext {
  public:
  uint32_t parentId;
  std::string_view name;
  uint32_t ino; // used by journal replay case
  uint32_t mode;
  FileType type;
  // below used by createRecursive
  std::string_view path; // should start with '/'
  uint32_t pos;
  op_cb callback;
  void *args;

  CreateContext() : ino(UINT32_MAX) {}

  void serialize(ByteBuffer *buf) { // TODO chenxu14 add magic header
    buf->write<uint32_t>(parentId);
    uint8_t nameLen = name.size();
    buf->write<uint8_t>(nameLen);
    buf->putBytes(name.data(), nameLen);
    buf->write<uint32_t>(ino);
    buf->write<uint32_t>(mode);
    buf->putByte((type == FASTFS_DIR ? 1 : 0));
  }

  void deserialize(ByteBuffer *buf) {
    buf->read<uint32_t>(parentId);
    uint8_t nameLen = 0;
    buf->read<uint8_t>(nameLen);
    name = std::string_view(buf->getBuffer(), nameLen);
    buf->skip(nameLen);
    buf->read<uint32_t>(ino);
    buf->read<uint32_t>(mode);
    char val = 0;
    buf->getByte(val);
    type = (val == 1 ? FASTFS_DIR : FASTFS_REGULAR_FILE);
  }
};

class DeleteContext {
  public:
  uint32_t parentId;
  std::string name;
  bool recursive = false;

  void serialize(ByteBuffer *buf) {
    buf->write<uint32_t>(parentId);
    uint8_t nameLen = name.size();
    buf->write<uint8_t>(nameLen);
    buf->putBytes(name.data(), nameLen);
    char val = recursive ? 1 : 0;
    buf->putByte(val);
  }

  void deserialize(ByteBuffer *buf) {
    buf->read<uint32_t>(parentId);
    uint8_t nameLen = 0;
    buf->read<uint8_t>(nameLen);
    name = std::string(buf->getBuffer(), nameLen);
    buf->skip(nameLen);
    char val = 0;
    buf->getByte(val);
    recursive = (val == 0 ? false : true);
  }
};

class TruncateContext {
  public:
  uint32_t ino;
  uint64_t size;

  void serialize(ByteBuffer *buf) {
    buf->write<uint32_t>(ino);
    buf->write<uint64_t>(size);
  }

  void deserialize(ByteBuffer *buf) {
    buf->read<uint32_t>(ino);
    buf->read<uint64_t>(size);
  }
};

class RenameContext {
  public:
  uint32_t olddir;
  uint32_t newdir;
  std::string oldname;
  std::string newname;

  void serialize(ByteBuffer *buf) {
    buf->write<uint32_t>(olddir);
    buf->write<uint32_t>(newdir);
    uint8_t nameLen = oldname.size();
    buf->write<uint8_t>(nameLen);
    buf->putBytes(oldname.data(), nameLen);
    nameLen = newname.size();
    buf->write<uint8_t>(nameLen);
    buf->putBytes(newname.data(), nameLen);
  }

  void deserialize(ByteBuffer *buf) {
    buf->read<uint32_t>(olddir);
    buf->read<uint32_t>(newdir);
    uint8_t nameLen = 0;
    buf->read<uint8_t>(nameLen);
    oldname = std::string(buf->getBuffer(), nameLen);
    buf->skip(nameLen);
    buf->read<uint8_t>(nameLen);
    newname = std::string(buf->getBuffer(), nameLen);
    buf->skip(nameLen);
  }
};

#endif /* FASTFS_SERDE_H_ */
