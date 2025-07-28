/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */
#ifndef FAST_FS_BYTE_BUFFER_H_
#define FAST_FS_BYTE_BUFFER_H_

#include <cstdlib>
#include <string>
#include <iostream>
#include <string.h>
#include <memory>
#include "spdk/env.h"

#define DEFAULT_BUFFER_SIZE 2048

class ByteBuffer {
public:
  ByteBuffer(uint32_t capacity = DEFAULT_BUFFER_SIZE,
      bool alloc = true, int32_t numa = 0, int32_t align = 1)
      : mark_(0), limit_(capacity), position_(0), capacity_(capacity), alloc_(alloc) {
    if (alloc_) {
      p_buffer_ = static_cast<char*>(spdk_dma_zmalloc_socket(capacity_, align, NULL, numa));
    }
  }

  ByteBuffer(char* buffer, uint32_t size) : ByteBuffer(size, false) {
    p_buffer_ = buffer;
  }

  ~ByteBuffer() {
    if (alloc_ && p_buffer_) {
      spdk_dma_free(p_buffer_);
    }
    p_buffer_ = nullptr;
  }

  ByteBuffer& limit(uint32_t newLimit) {
    if (position_ > newLimit) {
      position_ = newLimit;
    }
    limit_ = newLimit;
    return *this;
  }

  ByteBuffer& position(uint32_t newPosition) {
    position_ = newPosition;
    return *this;
  }

  ByteBuffer& skip(uint32_t len) {
    position_ += len;
    return *this;
  }

  ByteBuffer* duplicate() {
    ByteBuffer* newBuffer = new ByteBuffer(capacity_, false);
    newBuffer->p_buffer_ = p_buffer_;
    newBuffer->limit(limit_);
    newBuffer->position(position_);
    return newBuffer;
  }

  ByteBuffer* slice() {
    ByteBuffer *newBuffer = new ByteBuffer(remaining(), false);
    newBuffer->p_buffer_ = p_buffer_ + position_;
    newBuffer->limit(remaining());
    newBuffer->position(0);
    return newBuffer;
  }

  ByteBuffer& clear() {
    position_ = 0;
    mark_ = 0;
    limit_ = capacity_;
    return *this;
  }

  ByteBuffer& flip() {
    limit_ = position_;
    position_ = 0;
    mark_ = 0;
    return *this;
  }

  ByteBuffer& mark() {
    mark_ = position_;
    return *this;
  }

  ByteBuffer& reset() {
    position_ = mark_;
    return *this;
  }

  bool putBytes(const char* buf, uint32_t len) {
    if (!p_buffer_ || position_ + len > capacity_) {
      return false;
    }
    memcpy(&p_buffer_[position_], buf, len);
    position_ += len;
    return true;
  }

  bool putBytes(uint32_t index, const char* buf, uint32_t len) {
    if (!p_buffer_ || index + len > capacity_) {
      return false;
    }
    memcpy(&p_buffer_[index], buf, len);
    return true;
  }

  bool putByte(char value) {
    if (!p_buffer_ || position_ >= capacity_) {
      return false;
    }
    p_buffer_[position_++] = value;
    return true;
  }

  bool putByte(uint32_t index, char value) {
    if (!p_buffer_ || index >= capacity_) {
      return false;
    }
    p_buffer_[index] = value;
    return true;
  }

  template<typename T>
  bool write(T data) {
    uint32_t len = sizeof(data);
    if (!p_buffer_ || position_ + len > capacity_) {
      return false;
    }
    memcpy(&p_buffer_[position_], &data, len);
    position_ += len;
    return true;
  }

  template<typename T>
  bool pwrite(uint32_t index, T data) {
    uint32_t len = sizeof(data);
    if (!p_buffer_ || index + len > capacity_) {
      return false;
    }
    memcpy(&p_buffer_[index], &data, len);
    return true;
  }

  bool getBytes(char* buf, uint32_t len) {
    if (!p_buffer_ || position_ + len > limit_) {
      return false;
    }
    memcpy(buf, &p_buffer_[position_], len);
    position_ += len;
    return true;
  }

  bool getBytes(uint32_t index, char* buf, uint32_t len) const {
    if (!p_buffer_ || index + len > limit_) {
      return false;
    }
    memcpy(buf, &p_buffer_[index], len);
    return true;
  }

  bool getByte(char& val) {
    if (!p_buffer_ || position_ >= limit_) {
      return false;
    }
    val = p_buffer_[position_++];
    return true;
  }

  bool getByte(uint32_t index, char& val) const {
    if (!p_buffer_ || index >= limit_) {
      return false;
    }
    val = p_buffer_[index];
    return true;
  }

  template<typename T>
  bool read(T& val) {
    uint32_t len = sizeof(val);
    if (!p_buffer_ || position_ + len > limit_) {
      return false;
    }
    val = *((T *) &p_buffer_[position_]);
    position_ += len;
    return true;
  }

  template<typename T>
  bool pread(uint32_t index, T& val) const {
    uint32_t len = sizeof(val);
    if (!p_buffer_ || index + len > limit_) {
      return false;
    }
    val = *((T *) &p_buffer_[index]);
    return true;
  }

  uint32_t remaining() const {
    return position_ < limit_ ? limit_ - position_ : 0;
  }

  bool writable(int size) {
    return position_ + size < limit_;
  }

  uint32_t capacity() const {
    return capacity_;
  }

  uint32_t position() const {
    return position_;
  }

  uint32_t limit() const {
    return limit_;
  }

  char* getBuffer() {
    return p_buffer_ + position_;
  }

 public:
  uint64_t mark_;
  uint32_t limit_;
  uint32_t position_;
  uint32_t capacity_;
  char* p_buffer_ = nullptr;
  ByteBuffer* next = nullptr;
  void* private_data = nullptr;
  bool alloc_;
  char padding[8]; // align cache line
};

#endif  /* FAST_FS_BYTE_BUFFER_H_ */
