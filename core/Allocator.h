/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTFS_ALLOCATOR_H_
#define FASTFS_ALLOCATOR_H_

#include "spdk/bit_pool.h"
#include <unordered_set>
#define EXTENT_SIZE 1048576

class BitsAllocator {
 private:
  struct spdk_bit_pool* pool;
  uint32_t capacity_;

 public:
  BitsAllocator(uint32_t capacity) : capacity_(capacity) {
    pool = spdk_bit_pool_create(capacity);
  }

  ~BitsAllocator() {
    spdk_bit_pool_free(&pool);
  }

  uint32_t allocate() {
    return spdk_bit_pool_allocate_bit(pool);
  }

  void reserve(uint32_t index) {
    pool->lowest_free_bit = index;
    spdk_bit_pool_allocate_bit(pool);
  }

  void release(uint32_t index) {
    spdk_bit_pool_free_bit(pool, index);
  }

  uint32_t getFree() const {
    return spdk_bit_pool_count_free(pool);
  }

  uint32_t getAllocated() const {
    return spdk_bit_pool_count_allocated(pool);
  }
};

class BlockAllocator {
 private:
  struct spdk_bit_pool* pool;
  uint32_t capacity_;
  uint32_t blockSize_;
  uint32_t extentSize_;
  uint32_t extentBlocks_;

 public:
  BlockAllocator(
      uint64_t blocks, uint32_t blockSize, uint32_t extentSize)
      : blockSize_(blockSize), extentSize_(extentSize) {
    capacity_ = blocks * blockSize / extentSize;
    extentBlocks_ = extentSize / blockSize;
    pool = spdk_bit_pool_create(capacity_);
    reserve(0); // reserve first extent
  }

  ~BlockAllocator() {
    spdk_bit_pool_free(&pool);
  }

  uint32_t allocate() {
    return spdk_bit_pool_allocate_bit(pool);
  }

  void reserve(uint32_t index) {
    pool->lowest_free_bit = index;
    spdk_bit_pool_allocate_bit(pool);
  }

  void release(uint32_t index) {
    spdk_bit_pool_free_bit(pool, index);
  }

  uint32_t getBlockSize() const {
    return blockSize_;
  }

  uint32_t getExtentSize() const {
    return extentSize_;
  }

  uint32_t getCapacity() const {
    return capacity_;
  }

  uint32_t getExtentBlocks() const {
    return extentBlocks_;
  }

  uint32_t getFree() const {
    return spdk_bit_pool_count_free(pool);
  }

  uint32_t getLowestFreeIndex() const {
    return pool->lowest_free_bit;
  }
};

template <typename T>
class MemAllocator {
public:
  using value_type = T;

  template <typename U>
  struct rebind {
    using other = MemAllocator<U>;
  };

  explicit MemAllocator(uint32_t numa, uint32_t align)
      : numa_id(numa), align_(align) {}

  T* allocate(size_t n) {
    size_t size = n * sizeof(T);
    void* ptr = spdk_dma_zmalloc_socket(size, align_, NULL, numa_id);
    if (!ptr) {
      throw std::bad_alloc();
    }
    return static_cast<T*>(ptr);
  }

  void deallocate(T* ptr, size_t n) noexcept {
    spdk_dma_free(ptr);
  }

  template <typename U>
  MemAllocator(const MemAllocator<U>& other)
      : numa_id(other.numa_id), align_(other.align_) {}

  bool operator==(const MemAllocator& other) const {
    return (numa_id == other.numa_id) && (align_ == other.align_);
  }

  bool operator!=(const MemAllocator& other) const {
    return !(*this == other);
  }

  uint32_t numa_id;
  uint32_t align_;
};

#endif /* FASTFS_ALLOCATOR_H_ */
