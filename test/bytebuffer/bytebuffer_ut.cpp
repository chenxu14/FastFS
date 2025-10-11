/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

extern "C" {
#include <CUnit/Basic.h>
#include <CUnit/CUnit.h>
}
#include "core/ByteBuffer.h"

void *spdk_dma_zmalloc_socket(size_t size, size_t align, uint64_t *unused, int numa_id) { return malloc(size); }

void spdk_dma_free(void *buf) { free(buf); }

static void test_base_operation(void) {
  ByteBuffer buffer(1024);
  int64_t long_src = 123456;
  std::string text_src("Test Base Operation");
  uint32_t size = text_src.size();
  CU_ASSERT(buffer.putBytes(text_src.c_str(), size));
  buffer.mark();
  CU_ASSERT(buffer.write<int64_t>(long_src));

  ByteBuffer *dupBuf = buffer.duplicate();
  CU_ASSERT(dupBuf->position() == size + 8 /*int64_t*/);
  CU_ASSERT(dupBuf->limit() == 1024);
  delete dupBuf;

  ByteBuffer *sliceBuf = buffer.slice();
  CU_ASSERT(dupBuf->position() == 0);
  CU_ASSERT(dupBuf->limit() == buffer.remaining());
  delete sliceBuf;

  buffer.reset();
  CU_ASSERT(buffer.position() == size);
  int64_t long_target;
  CU_ASSERT(buffer.pread<int64_t>(size, long_target));
  CU_ASSERT(long_target == long_src);

  buffer.flip(); // limit upper size to cur position
  char text_target[text_src.size()];
  buffer.getBytes(text_target, text_src.size());
  CU_ASSERT(memcmp(text_target, text_src.data(), text_src.size()) == 0);
  CU_ASSERT(!buffer.read<int64_t>(long_target));
}

static void test_sequence_rw(void) {
  ByteBuffer buffer(1024);
  CU_ASSERT(buffer.capacity() == 1024);
  std::string text_src("FastFS");
  char char_src = 4;
  int32_t int_src = 1234;
  uint64_t long_src = 123456789;
  CU_ASSERT(buffer.putBytes(text_src.c_str(), text_src.size()));
  CU_ASSERT(buffer.putByte(char_src));
  CU_ASSERT(buffer.write<int32_t>(int_src));
  CU_ASSERT(buffer.write<uint64_t>(long_src));

  char text_target[text_src.size()];
  char char_target;
  int32_t int_target;
  uint64_t long_target;
  buffer.flip().getBytes(text_target, text_src.size());
  CU_ASSERT(memcmp(text_target, text_src.data(), text_src.size()) == 0);
  CU_ASSERT(buffer.getByte(char_target));
  CU_ASSERT(char_target == char_src);
  CU_ASSERT(buffer.read<int32_t>(int_target));
  CU_ASSERT(int_target == int_src);
  CU_ASSERT(buffer.read<uint64_t>(long_target));
  CU_ASSERT(long_target == long_src);
}

static void test_random_rw(void) {
  ByteBuffer buffer(1024);
  uint32_t pos = 0;
  std::string text_src("FastFS");
  char char_src = 4;
  int32_t int_src = 1234;
  uint64_t long_src = 123456789;
  CU_ASSERT(buffer.putBytes(pos, text_src.c_str(), text_src.size()));
  pos += text_src.size();
  CU_ASSERT(buffer.putByte(pos, char_src));
  pos += 1;
  CU_ASSERT(buffer.pwrite<int32_t>(pos, int_src));
  pos += 4;
  CU_ASSERT(buffer.pwrite<uint64_t>(pos, long_src));

  char text_target[text_src.size()];
  char char_target;
  int32_t int_target;
  uint64_t long_target;
  pos = 0;
  buffer.getBytes(pos, text_target, text_src.size());
  CU_ASSERT(memcmp(text_target, text_src.data(), text_src.size()) == 0);
  pos += text_src.size();
  CU_ASSERT(buffer.getByte(pos, char_target));
  CU_ASSERT(char_target == char_src);
  pos += 1;
  CU_ASSERT(buffer.pread<int32_t>(pos, int_target));
  CU_ASSERT(int_target == int_src);
  pos += 4;
  CU_ASSERT(buffer.pread<uint64_t>(pos, long_target));
  CU_ASSERT(long_target == long_src);

  CU_ASSERT(buffer.position() == 0);
  CU_ASSERT(buffer.capacity() == 1024);
  CU_ASSERT(buffer.limit() == 1024);
  CU_ASSERT(buffer.remaining() == 1024);
}

static void test_buffer_overflow(void) {
  ByteBuffer buffer(1024);
  std::string text_src("test buffer overflow");
  CU_ASSERT(!buffer.putBytes(1020, text_src.c_str(), text_src.size()));
  CU_ASSERT(!buffer.position(1020).putBytes(text_src.c_str(), text_src.size()));
  CU_ASSERT(!buffer.putByte(1024, 4));
  CU_ASSERT(!buffer.position(1024).putByte(4));
  CU_ASSERT(!buffer.pwrite<int64_t>(1020, 123456));
  CU_ASSERT(!buffer.position(1020).write<int64_t>(123456));

  buffer.clear();
  char text_target[10] = {0};
  char char_target;
  int64_t long_target;
  CU_ASSERT(!buffer.getBytes(1020, text_target, 10));
  CU_ASSERT(!buffer.position(1020).getBytes(text_target, 10));
  CU_ASSERT(!buffer.getByte(1024, char_target));
  CU_ASSERT(!buffer.position(1024).getByte(char_target));
  CU_ASSERT(!buffer.pread<int64_t>(1020, long_target));
  CU_ASSERT(!buffer.position(1020).read<int64_t>(long_target));
}

int main() {
  if (CUE_SUCCESS != CU_initialize_registry()) {
    return CU_get_error();
  }

  CU_pSuite suite = CU_add_suite("ByteBuffer Tests", NULL, NULL);
  if (suite == NULL) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  if (CU_add_test(suite, "base operation", test_base_operation) == NULL ||
      CU_add_test(suite, "buffer overflow", test_buffer_overflow) == NULL ||
      CU_add_test(suite, "random read write", test_random_rw) == NULL ||
      CU_add_test(suite, "sequence read write", test_sequence_rw) == NULL) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  CU_basic_set_mode(CU_BRM_VERBOSE);
  CU_basic_run_tests();

  CU_cleanup_registry();
  return CU_get_number_of_failures();
}
