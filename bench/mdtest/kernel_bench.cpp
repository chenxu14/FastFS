/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <string.h>

#define FILEMODE S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH

static int subdirs = 1024;
static int files_per_dir = 128;
static bool DIRECT_IO = false;
static bool readWrite = false;
static std::string PATH_PREFIX = "/chenxu14/mdtest_tree.";

struct timespec time_start;
struct timespec time_end;

struct fs_bench_context {
  uint32_t size = 4096;
  void* data = nullptr;
  bool verify = false;
  int writeFlags = 0;
  fs_bench_context() {
    if (DIRECT_IO) {
      if (posix_memalign(&data, getpagesize(), size) != 0) {
        printf("alloc buffer failed.\n");
        exit(-1);
      }
      writeFlags = O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT;
    } else {
      data = new char[size];
      writeFlags = O_WRONLY | O_CREAT | O_TRUNC;
    }
    memset(data, 'A', size);
  }
};
static fs_bench_context ctx;

static timespec timespec_diff(timespec start, timespec end) {
  timespec temp;
  if ((end.tv_nsec - start.tv_nsec) < 0) {
    temp.tv_sec = end.tv_sec - start.tv_sec - 1;
    temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
  } else {
    temp.tv_sec = end.tv_sec - start.tv_sec;
    temp.tv_nsec = end.tv_nsec - start.tv_nsec;
  }
  return temp;
}

static void remove() {
  clock_gettime(CLOCK_MONOTONIC, &time_start);
  for (int i = 0; i < subdirs; i++) {
    std::string path;
    for (int j = 0; j < files_per_dir; j++) {
      path = PATH_PREFIX + std::to_string(i) + "/file." + std::to_string(j);
      unlink(path.c_str());
    }
    path = PATH_PREFIX + std::to_string(i);
    rmdir(path.c_str());
  }
  clock_gettime(CLOCK_MONOTONIC, &time_end);
  auto d = timespec_diff(time_start, time_end);
  long inodes = subdirs * files_per_dir + subdirs;
  printf("[Remove] delete %ld inodes use %ld ms, ops is %s.\n",
      inodes, (long)(d.tv_sec * 1000 + d.tv_nsec / 1000000.0),
      std::to_string(inodes / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)).c_str());
}

static void read() {
  clock_gettime(CLOCK_MONOTONIC, &time_start);
  for (int i = 0; i < subdirs; i++) {
    for (int j = 0; j < files_per_dir; j++) {
      std::string path = PATH_PREFIX + std::to_string(i) + "/file." + std::to_string(j);
      int fd = open(path.c_str(), O_RDONLY, FILEMODE);
      if (readWrite) {
        void* read_buff = nullptr;
        if (DIRECT_IO) {
          if (posix_memalign(&read_buff, getpagesize(), ctx.size) != 0) {
            printf("alloc read buffer failed.\n");
            exit(-1);
          }
        } else {
          read_buff = malloc(ctx.size);
        }
        if (read(fd, read_buff, ctx.size) < 0 ||
            (ctx.verify && memcmp(read_buff, ctx.data, ctx.size) != 0)) {
          printf("read file failed");
          exit(-1);
        }
        if (read_buff) {
          free(read_buff);
        }
      }
      close(fd);
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &time_end);
  auto d = timespec_diff(time_start, time_end);
  long filesCnt = subdirs * files_per_dir;
  printf("[Read] read %ld files use %ld ms, ops is %s.\n",
      filesCnt, (long)(d.tv_sec * 1000 + d.tv_nsec / 1000000.0),
      std::to_string(filesCnt / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)).c_str());
}

static void write() {
  clock_gettime(CLOCK_MONOTONIC, &time_start);
  for (int i = 0; i < subdirs; i++) {
    for (int j = 0; j < files_per_dir; j++) {
      std::string path = PATH_PREFIX + std::to_string(i) + "/file." + std::to_string(j);
      int fd = open(path.c_str(), ctx.writeFlags, FILEMODE);
      if (readWrite) {
        if (write(fd, ctx.data, ctx.size) < 0) {
          printf("write file failed");
          exit(-1);
        }
      }
      close(fd);
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &time_end);
  auto d = timespec_diff(time_start, time_end);
  long filesCnt = subdirs * files_per_dir;
  printf("[Write] write %ld files use %ld ms, ops is %s.\n",
      filesCnt, (long)(d.tv_sec * 1000 + d.tv_nsec / 1000000.0),
      std::to_string(filesCnt / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)).c_str());
}

static void stat() {
  clock_gettime(CLOCK_MONOTONIC, &time_start);
  for (int i = 0; i < subdirs; i++) {
    for (int j = 0; j < files_per_dir; j++) {
      std::string path = PATH_PREFIX + std::to_string(i) + "/file." + std::to_string(j);
      struct stat fileInfo;
      if (stat(path.c_str(), &fileInfo) != 0) {
        printf("stat file failed : %s\n", path.c_str());
        exit(-1);
      }
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &time_end);
  auto d = timespec_diff(time_start, time_end);
  long filesCnt = subdirs * files_per_dir;
  printf("[Stats] stats %ld files use %ld ms, ops is %s.\n",
      filesCnt, (long)(d.tv_sec * 1000 + d.tv_nsec / 1000000.0),
      std::to_string(filesCnt / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)).c_str());
}

static void create() {
  for (int i = 0; i < subdirs; i++) {
    std::string path = PATH_PREFIX + std::to_string(i);
    mkdir(path.c_str(), FILEMODE);
  }
  clock_gettime(CLOCK_MONOTONIC, &time_start);
  for (int i = 0; i < subdirs; i++) {
    for (int j = 0; j < files_per_dir; j++) {
      std::string path = PATH_PREFIX + std::to_string(i) + "/file." + std::to_string(j);
      int fd = creat(path.c_str(), FILEMODE);
      if (fd == -1) {
        printf("create file failed : %s\n", path.c_str());
        exit(-1);
      }
      close(fd);
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &time_end);
  auto d = timespec_diff(time_start, time_end);
  long files = subdirs * files_per_dir;
  printf("[Creation] create %ld inodes use %ld ms, ops is %s.\n",
      files, (long)(d.tv_sec * 1000 + d.tv_nsec / 1000000.0),
      std::to_string(files / ((double) d.tv_sec + d.tv_nsec / 1000000000.0)).c_str());
}

int main(int argc, char **argv) {
  create();
  stat();
  write();
  read();
  remove();
}
