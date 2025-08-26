#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

SPDK_ROOT_DIR := /chenxu14/workspace/spdk
LDFLAGS = -Wl,-rpath=$(SPDK_ROOT_DIR)/dpdk/build/lib
LDFLAGS += -Wl,-rpath=$(SPDK_ROOT_DIR)/build/lib

include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk
CXXFLAGS += -std=c++17

FASTFS_ROOT_DIR := $(abspath $(CURDIR)/..)
SYS_LIBS += -L$(FASTFS_ROOT_DIR)/core -lfastfs
LDFLAGS += -Wl,-rpath=$(FASTFS_ROOT_DIR)/core
CXXFLAGS += -I$(FASTFS_ROOT_DIR)

SYS_LIBS += -lfuse3

APP = fastfs_fuse

CXX_SRCS := fastfs_fuse.cpp

SPDK_LIB_LIST = $(ALL_MODULES_LIST) event event_bdev

include $(SPDK_ROOT_DIR)/mk/spdk.app_cxx.mk
