#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

SPDK_ROOT_DIR := /chenxu14/workspace/spdk
LDFLAGS = -Wl,-rpath=$(SPDK_ROOT_DIR)/dpdk/build/lib
LDFLAGS += -Wl,-rpath=$(SPDK_ROOT_DIR)/build/lib

include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.app_vars.mk
CXXFLAGS += -std=c++17
CXXFLAGS += -I$(SPDK_ROOT_DIR)/dpdk/build/include

FASTFS_ROOT_DIR := $(abspath $(CURDIR)/../..)
SYS_LIBS += -L$(FASTFS_ROOT_DIR)/core -lfastfs
LDFLAGS += -Wl,-rpath=$(FASTFS_ROOT_DIR)/core
CXXFLAGS += -I$(FASTFS_ROOT_DIR)

SYS_LIBS += -lcunit
LDFLAGS += -Wl,--gc-sections
CXXFLAGS += -ffunction-sections
CXXFLAGS += -DSPDK_UNIT_TEST=1

APP = fastfs_ut
CXX_SRCS := fastfs_ut.cpp

SPDK_LIB_LIST = $(ALL_MODULES_LIST) event event_bdev

include $(SPDK_ROOT_DIR)/mk/spdk.app_cxx.mk
