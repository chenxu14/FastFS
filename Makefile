#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

SPDK_ROOT_DIR := /chenxu14/workspace/spdk
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

DIRS-y += core
DIRS-y += tools
DIRS-y += bench
DIRS-y += fuse
DIRS-y += test

.PHONY: all clean $(DIRS-y)

all: $(DIRS-y)
clean: $(DIRS-y)

include $(SPDK_ROOT_DIR)/mk/spdk.subdirs.mk
