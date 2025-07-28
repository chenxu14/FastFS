#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

FASTFS_ROOT_DIR := $(abspath $(CURDIR)/../..)
SPDK_ROOT_DIR := /chenxu14/workspace/spdk
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SYS_LIBS += -lcunit
LDFLAGS += -Wl,--gc-sections
CXXFLAGS += -std=c++17
CXXFLAGS += -I$(FASTFS_ROOT_DIR)
CXXFLAGS += -ffunction-sections

APP = bytebuffer_ut

CXX_SRCS := bytebuffer_ut.cpp

CLEAN_FILES = $(APP)

all : $(APP)
	@:

install: empty_rule

uninstall: empty_rule

# To avoid overwriting warning
empty_rule:
	@:

$(APP) : $(OBJS)
	$(Q)+echo "  LINK $(notdir $@)"; \
	$(CXX) -o $@ $(CXXFLAGS) $(LDFLAGS) $(OBJS) $(SYS_LIBS)

clean :
	$(CLEAN_C) $(CLEAN_FILES)
