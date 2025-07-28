#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

SPDK_ROOT_DIR := /chenxu14/workspace/spdk
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

#XXHASH_DIR := /chenxu14/workspace/xxHash
#SYS_LIBS += -L$(XXHASH_DIR) -lxxhash
#LDFLAGS += -Wl,-rpath=$(XXHASH_DIR)
#CXXFLAGS += -I$(XXHASH_DIR)
CXXFLAGS += -I$(SPDK_ROOT_DIR)/dpdk/build/include

SO_NAME = libfastfs.so
CXXFLAGS += -shared -std=c++17
LDFLAGS = -Wl,-rpath=$(SPDK_ROOT_DIR)/dpdk/build/lib
LDFLAGS += -Wl,-rpath=$(SPDK_ROOT_DIR)/build/lib

CXX_SRCS = $(wildcard *.cpp)
SPDK_LIB_LIST = $(ALL_MODULES_LIST) event event_bdev

include $(SPDK_ROOT_DIR)/mk/spdk.app_vars.mk
LIBS += $(SPDK_LIB_LINKER_ARGS)

all : $(SO_NAME)
	@:

install: empty_rule

uninstall: empty_rule

# To avoid overwriting warning
empty_rule:
	@:

$(SO_NAME) : $(OBJS) $(SPDK_LIB_FILES) $(ENV_LIBS)
	$(CXX) -o $@ $(CXXFLAGS) $(LDFLAGS) $(OBJS) $(LIBS) $(ENV_LDFLAGS) $(SYS_LIBS)

clean :
	rm -rf ${OBJS} ${SO_NAME}

include $(SPDK_ROOT_DIR)/mk/spdk.deps.mk

