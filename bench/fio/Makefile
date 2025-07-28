#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

SPDK_ROOT_DIR := /chenxu14/workspace/spdk
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk
CXXFLAGS += -std=c++17
LDFLAGS = -Wl,-rpath=$(SPDK_ROOT_DIR)/dpdk/build/lib
LDFLAGS += -Wl,-rpath=$(SPDK_ROOT_DIR)/build/lib

FASTFS_ROOT_DIR := $(abspath $(CURDIR)/../..)
SYS_LIBS += -L$(FASTFS_ROOT_DIR)/core -lfastfs
LDFLAGS += -Wl,-rpath=$(FASTFS_ROOT_DIR)/core
CXXFLAGS += -I$(FASTFS_ROOT_DIR)

FIO_SRC_DIR := /chenxu14/workspace/fio
PLUGIN_NAME = fastfs_fio
SO_NAME = ${PLUGIN_NAME}.so
CXXFLAGS += -I${FIO_SRC_DIR} -include config-host.h
CXXFLAGS += -fpermissive -O3 -shared -rdynamic

CXX_SRCS := ${PLUGIN_NAME}.cpp
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
