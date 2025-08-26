/*
 * Copyright (C) 2025 chenxu14
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTFS_FUSE_H_
#define FASTFS_FUSE_H_

#include <sys/ioctl.h>

#define FASTFS_IOCTYPE_ID 'f'

enum {
  FASTFS_IOCTL_GET_FD = _IOR(FASTFS_IOCTYPE_ID, 1, int32_t)
};

#endif /* FASTFS_FUSE_H_ */
