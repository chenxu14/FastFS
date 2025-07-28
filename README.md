### Compilation and Build
The compilation depends on SPDK, DPDK, and FIO, with the versions spdk-25.05, dpdk-22.11, and fio-3.39 used respectively. 

The spdk.patch must be applied during SPDK compilation, and C++17 compiler support is required.

Assuming all projects are located in /chenxu14/workspace

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/chenxu14/workspace/spdk/build/lib:/chenxu14/workspace/spdk/dpdk/build/lib
    make

### Benchmark
MDTEST Bench

    ./bench/mdtest/fs_bench -c /chenxu14/workspace/FastFS/bench/bdev.json -b Malloc0

FIO Bench

    fio /chenxu14/workspace/FastFS/bench/fio/fastfs.fio

### Maintenance Tools
Format the filesystem

    ./tools/fastfs_tools -c /chenxu14/workspace/FastFS/bench/bdev_aio.json -b aio0 -f

Dump metadata

    ./tools/fastfs_tools -c /chenxu14/workspace/FastFS/bench/bdev_aio.json -b aio0 -D

Checkpoint operation

    ./tools/fastfs_tools -c /chenxu14/workspace/FastFS/bench/bdev_aio.json -b aio0 -C
