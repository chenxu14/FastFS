## Here's my code: 
### Compilation and Build
The compilation depends on SPDK (as git submodule in third_party/spdk), DPDK, and optionally FIO for plugin support, with the versions spdk-25.05, dpdk-22.11, and fio-3.39 used respectively. 

The spdk.patch must be applied during SPDK compilation, and C++17 compiler support is required.

Assuming all projects are located in /chenxu14/workspace

#### Initialize SPDK Submodule
First, initialize the SPDK submodule:
```bash
git submodule update --init
```

#### Using CMake (Recommended)
```bash
# Basic build (automatically compiles SPDK and builds without FIO plugin)
./compile.sh

# Build with FIO plugin support (automatically downloads and compiles FIO)
./compile.sh --enable-fio-plugin

# Build without automatically compiling SPDK (assuming SPDK is already built)
./compile.sh --disable-spdk-build

# Build with custom FIO version
./compile.sh --enable-fio-plugin --fio-version fio-3.39

# Manual CMake configuration
mkdir build && cd build
cmake .. -DENABLE_FIO_PLUGIN=ON -DBUILD_SPDK=ON -DBUILD_FIO=ON
make -j$(nproc)
```

#### Environment Setup
```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/third_party/spdk/build/lib:$(pwd)/third_party/spdk/dpdk/build/lib
```

### Benchmark
MDTEST Bench

    ./build/bin/fs_bench -c /path/to/FastFS/bench/bdev.json -b Malloc0

FIO Bench

    fio /path/to/FastFS/bench/fio/fastfs.fio

### Maintenance Tools
Format the filesystem

    ./build/bin/fastfs_tools -c /path/to/FastFS/bench/bdev_aio.json -b aio0 -f

Dump metadata

    ./build/bin/fastfs_tools -c /path/to/FastFS/bench/bdev_aio.json -b aio0 -D

Checkpoint operation

    ./build/bin/fastfs_tools -c /path/to/FastFS/bench/bdev_aio.json -b aio0 -C

Mount FUSE

    ./build/bin/fastfs_fuse -c /path/to/FastFS/bench/bdev_aio.json -b aio0 -m /mnt/fastfs

### CMake Build Options
- `-DBUILD_SPDK=ON/OFF`: Automatically build SPDK submodule (default: ON)
- `-DENABLE_FIO_PLUGIN=ON/OFF`: Enable FIO plugin support (default: OFF)
- `-DBUILD_FIO=ON/OFF`: Automatically build FIO when plugin enabled (default: ON)
- `-DFIO_VERSION=version`: FIO version to download (default: fio-3.39)
- `-DENABLE_FUSE=ON/OFF`: Enable FUSE support (default: ON)
- `-DENABLE_TESTS=ON/OFF`: Enable unit tests (default: ON)