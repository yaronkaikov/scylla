#!/bin/bash
#
# Build LLVM/Clang from latest upstream source
# Usage: ./build-clang-latest.sh [VERSION] [PREFIX]
#   VERSION: LLVM/Clang version (default: 21.1.8)
#   PREFIX: Installation prefix (default: /usr/local/clang)
#

set -e

CLANG_VERSION="${1:-21.1.8}"
PREFIX="${2:-/usr/local/clang}"

echo "=========================================="
echo "Building LLVM/Clang ${CLANG_VERSION}"
echo "=========================================="
echo "Installation prefix: ${PREFIX}"
echo ""

BUILD_DIR="/tmp/clang-build-${CLANG_VERSION}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Clone LLVM project
echo "[1/4] Cloning LLVM project (version ${CLANG_VERSION})..."
if ! git clone --depth 1 --branch "llvmorg-${CLANG_VERSION}" \
    "https://github.com/llvm/llvm-project.git" "llvm-project" 2>&1 | tail -20; then
    echo "  Shallow clone failed, attempting full clone (this may take a while)..."
    if ! git clone --branch "llvmorg-${CLANG_VERSION}" \
        "https://github.com/llvm/llvm-project.git" "llvm-project" 2>&1 | tail -20; then
        echo "ERROR: Failed to clone LLVM project"
        exit 1
    fi
fi
if [ ! -d "llvm-project/llvm/CMakeLists.txt" ] && [ ! -f "llvm-project/llvm/CMakeLists.txt" ]; then
    echo "ERROR: LLVM project directory is empty or invalid"
    exit 1
fi
echo "  ✓ Cloned"

# Configure
echo "[2/4] Configuring LLVM/Clang..."
cd llvm-project
if [ ! -f "llvm/CMakeLists.txt" ]; then
    echo "ERROR: llvm/CMakeLists.txt not found in llvm-project"
    exit 1
fi
mkdir -p build
cd build
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra" \
    -DLLVM_ENABLE_RUNTIMES="compiler-rt;libcxx;libcxxabi;libunwind" \
    -DLLVM_TARGETS_TO_BUILD="X86;AArch64;WebAssembly" \
    -DLLVM_ENABLE_BINDINGS=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_ENABLE_LTO=Thin \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DLLVM_LIBDIR_SUFFIX=64 \
    -DLLVM_INSTALL_TOOLCHAIN_ONLY=ON \
    .. 2>&1 | tail -30
echo "  ✓ Configured"

# Build and install
echo "[3/4] Building (this may take 1-2 hours)..."
NPROCS=$(nproc)
echo "  Using ${NPROCS} parallel jobs"
ninja -j${NPROCS} 2>&1 | tail -50
echo "  ✓ Built"

echo "[4/4] Installing..."
ninja install 2>&1 | tail -20
cd /

# Verify installation
echo ""
echo "=========================================="
echo "LLVM/Clang Installation Complete"
echo "=========================================="
"${PREFIX}/bin/clang" --version | head -1
echo ""
echo "Location: ${PREFIX}"
echo "Binary: ${PREFIX}/bin/clang"
echo ""
