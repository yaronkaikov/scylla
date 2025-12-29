#!/bin/bash -ue

trap 'echo "error $? in $0 line $LINENO"' ERR

case "${LLVM_BUILD:-SKIP}" in
    "SKIP")
        echo "LLVM_BUILD: SKIP"
        exit 0
        ;;
    "INSTALL")
        if [[ -z "${LLVM_CLANG_TAG:-}" ]]; then
            echo "LLVM_CLANG_TAG not specified"
            exit 1
        fi
        ;;
    *)
        echo "Invalid mode specified on LLVM_BUILD: ${LLVM_BUILD}"
        exit 1
        ;;
esac

ARCH="$(arch)"
SCYLLA_DIR=/mnt
LLVM_ROOT_DIR="${SCYLLA_DIR}/llvm_build"
LLVM_TARBALL="${LLVM_ROOT_DIR}/llvm-project-${LLVM_CLANG_TAG}.src.tar.xz"
LLVM_SRC_DIR="${LLVM_ROOT_DIR}/llvm-project-${LLVM_CLANG_TAG}.src"
LLVM_BUILD_DIR="${LLVM_ROOT_DIR}/build-${ARCH}"

mkdir -p "${LLVM_ROOT_DIR}"

# Check if tarball was pre-downloaded (mounted from host)
if [[ ! -f "${LLVM_TARBALL}" ]]; then
    echo "[llvm] ERROR: LLVM tarball not found at ${LLVM_TARBALL}"
    echo "[llvm] Please download it first on the host and it will be mounted into the container"
    echo "[llvm] Download from:"
    echo "[llvm]   https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_CLANG_TAG}/llvm-project-${LLVM_CLANG_TAG}.src.tar.xz"
    exit 1
fi

rm -rf "${LLVM_SRC_DIR}" "${LLVM_BUILD_DIR}"
echo "[llvm] Extracting LLVM source..."
tar -C "${LLVM_ROOT_DIR}" -xf "${LLVM_TARBALL}"

mkdir -p "${LLVM_BUILD_DIR}"
cd "${LLVM_BUILD_DIR}"

echo "[llvm] Configuring LLVM/clang ${LLVM_CLANG_TAG}..."
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lld" \
    -DLLVM_ENABLE_RUNTIMES="compiler-rt" \
    -DLLVM_TARGETS_TO_BUILD="X86;AArch64;WebAssembly" \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_INCLUDE_DOCS=OFF \
    -DLLVM_BUILD_DOCS=OFF \
    -DLLVM_ENABLE_BINDINGS=OFF \
    -DLLVM_ENABLE_OCAMLDOC=OFF \
    -DLLVM_ENABLE_Z3_SOLVER=OFF \
    -DLLVM_PARALLEL_LINK_JOBS=2 \
    -DLLVM_BUILD_LLVM_DYLIB=ON \
    -DLLVM_LINK_LLVM_DYLIB=ON \
    "${LLVM_SRC_DIR}/llvm"

echo "[llvm] Building LLVM/clang (this may take a while)..."
ninja -j"$(nproc)"

echo "[llvm] Installing LLVM/clang..."
ninja install

echo "[llvm] Cleaning up build directory..."
rm -rf "${LLVM_BUILD_DIR}" "${LLVM_SRC_DIR}"

echo "[llvm] LLVM/clang ${LLVM_CLANG_TAG} installed successfully"
/usr/local/bin/clang --version
