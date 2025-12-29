#!/bin/bash -ue

trap 'echo "error $? in $0 line $LINENO"' ERR

case "${GCC_BUILD}" in
    "")
        echo "GCC_BUILD not specified"
        exit 1
        ;;
    "SKIP")
        echo "GCC_BUILD: ${GCC_BUILD}"
        exit 0
        ;;
    "INSTALL")
        if [[ -z "${GCC_VERSION}" ]]; then
            echo "GCC_VERSION not specified"
            exit 1
        fi
        ;;
    *)
        echo "Invalid mode specified on GCC_BUILD: ${GCC_BUILD}"
        exit 1
        ;;
 esac

ARCH="$(arch)"
SCYLLA_DIR=/mnt
GCC_ROOT_DIR="${SCYLLA_DIR}/gcc_build"
GCC_TARBALL="${GCC_ROOT_DIR}/gcc-${GCC_VERSION}.tar.xz"
GCC_SRC_DIR="${GCC_ROOT_DIR}/gcc-${GCC_VERSION}"
GCC_BUILD_DIR="${GCC_ROOT_DIR}/build-${ARCH}"

mkdir -p "${GCC_ROOT_DIR}"

# Check if tarball was pre-downloaded (mounted from host)
if [[ ! -f "${GCC_TARBALL}" ]]; then
    echo "[gcc] ERROR: GCC tarball not found at ${GCC_TARBALL}"
    echo "[gcc] Please download it first on the host and it will be mounted into the container"
    echo "[gcc] Download from one of:"
    echo "[gcc]   https://ftp.gnu.org/gnu/gcc/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.xz"
    echo "[gcc]   https://mirrors.kernel.org/gnu/gcc/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.xz"
    echo "[gcc]   https://sourceware.org/pub/gcc/releases/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.xz"
    exit 1
fi

rm -rf "${GCC_SRC_DIR}" "${GCC_BUILD_DIR}"
tar -C "${GCC_ROOT_DIR}" -xf "${GCC_TARBALL}"

cd "${GCC_SRC_DIR}"
./contrib/download_prerequisites

mkdir -p "${GCC_BUILD_DIR}"
cd "${GCC_BUILD_DIR}"
../gcc-${GCC_VERSION}/configure \
    --prefix=/usr/local \
    --enable-languages=c,c++ \
    --disable-multilib \
    --enable-lto \
    --enable-bootstrap \
    --with-system-zlib

make -j"$(nproc)" bootstrap
make -j"$(nproc)" install
