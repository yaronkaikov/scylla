#!/bin/bash
#
# Build GCC from latest upstream source
# Usage: ./build-gcc-latest.sh [VERSION] [PREFIX]
#   VERSION: GCC version (default: 15.2.0)
#   PREFIX: Installation prefix (default: /usr/local/gcc)
#

set -e

GCC_VERSION="${1:-15.2.0}"
PREFIX="${2:-/usr/local/gcc}"

echo "=========================================="
echo "Building GCC ${GCC_VERSION}"
echo "=========================================="
echo "Installation prefix: ${PREFIX}"
echo ""

BUILD_DIR="/tmp/gcc-build-${GCC_VERSION}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Download GCC from upstream
echo "[1/4] Downloading GCC ${GCC_VERSION}..."
if ! wget -q "https://ftpmirror.gnu.org/gcc/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.xz" -O "gcc-${GCC_VERSION}.tar.xz" 2>/dev/null; then
    echo "  Primary mirror failed, trying alternative..."
    wget -q "https://gcc.gnu.org/pub/gcc/releases/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.xz" -O "gcc-${GCC_VERSION}.tar.xz"
fi
echo "  ✓ Downloaded"

# Extract and prepare
echo "[2/4] Extracting and preparing GCC..."
tar xf "gcc-${GCC_VERSION}.tar.xz"
cd "gcc-${GCC_VERSION}"
echo "  Running contrib/download_prerequisites..."
contrib/download_prerequisites > /dev/null 2>&1 || true
cd ..
mkdir -p gcc-build
echo "  ✓ Prepared"

# Configure
echo "[3/4] Configuring GCC..."
cd gcc-build
../gcc-${GCC_VERSION}/configure \
    --prefix="${PREFIX}" \
    --enable-languages=c,c++ \
    --enable-host-shared \
    --disable-bootstrap \
    --disable-libsanitizer \
    --disable-libvtv \
    --disable-multilib \
    --disable-nls \
    CFLAGS="-O2" \
    CXXFLAGS="-O2" 2>&1 | tail -20
echo "  ✓ Configured"

# Build and install
echo "[4/4] Building and installing (this may take 1-2 hours)..."
NPROCS=$(nproc)
echo "  Using ${NPROCS} parallel jobs"
make -j${NPROCS} 2>&1 | tail -20
make install 2>&1 | tail -20
cd /

# Verify installation
echo ""
echo "=========================================="
echo "GCC Installation Complete"
echo "=========================================="
"${PREFIX}/bin/gcc" --version | head -1
echo ""
echo "Location: ${PREFIX}"
echo "Binary: ${PREFIX}/bin/gcc"
echo ""
