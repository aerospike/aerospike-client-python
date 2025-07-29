# Build Instructions

## Prerequisites

### Dependencies

This project requires the Aerospike C client library. Make sure you have the following installed:

- Python 3.7+
- Aerospike C client library
- OpenSSL (for SSL/TLS support)
- Build tools (gcc, make, etc.)

### macOS Installation

```bash
# Install Aerospike C client using Homebrew
brew install aerospike/tap/aerospike-client-c

# Or build from source:
git submodule update --init --recursive
cd aerospike-client-c
make
```

### Ubuntu/Debian Installation

```bash
# Install dependencies
sudo apt-get update
sudo apt-get install build-essential libssl-dev

# Download and install Aerospike C client
wget https://download.aerospike.com/artifacts/aerospike-client-c/latest/aerospike-client-c-devel-latest.ubuntu20.04.x86_64.tgz
tar -xzf aerospike-client-c-devel-latest.ubuntu20.04.x86_64.tgz
sudo dpkg -i aerospike-client-c-devel-*.deb
```

## Building

### Standard Build

```bash
# Clean previous builds
python3 setup.py clean

# Install dependencies  
python3 -m pip install -r requirements.txt

# Build wheel package
python3 -m build

# Install the built package
pip install dist/aerospike-*.whl
```

### Development Build

For development with debugging symbols:

```bash
export DEBUG=1
python3 setup.py build_ext --inplace
python3 setup.py install
```

## Platform-Specific Instructions

### macOS with Homebrew

```bash
export SSL_LIB_PATH="$(brew --prefix openssl@3)/lib/"
export CPATH="$(brew --prefix openssl@3)/include/"  
export STATIC_SSL=1

python3 setup.py clean
python3 -m pip install -r requirements.txt
python3 -m build
```

### CentOS/RHEL

```bash
# Install dependencies
sudo yum groupinstall "Development Tools"
sudo yum install openssl-devel

# Set environment variables
export CPATH=/usr/include/openssl
export LIBRARY_PATH=/usr/lib64

# Build
python3 setup.py clean
python3 -m pip install -r requirements.txt
python3 -m build
```

### Alpine Linux

```bash
# Install dependencies
apk add --no-cache \
    build-base \
    linux-headers \
    openssl-dev \
    python3-dev

# Build
python3 setup.py clean  
python3 -m pip install -r requirements.txt
python3 -m build
```

## Docker Build

### Using Dockerfile

```bash
docker build -t aerospike-python-client .
docker run -it aerospike-python-client
```

### Multi-stage Build

For production deployments:

```dockerfile
FROM python:3.9-alpine as builder
RUN apk add --no-cache build-base linux-headers openssl-dev
COPY . /app
WORKDIR /app
RUN python3 -m build

FROM python:3.9-alpine
RUN apk add --no-cache openssl
COPY --from=builder /app/dist/*.whl /tmp/
RUN pip install /tmp/*.whl
```

## Build Troubleshooting

### Common Issues

1. **OpenSSL not found**
   ```bash
   export SSL_LIB_PATH="/path/to/openssl/lib"
   export CPATH="/path/to/openssl/include"
   ```

2. **Aerospike C client not found**
   ```bash
   export AEROSPIKE_C_HOME="/path/to/aerospike-client-c"
   ```

3. **Linking errors on Linux**
   ```bash
   export LDFLAGS="-Wl,-rpath,/usr/local/lib"
   ```

### Debug Build

Enable debug mode for detailed build information:

```bash
export DEBUG=1
export VERBOSE=1
python3 setup.py build_ext --inplace --debug
```

## Static Linking

For static linking with OpenSSL:

```bash
export STATIC_SSL=1
export SSL_LIB_PATH="/path/to/openssl/lib"
python3 -m build
```

## Cross-compilation

### ARM64 on x86_64

```bash
export CC=aarch64-linux-gnu-gcc
export CXX=aarch64-linux-gnu-g++
export AR=aarch64-linux-gnu-ar
export STRIP=aarch64-linux-gnu-strip

python3 setup.py build_ext
python3 -m build
```

## Testing

### Unit Tests

```bash
python3 -m pytest test/
```

### Integration Tests

Requires running Aerospike server:

```bash
# Start Aerospike server (Docker)
docker run -d --name aerospike -p 3000:3000 aerospike/aerospike-server

# Run integration tests
python3 test_batch_read_numpy.py
```

## Performance Testing

### Benchmark Tests

```bash
# Compare with original aerospike client
python3 test_performance_500keys_aerospike.py
python3 test_performance_500keys_fast_aerospike.py
```

## Testing with Virtual Environment

### Build and Test with venv (Recommended)

Using virtual environment for building and testing:

```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2. Set build environment (macOS)
export SSL_LIB_PATH="$(brew --prefix openssl@3)/lib/"
export CPATH="$(brew --prefix openssl@3)/include/"
export STATIC_SSL=1

# 3. Install dependencies and build
python3 setup.py clean
python3 -m pip install -r requirements.txt
python3 -m build

# 4. Install built wheel
pip install dist/aerospike-*.whl

# 5. Run tests
python test_batch_read_numpy.py
```

### numpy rec array functionality test

Test the batch_read function that returns numpy structured array (rec array):

```bash
# Basic test (no Aerospike server required)
python test_batch_read_numpy.py

# Real server connection test (requires Aerospike server on localhost:3000)
python test_batch_read_real.py

# Usage examples
```
