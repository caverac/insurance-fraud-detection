#!/bin/bash
# EMR Bootstrap script to install Python 3.13 and the fraud_detection package
# This runs on ALL nodes (master and workers)

set -e

SCRIPTS_BUCKET=$1

echo "=== Installing Python 3.13 on $(hostname) ==="

# Install dependencies for building Python
sudo yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel readline-devel sqlite-devel xz-devel

# Download and install Python 3.13
cd /tmp
wget -q https://www.python.org/ftp/python/3.13.0/Python-3.13.0.tgz
tar xzf Python-3.13.0.tgz
cd Python-3.13.0

# Configure without optimizations (faster build, avoids test failures)
./configure --prefix=/usr/local/python3.13
make -j $(nproc)
sudo make altinstall

# Create symlinks
sudo ln -sf /usr/local/python3.13/bin/python3.13 /usr/bin/python3.13
sudo ln -sf /usr/local/python3.13/bin/pip3.13 /usr/bin/pip3.13

echo "Python 3.13 installed at /usr/bin/python3.13"
/usr/bin/python3.13 --version

echo "=== Installing fraud_detection package ==="

# Install the wheel using Python 3.13 (system-wide with sudo so YARN containers can access it)
aws s3 cp "s3://${SCRIPTS_BUCKET}/dist/" /tmp/dist/ --recursive --include "*.whl"
sudo /usr/local/python3.13/bin/pip3.13 install /tmp/dist/*.whl

# Verify installation
echo "Installed packages:"
/usr/local/python3.13/bin/pip3.13 list

echo "=== Bootstrap complete on $(hostname) ==="
