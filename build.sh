#!/bin/bash
# build.sh - Package the project for AWS Lambda deployment

set -e

LAMBDA_ZIP="lambda_package.zip"
LAMBDA_DIR="lambda_build"
PYTHON_VERSION="python3.12"

# Clean up any previous builds
rm -rf "$LAMBDA_DIR" "$LAMBDA_ZIP"
mkdir "$LAMBDA_DIR"

# Copy project source files (edit as needed for your entrypoint and modules)
cp *.py "$LAMBDA_DIR"/
cp .env "$LAMBDA_DIR"/ || true

# Install dependencies to the build directory
pip install --platform manylinux2014_x86_64 --target "$LAMBDA_DIR" --implementation cp --python-version 3.12 --only-binary=:all: --upgrade -r requirements.txt

# Create the deployment package
cd "$LAMBDA_DIR"
zip -r "../$LAMBDA_ZIP" .
cd ..

echo "Build complete. Deployment package: $LAMBDA_ZIP"
