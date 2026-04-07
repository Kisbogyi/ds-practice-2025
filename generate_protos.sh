#!/usr/bin/env bash

set -u  # error on undefined variables

BASE_DIR="./utils/pb"

echo "Starting protobuf generation..."
echo "Base directory: $BASE_DIR"
echo

# Check base directory exists
if [ ! -d "$BASE_DIR" ]; then
  echo "ERROR: Directory $BASE_DIR does not exist"
  exit 1
fi

cd "$BASE_DIR" || exit 1

# Loop through each direct child directory
for dir in */; do
  echo "========================================"
  echo "Processing directory: $dir"
  echo "========================================"

  cd "$dir" || { echo "Failed to enter $dir"; continue; }

  # Remove old generated files (ignore errors if none exist)
  rm -f *.py *.pyi 2>/dev/null

  # Check if there are proto files
  proto_files=( *.proto )
  if [ ! -e "${proto_files[0]}" ]; then
    echo "No .proto files found in $dir, skipping..."
    cd ..
    continue
  fi

  echo "Running protoc..."

  # Run protoc and capture output
  if ! python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --mypy_out=. ./*.proto; then
    echo "❌ ERROR: protoc failed in $dir"
    cd ..
    continue
  fi
  
  if [ ! -f "__init__.py" ]; then
      touch __init__.py
      echo "Created __init__.py in $dir"
  fi

  echo "✅ Success in $dir"
  cd ..
  echo
done

echo "Done."
