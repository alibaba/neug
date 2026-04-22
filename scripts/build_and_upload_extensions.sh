#!/bin/bash
set -e

# Script to build extensions and upload to OSS
# Usage: build_and_upload_extensions.sh [OPTIONS]
# Options:
#   --extensions EXTENSIONS    Semicolon-separated list of extensions (default: json)
#   --version VERSION         Version tag (default: dev)
#   --platform PLATFORM       Platform identifier (default: linux-x86_64)
#   --workspace WORKSPACE     Workspace directory (default: current directory)
#   --python-version VER      Python dir under /opt/python (default: cp310-cp310, or env PYTHON_VERSION)
#   --skip-build              Skip building, only package and upload
#   --skip-upload             Skip uploading to OSS
#   --help                    Show this help message

# Default values
EXTENSIONS="json"
VERSION="0.1.1"
PLATFORM="linux_x86_64"
WORKSPACE_DIR="$(pwd)"
SKIP_BUILD=false
SKIP_UPLOAD=false
# Python version dir under /opt/python (e.g. cp310-cp310), overridable via env PYTHON_VERSION
PYTHON_VERSION="${PYTHON_VERSION:-cp310-cp310}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --extensions)
            EXTENSIONS="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --workspace)
            WORKSPACE_DIR="$2"
            shift 2
            ;;
        --python-version)
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-upload)
            SKIP_UPLOAD=true
            shift
            ;;
        --help)
            cat << EOF
Usage: $0 [OPTIONS]

Build extensions and upload to OSS.

Options:
    --extensions EXTENSIONS    Semicolon-separated list of extensions (default: json)
    --version VERSION         Version tag (default: dev)
    --platform PLATFORM       Platform identifier (default: linux-x86_64)
    --workspace WORKSPACE     Workspace directory (default: current directory)
    --python-version VER      Python dir under /opt/python (default: cp310-cp310, or env PYTHON_VERSION)
    --skip-build              Skip building, only package and upload
    --skip-upload             Skip uploading to OSS
    --help                    Show this help message

Environment variables required for OSS upload:
    OSS_ACCESS_KEY_ID         OSS access key ID
    OSS_ACCESS_KEY_SECRET    OSS access key secret
    OSS_ENDPOINT             OSS endpoint
    OSS_BUCKET_NAME          OSS bucket name

Examples:
    # Build json extension and upload
    $0 --extensions json --version v0.1.1 --platform linux-x86_64

    # Build multiple extensions
    $0 --extensions "json;csv" --version v0.1.1

    # Only package and upload (skip build)
    $0 --extensions json --skip-build
EOF
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Build and Upload Extensions"
echo "=========================================="
echo "Extensions: ${EXTENSIONS}"
echo "Version: ${VERSION}"
echo "Platform: ${PLATFORM}"
echo "Workspace: ${WORKSPACE_DIR}"
echo "=========================================="

# Change to workspace directory
cd "${WORKSPACE_DIR}"

# Set Python 3 environment: use /opt/python/$PYTHON_VERSION/bin if present
if [ -d /opt/python ] && [ -d "/opt/python/${PYTHON_VERSION}/bin" ]; then
    export PATH="/opt/python/${PYTHON_VERSION}/bin:${PATH}"
    echo "Using Python from /opt/python/${PYTHON_VERSION}/bin"
fi

# Step 1: Build extensions
if [ "${SKIP_BUILD}" = false ]; then
    echo ""
    echo "Step 1: Building extensions..."
    
    # Source environment if exists (use $HOME for both Linux and macOS)
    if [ -f "${HOME}/.neug_env" ]; then
        . "${HOME}/.neug_env"
    fi
    
    # Set build environment variables
    # nproc on Linux, sysctl -n hw.ncpu on macOS
    NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    export BUILD_TYPE=RELEASE
    export CMAKE_BUILD_PARALLEL_LEVEL=${NPROC}
    export BUILD_EXTENSIONS="${EXTENSIONS}"
    
    # Build
    cd tools/python_bind
    make clean || true
    make requirements || true
    make build
    
    echo "Build completed successfully!"
else
    echo ""
    echo "Step 1: Skipping build (--skip-build specified)"
fi

# Step 2: Package extension files and compute checksums
echo ""
echo "Step 2: Packaging extension files and computing checksums..."

cd "${WORKSPACE_DIR}/tools/python_bind"
EXT_DIR="build"
PACKAGE_DIR="/tmp/extension_packages"
mkdir -p ${PACKAGE_DIR}

# Find all extension files
EXT_FILES=""
for ext in $(echo ${EXTENSIONS} | tr ';' ' '); do
    EXT_PATH="${EXT_DIR}"
    if [ -d "${EXT_PATH}" ]; then
        EXT_FILE=$(find "${EXT_PATH}" -name "lib${ext}.neug_extension" | head -1)
        if [ -n "${EXT_FILE}" ] && [ -f "${EXT_FILE}" ]; then
            EXT_FILES="${EXT_FILES} ${EXT_FILE}"
            echo "Found extension file: ${EXT_FILE}"
        else
            echo "Warning: Extension file not found for ${ext} in ${EXT_PATH}"
        fi
    else
        echo "Warning: Extension directory not found: ${EXT_PATH}"
    fi
done

if [ -z "${EXT_FILES}" ]; then
    echo "Error: No extension files found"
    exit 1
fi

# Copy extension files with original names and strip them
for ext_file in ${EXT_FILES}; do
    original_name=$(basename ${ext_file})
    cp ${ext_file} ${PACKAGE_DIR}/${original_name}
    
    # Strip debug symbols to reduce file size
    if command -v strip &> /dev/null; then
        strip ${PACKAGE_DIR}/${original_name} || echo "Warning: strip failed for ${original_name}"
    fi
    echo "Packaged and stripped: ${original_name}"
    
    # Compute SHA256 checksum
    # Generate checksum filename: {original_name}.sha256
    # e.g., libjson.neug_extension.sha256
    checksum_file="${PACKAGE_DIR}/${original_name}.sha256"
    
    # Use compute_sha256.py script
    if [ -f "${WORKSPACE_DIR}/scripts/compute_sha256.py" ]; then
        python3 "${WORKSPACE_DIR}/scripts/compute_sha256.py" ${PACKAGE_DIR}/${original_name} ${checksum_file}
        echo "Computed checksum: ${checksum_file}"
    else
        echo "Error: compute_sha256.py not found at ${WORKSPACE_DIR}/scripts/compute_sha256.py"
        exit 1
    fi
done

# List all packages
echo ""
echo "Extension packages:"
ls -lh ${PACKAGE_DIR}/

# Step 3: Upload to OSS
if [ "${SKIP_UPLOAD}" = false ]; then
    echo ""
    echo "Step 3: Uploading to OSS..."
    
    # Check OSS credentials
    if [ -z "${OSS_ACCESS_KEY_ID}" ] || [ -z "${OSS_ACCESS_KEY_SECRET}" ] || [ -z "${OSS_ENDPOINT}" ] || [ -z "${OSS_BUCKET_NAME}" ]; then
        echo "Warning: OSS credentials not configured. Skipping upload."
        echo "Required environment variables: OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_BUCKET_NAME"
        exit 0
    fi
    
    # Install oss2 if not available (urllib3<2 for OpenSSL 1.0.2 compatibility, e.g. RHEL 7)
    if ! python3 -c "import oss2" 2>/dev/null; then
        echo "Installing oss2..."
        python3 -m pip  install 'urllib3<2' oss2
    fi
    
    # Upload using Python script
    python3 << EOF
import os
import sys
import oss2
import glob

access_key_id = os.environ.get('OSS_ACCESS_KEY_ID')
access_key_secret = os.environ.get('OSS_ACCESS_KEY_SECRET')
endpoint = os.environ.get('OSS_ENDPOINT')
bucket_name = os.environ.get('OSS_BUCKET_NAME')
package_dir = '${PACKAGE_DIR}'
version = '${VERSION}'
platform = '${PLATFORM}'

# Initialize OSS client
auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(auth, endpoint, bucket_name)

# Upload each extension package and its checksum file
extension_files = glob.glob(os.path.join(package_dir, '*.neug_extension'))
checksum_files = glob.glob(os.path.join(package_dir, '*.sha256'))

if not extension_files:
    print("No extension files found to upload")
    sys.exit(1)

# Upload extension files
for ext_file in extension_files:
    filename = os.path.basename(ext_file)
    # Extract extension name from filename (e.g., libjson.neug_extension -> json)
    # Remove 'lib' prefix and '.neug_extension' suffix
    if filename.startswith('lib') and filename.endswith('.neug_extension'):
        ext_name = filename[3:-len('.neug_extension')]
    else:
        # Fallback: try to extract from filename
        ext_name = filename.replace('.neug_extension', '').replace('lib', '')
    # Construct OSS path: neug/extensions/v{version}/{platform}/{extension_name}/{filename}
    # Example: neug/extensions/v0.1.1/linux-x86_64/json/libjson.neug_extension
    oss_path = f"neug/extensions/v{version}/{platform}/{ext_name}/{filename}"
    
    print(f"Uploading {filename} to oss://{bucket_name}/{oss_path}")
    
    try:
        result = bucket.put_object_from_file(oss_path, ext_file)
        print(f"Successfully uploaded {filename}")
        print(f"  ETag: {result.etag}")
        print(f"  Request ID: {result.request_id}")
    except Exception as e:
        print(f"Failed to upload {filename}: {e}")
        sys.exit(1)

# Upload checksum files
for checksum_file in checksum_files:
    filename = os.path.basename(checksum_file)
    # Extract extension name from checksum filename (e.g., libjson.neug_extension.sha256 -> json)
    if filename.startswith('lib') and filename.endswith('.neug_extension.sha256'):
        ext_name = filename[3:-len('.neug_extension.sha256')]
    else:
        ext_name = filename.replace('.neug_extension.sha256', '').replace('lib', '')
    # Construct OSS path for checksum file
    oss_path = f"neug/extensions/v{version}/{platform}/{ext_name}/{filename}"
    
    print(f"Uploading {filename} to oss://{bucket_name}/{oss_path}")
    
    try:
        result = bucket.put_object_from_file(oss_path, checksum_file)
        print(f"Successfully uploaded {filename}")
        print(f"  ETag: {result.etag}")
        print(f"  Request ID: {result.request_id}")
    except Exception as e:
        print(f"Failed to upload {filename}: {e}")
        sys.exit(1)

print(f"\nAll extensions and checksums uploaded successfully!")
print(f"OSS base URL: https://{bucket_name}.{endpoint}/neug/extensions/v{version}/{platform}/")
EOF
    echo "Upload completed successfully!"
else
    echo ""
    echo "Step 3: Skipping upload (--skip-upload specified)"
    echo "Packages are available at: ${PACKAGE_DIR}"
fi

echo ""
echo "=========================================="
echo "Build and upload completed successfully!"
echo "=========================================="
