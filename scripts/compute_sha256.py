#!/usr/bin/env python3
"""
Compute SHA256 hash of a binary file and write to output file.

This module provides a Python implementation of computeFileSHA256
that can be used independently without lib_neug dependencies.

Usage:
    python3 compute_sha256.py <input_binary_file> <output_file>
"""

import hashlib
import sys
from pathlib import Path


def compute_file_sha256(input_file: str, output_file: str) -> bool:
    """
    Compute SHA256 hash of a binary file and write to output file.

    Args:
        input_file: Path to the input binary file to hash.
        output_file: Path to the output file where hash will be written.

    Returns:
        True if successful, False on error.
    """
    try:
        sha256_hash = hashlib.sha256()
        buffer_size = 8192  # 8KB buffer, same as C++ implementation

        # Read input binary file in chunks
        with open(input_file, "rb") as f:
            while True:
                chunk = f.read(buffer_size)
                if not chunk:
                    break
                sha256_hash.update(chunk)

        # Get hexadecimal hash
        hash_value = sha256_hash.hexdigest()

        # Write hash to output file
        with open(output_file, "w") as f:
            f.write(hash_value)

        return True
    except IOError as e:
        print(f"Error: Cannot open or read file: {input_file}", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error: Unexpected error while computing hash: {e}", file=sys.stderr)
        return False


def main():
    """Command-line interface for computing file SHA256."""
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input_binary_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Check if input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)

    success = compute_file_sha256(input_file, output_file)
    if not success:
        sys.exit(1)

    print(f"SHA256 hash computed and written to: {output_file}")
    sys.exit(0)


if __name__ == "__main__":
    main()
