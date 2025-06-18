#!/bin/bash

# Script to test all PUBLIC_NEXT_DAY_OFFER_ENERGY_SPARSE_* files
# Stops on first failure

set -e  # Exit on any error
set -o pipefail  # Exit if any command in a pipeline fails

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
total_files=0
processed_files=0
failed_files=0

echo "Starting test of PUBLIC_NEXT_DAY_OFFER_ENERGY_SPARSE_* files..."
echo "============================================================="

# Check if the binary exists
if [[ ! -f "./target/release/test_processing" ]]; then
    echo -e "${RED}Error: ./target/release/test_processing not found${NC}"
    echo "Please build the project first: cargo build --release"
    exit 1
fi

# Get list of files matching the pattern
files=(assets/zips/PUBLIC_NEXT_DAY_OFFER_ENERGY_SPARSE_*)

# Check if any files match
if [[ ${#files[@]} -eq 1 && ! -f "${files[0]}" ]]; then
    echo -e "${RED}Error: No files found matching pattern 'assets/zips/PUBLIC_NEXT_DAY_OFFER_ENERGY_SPARSE_*'${NC}"
    exit 1
fi

total_files=${#files[@]}
echo "Found $total_files files to process"
echo ""

# Process each file
for file in "${files[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo -e "${YELLOW}Warning: File not found: $file${NC}"
        continue
    fi
    
    processed_files=$((processed_files + 1))
    echo -e "${YELLOW}[$processed_files/$total_files]${NC} Processing: $(basename "$file")"
    
    # Run the test_processing command
    if ./target/release/test_processing "$file"; then
        echo -e "${GREEN}✓ SUCCESS:${NC} $(basename "$file")"
        echo ""
    else
        exit_code=$?
        failed_files=$((failed_files + 1))
        echo -e "${RED}✗ FAILED:${NC} $(basename "$file") (exit code: $exit_code)"
        echo ""
        echo -e "${RED}Stopping on first failure as requested.${NC}"
        echo ""
        echo "Summary:"
        echo "  Total files found: $total_files"
        echo "  Successfully processed: $((processed_files - failed_files))"
        echo "  Failed: $failed_files"
        echo "  Remaining: $((total_files - processed_files))"
        exit $exit_code
    fi
done

# Success message
echo -e "${GREEN}🎉 All files processed successfully!${NC}"
echo ""
echo "Summary:"
echo "  Total files processed: $processed_files"
echo "  All tests passed: ✓"