"""
Fetch data.json from multiple directories, combine into a single JSON array,
and write the output to a file for sending to AWS Lambda.

The Lambda handles conversion to InfluxDB line protocol and writes to InfluxDB.
"""

import json
import os
import sys
import glob


DATA_DIR = os.environ.get("DATA_DIR", "data")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "combined_data.json")


def find_data_files(base_dir):
    """Recursively find all data.json files under the base directory."""
    pattern = os.path.join(base_dir, "**", "data.json")
    files = sorted(glob.glob(pattern, recursive=True))
    return files


def load_json_file(filepath):
    """Load and parse a JSON file. Returns a list of records."""
    with open(filepath, "r") as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = [data]
    return data


def collect_all(base_dir):
    """Fetch all data.json files, combine into a single list."""
    data_files = find_data_files(base_dir)

    if not data_files:
        print(f"No data.json files found under '{base_dir}'")
        sys.exit(1)

    all_records = []

    for filepath in data_files:
        dir_name = os.path.basename(os.path.dirname(filepath))
        print(f"  [+] Reading: {filepath}  (source: {dir_name})")

        records = load_json_file(filepath)
        print(f"      Found {len(records)} records")
        all_records.extend(records)

    return all_records


def main():
    base_dir = DATA_DIR
    print(f"=== Fetching data.json from directories under '{base_dir}' ===\n")

    records = collect_all(base_dir)

    print(f"\n=== Collected {len(records)} total records ===\n")

    # Preview first 3 records
    print("Sample records:")
    for rec in records[:3]:
        print(f"  {rec}")
    if len(records) > 3:
        print(f"  ... and {len(records) - 3} more\n")

    # Write combined JSON to output file
    with open(OUTPUT_FILE, "w") as f:
        json.dump(records, f)

    print(f"Written to: {OUTPUT_FILE}")
    print(f"Total records: {len(records)}")


if __name__ == "__main__":
    main()
