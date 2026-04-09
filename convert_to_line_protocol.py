"""
Fetch data.json from multiple directories, convert to InfluxDB line protocol,
and write the output to a file for sending to AWS Lambda -> InfluxDB.

InfluxDB Line Protocol format:
  measurement,tag1=val1,tag2=val2 field1=value1,field2=value2 timestamp

Example output:
  ci_pipeline,repo=frontend,status=success,env=prod,branch=main duration=80i 1712600000000000000
"""

import json
import os
import sys
import time
import glob


MEASUREMENT = "ci_pipeline"
# Tags: indexed metadata for filtering
TAG_KEYS = ["repo", "status", "env", "branch"]
# Fields: actual metric values
FIELD_KEYS = ["time"]
# Mapping from JSON key -> line protocol field name
FIELD_RENAME = {"time": "duration"}

DATA_DIR = os.environ.get("DATA_DIR", "data")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "line_protocol.txt")


def escape_tag_value(value):
    """Escape special characters in tag values per line protocol spec."""
    return str(value).replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def to_line_protocol(record, timestamp_ns):
    """Convert a single JSON record to InfluxDB line protocol.

    Args:
        record: dict with keys like repo, status, env, branch, time
        timestamp_ns: nanosecond Unix timestamp

    Returns:
        A line protocol string, e.g.:
        ci_pipeline,repo=frontend,status=success,env=prod,branch=main duration=80i 1712600000000000000
    """
    # Build tags: measurement,tag1=val1,tag2=val2
    tags = ",".join(
        f"{key}={escape_tag_value(record[key])}"
        for key in TAG_KEYS
        if key in record
    )

    # Build fields: field1=value1,field2=value2
    fields_parts = []
    for key in FIELD_KEYS:
        if key in record:
            field_name = FIELD_RENAME.get(key, key)
            value = record[key]
            if isinstance(value, int):
                fields_parts.append(f"{field_name}={value}i")
            elif isinstance(value, float):
                fields_parts.append(f"{field_name}={value}")
            else:
                # String fields must be quoted
                fields_parts.append(f'{field_name}="{value}"')
    fields = ",".join(fields_parts)

    return f"{MEASUREMENT},{tags} {fields} {timestamp_ns}"


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


def convert_all(base_dir):
    """Fetch all data.json files, convert to line protocol lines."""
    data_files = find_data_files(base_dir)

    if not data_files:
        print(f"No data.json files found under '{base_dir}'")
        sys.exit(1)

    # Use current time as timestamp (nanoseconds)
    timestamp_ns = int(time.time() * 1e9)

    all_lines = []

    for filepath in data_files:
        dir_name = os.path.basename(os.path.dirname(filepath))
        print(f"  [+] Reading: {filepath}  (source: {dir_name})")

        records = load_json_file(filepath)
        print(f"      Found {len(records)} records")

        for record in records:
            line = to_line_protocol(record, timestamp_ns)
            all_lines.append(line)

    return all_lines


def main():
    base_dir = DATA_DIR
    print(f"=== Fetching data.json from directories under '{base_dir}' ===\n")

    lines = convert_all(base_dir)

    print(f"\n=== Converted {len(lines)} records to InfluxDB line protocol ===\n")

    # Preview first 5 lines
    print("Sample output:")
    for line in lines[:5]:
        print(f"  {line}")
    if len(lines) > 5:
        print(f"  ... and {len(lines) - 5} more lines\n")

    # Write to output file
    with open(OUTPUT_FILE, "w") as f:
        f.write("\n".join(lines))

    print(f"Written to: {OUTPUT_FILE}")
    print(f"Total lines: {len(lines)}")


if __name__ == "__main__":
    main()
