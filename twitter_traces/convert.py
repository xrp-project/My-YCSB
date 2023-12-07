#!/usr/bin/python3

import os
import sys
import hashlib

def decompress_file(filename):
    base, ext = os.path.splitext(filename)
    decompressed_filename = f"{base}_decompressed.txt"
    os.system(f'zstd -d -T16 {filename} -o {decompressed_filename}')
    return decompressed_filename

def hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()[:15]

def convert_line(line):
    fields = line.strip().split(',')

    operation = fields[5].upper()
    key = hash_key(fields[1])

    if operation in ['ADD', 'REPLACE', 'CAS', 'APPEND', 'PREPEND', 'DELETE', 'INCR', 'DECR']:
        return f"UPD,{key}"
    elif operation in ['SET']:
        return f"SET,{key}"
    elif operation in ['GET', 'GETS']:
        return f"GET,{key}"

    return None

def convert_file(input_file, base):
    full = f"{base}_full.txt"
    unique = f"{base}_unique.txt"

    seen_keys = set()
    line_count = 0
    print_interval = 1000 * 1000 * 10  # Adjust this value to change the

    with open(input_file, 'r') as infile, \
        open(full, 'w') as full_trace_file, \
        open(unique, 'w') as unique_trace_file:

        for i, line in enumerate(infile):
            converted_line = convert_line(line)
            if converted_line is None:
                continue

            _, key = converted_line.strip().split(',')

            full_trace_file.write(converted_line)

            if key not in seen_keys:
                unique_trace_file.write(converted_line)
                seen_keys.add(key)

            line_count += 1
            if line_count % print_interval == 0:
                print(f"Lines read: {line_count}", end="\r")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 convert.py <path_to_zst_file>")
        sys.exit(1)

    zst_file = sys.argv[1]
    trace_file = decompress_file(zst_file)

    base = os.path.splitext(zst_file)[0]
    convert_file(trace_file, base)

    print(f"Converted data written to {output_file}")

