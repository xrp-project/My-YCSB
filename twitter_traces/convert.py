#!/usr/bin/python3

import os
import sys
import hashlib

def decompress_file(filename):
    base, ext = os.path.splitext(filename)
    decompressed_filename = f"{base}_decompressed.txt"
    os.system(f'zstd -d {filename} -o {decompressed_filename}')
    return decompressed_filename

def hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()[:63]

def convert_line(line):
    fields = line.strip().split(',')

    operation = fields[5].upper()
    key = hash_key(fields[1])

    if operation in ['SET', 'ADD', 'REPLACE', 'CAS', 'APPEND', 'PREPEND', 'DELETE', 'INCR', 'DECR']:
        return f"UPDATE,{key}"
    elif operation in ['GET', 'GETS']:
        return f"READ,{key}"

    return None

def convert_file(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            converted_line = convert_line(line)
            if converted_line:
                outfile.write(converted_line + '\n')

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 script.py <path_to_zst_file>")
        sys.exit(1)

    zst_file = sys.argv[1]
    trace_file = decompress_file(zst_file)

    base = os.path.splitext(zst_file)[0]
    output_file = f"{base}-MyYCSB.txt"
    convert_file(trace_file, output_file)
    print(f"Converted data written to {output_file}")

