#!/usr/bin/python3

import os
import sys
import requests
import hashlib

def download_file(url, filename):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

def decompress_file(filename):
    decompressed_filename = filename.replace('.zst', '.txt')
    os.system(f'zstd -d {filename} -o {decompressed_filename}')
    return decompressed_filename

def download_and_decompress_trace(cluster_num, data_type):
    cluster_num_str = f"{int(cluster_num):03d}"
    base_urls = {
        "full": "https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/open_source",
        "sample": "https://raw.githubusercontent.com/twitter/cache-trace/master/samples/2020Mar"
    }
    file_url = f"{base_urls[data_type]}/cluster{cluster_num_str}"
    filename = f"cluster{cluster_num_str}"

    if data_type == "full":
        filename += ".zst"

    download_file(file_url, filename)

    if data_type == "full":
        filename = decompress_file(filename)
    
    print(f"Downloaded to {filename}")

    return filename

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
        print("Usage: python3 script.py <cluster_num> [sample/full]")
        sys.exit(1)

    cluster_num = sys.argv[1]
    data_type = sys.argv[2] if len(sys.argv) > 2 else "sample"
    trace_file = download_and_decompress_trace(cluster_num, data_type)

    output_file = f"{cluster_num}-MyYCSB"
    convert_file(trace_file, output_file)
    print(f"Converted data written to {output_file}")
