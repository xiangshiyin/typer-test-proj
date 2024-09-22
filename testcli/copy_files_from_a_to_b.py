"""
Steps to follow:
1. List all the files in the GCS folder
2. Split the files into multiple folders based upon the prefix of the file. The prefix is usually a unix timestamp, we just need to calculate the mode of the prefix and create a folder with that name.
3. Move the files to the respective folders concurrently using concurrent.futures.ThreadPoolExecutor
"""
import os
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import threading
import time

storage_client = storage.Client()

def copy_file(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name, progress_bar):
    """Copy a file to the bucket and updates the progress bar."""
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_blob_name)

    # Copy the blob from the source bucket to the destination bucket
    destination_blob.rewrite(source_blob)
    with progress_bar_lock:
        progress_bar.update(1)
    # print(f"File {source_blob_name} moved to {destination_blob_name}")

def get_files_in_folder(bucket_name, directory):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=directory)
    print(f"Getting files with the prefix gs://{bucket_name}/{directory}")
    start_ts = time.time()
    files = [blob.name for blob in blobs if not blob.name.endswith('/')]
    print(f"Got {len(files)} files in {time.time() - start_ts} seconds")
    return files

def main(source_bucket_name, source_directory, destination_bucket_name, destination_directory, max_workers=1):
    global progress_bar_lock
    progress_bar_lock = threading.Lock()

    files_to_copy = get_files_in_folder(source_bucket_name, source_directory)
    progress_bar = tqdm(
        total=len(files_to_copy),
        desc="Moving files",
        unit="file",
        bar_format="{l_bar}{bar}| [{n_fmt}/{total_fmt}] [{percentage:.0f}% Done] [ETA: {remaining}]",
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for source_blob_name in files_to_copy:
            destination_directory_full_name = f"{destination_directory}/{int(os.path.basename(source_blob_name).split('-')[0]) % 10}/"
            destination_blob_name = os.path.join(destination_directory_full_name, os.path.basename(source_blob_name))
            futures.append(
                executor.submit(
                    copy_file,
                    source_bucket_name,
                    source_blob_name,
                    destination_bucket_name,
                    destination_blob_name,
                    progress_bar,
                )
            )
        
        # Wait for all the futures to complete
        for future in futures:
            future.result()
    progress_bar.close()

if __name__ == "__main__":
    start_ts = time.time()
    source_bucket_name = "<source_bucket_name>"
    source_directory = "<source_directory_prefix>"
    destination_bucket_name = "<destination_bucket_name>"
    destination_directory = "<destination_directory_name>"
    main(source_bucket_name, source_directory, destination_bucket_name, destination_directory, max_workers=100)
    print(f"Total time taken: {time.time() - start_ts} seconds")


