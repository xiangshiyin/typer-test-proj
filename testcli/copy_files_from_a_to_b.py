import os
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, Future
from tqdm import tqdm
import threading
import time
from typing import List

# Initialize the Google Cloud Storage client
storage_client = storage.Client()


def copy_file(
    source_bucket_name: str,
    source_blob_name: str,
    destination_bucket_name: str,
    destination_blob_name: str,
    progress_bar: tqdm,
) -> None:
    """Copy a file from the source bucket to the destination bucket and update the progress bar.

    Args:
        source_bucket_name (str): Name of the source bucket.
        source_blob_name (str): Name of the source blob.
        destination_bucket_name (str): Name of the destination bucket.
        destination_blob_name (str): Name of the destination blob.
        progress_bar (tqdm): Progress bar to update.
    """
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_blob_name)

    # Copy the blob from the source bucket to the destination bucket
    destination_blob.rewrite(source_blob)
    with progress_bar_lock:
        progress_bar.update(1)


def get_files_in_folder(bucket_name: str, directory: str) -> List[str]:
    """Retrieve a list of files in a specified directory within a bucket.

    Args:
        bucket_name (str): Name of the bucket.
        directory (str): Directory prefix to search for files.

    Returns:
        List[str]: List of file names in the specified directory.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=directory)
    print(f"Getting files with the prefix gs://{bucket_name}/{directory}")
    start_ts = time.time()
    files = [blob.name for blob in blobs if not blob.name.endswith("/")]
    print(f"Got {len(files)} files in {time.time() - start_ts} seconds")
    return files


def copy_a2b(
    source_bucket_name: str,
    source_directory_prefix: str,
    destination_bucket_name: str,
    destination_directory: str,
    destination_slices: int = 1,
    max_workers: int = 1,
) -> None:
    """Copy files from a source bucket to a destination bucket with multithreading support.

    Args:
        source_bucket_name (str): Name of the source bucket.
        source_directory_prefix (str): Directory prefix in the source bucket.
        destination_bucket_name (str): Name of the destination bucket.
        destination_directory (str): Directory in the destination bucket.
        destination_slices (int, optional): Number of slices for the destination directory. Defaults to 1.
        max_workers (int, optional): Maximum number of worker threads. Defaults to 1.
    """
    global progress_bar_lock
    progress_bar_lock = threading.Lock()

    files_to_copy = get_files_in_folder(source_bucket_name, source_directory_prefix)
    progress_bar = tqdm(
        total=len(files_to_copy),
        desc="Moving files",
        unit="file",
        bar_format="{l_bar}{bar}| [{n_fmt}/{total_fmt}] [{percentage:.0f}% Done] [ETA: {remaining}]",
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: List[Future] = []
        for source_blob_name in files_to_copy:
            file_name = os.path.basename(source_blob_name)
            destination_directory_full = os.path.join(
                destination_directory,
                str(int(file_name.split("-")[0]) % destination_slices),
            )
            destination_blob_name = os.path.join(destination_directory_full, file_name)
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

    # Print the total elapsed time
    print(f"Total time used: {progress_bar.format_dict['elapsed']:.2f} seconds")
