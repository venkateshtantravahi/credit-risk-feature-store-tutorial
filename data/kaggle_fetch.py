# data/kaggle_fetch.py
"""
This Script provides a mechanism for downloading, extracting, and preparing
dataset from Kaggle.

Key Functionalities include:
- Authentication with the kaggle API using .json file (~/.kaggle/kaggle.json).
- Downloading and extracting data from Kaggle.
- Selecting the most relevant csv file from the dataset.
- Inferring a database-compatible schema from csv file.
- Storing the raw data and schema metadata in a structured directory layout.
"""

import os
import json
import zipfile
import logging
from pathlib import Path
from typing import List, Dict

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

# Configuration logging to provide detailed output helps in debugging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

"""
Constants 
Defined directory paths using the script's location as base. This makes
it portable and ensures that paths are resolved correctly regardless of
where the script is executed from.
"""
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "raw"
METADATA_DIR = BASE_DIR / "metadata"

# Custom Exceptions
# To provide more specific and informative error messages for granular error handling.
class KaggleFetchError(Exception):
    """Base exception for errors in this script."""
    pass

class KaggleApiError(KaggleFetchError):
    """Raised when there is an issue with the kaggle API."""
    pass

class FileProcessingError(KaggleFetchError):
    """Raised when there is an issue with file processing."""
    pass

# Core Functions

def setup_directories():
    """
    Creates the necessary directories for storing raw data and metadata
    if they do not already exist.
    :return:
    """
    try:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        METADATA_DIR.mkdir(parents=True, exist_ok=True)
        logging.info(f"Data directories ensured to exist at {RAW_DIR} and {METADATA_DIR}")
    except OSError as e:
        logging.error(f"Failed to setup directories: {e}")
        raise FileProcessingError(f"Could not setup directories: {e}") from e


def get_kaggle_api() -> KaggleApi:
    """
    Authenticates with kaggle API and returns a KaggleApi object.
    Handles authentication errors.
    :return:
        KaggleApi: An authenticated KaggleApi object.
    :raises:
        KaggleApiError: If authentication fails.
    """
    try:
        api = KaggleApi()
        api.authenticate()
        logging.info(f"Kaggle API authenticated")
        return api
    except Exception as e:
        logging.error(f"Kaggle API authentication failed. Ensure kaggle.json is configured")
        raise KaggleApiError("Could not authenticate") from e


def download_dataset(api: KaggleApi, dataset_slug: str):
    """
    Downloads a dataset from Kaggle to the raw data directory.
    :param api: The authenticated KaggleApi object.
    :param dataset_slug: The slug of the dataset to download (e.g., 'user/dataset-name').
    :return:

    :raises:
    KaggleApiError: If authentication fails.
    """
    logging.info(f"Downloading dataset {dataset_slug}")
    try:
        api.dataset_download_files(
            dataset_slug,
            path=str(RAW_DIR),
            force=False,
            quiet=False
        )
        logging.info(f"Dataset {dataset_slug} downloaded successfully.")
    except Exception as e:
        logging.error(f"Failed to download dataset {dataset_slug}: {e}")
        raise KaggleApiError("Could not download dataset") from e



def extract_dataset(zip_path: Path) -> List[Path]:
    """
    Extracts a dataset from Kaggle to the raw data directory.
    :param zip_path: The path of the zip file to extract.
    :return: A list of paths extracted files.
    :raises:
    FileProcessingError: If there is an issue with file processing.
    """
    if not zip_path.exists():
        raise FileProcessingError(f"File {zip_path} does not exist")

    logging.info(f"Extracting dataset from {zip_path.name}")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(RAW_DIR)
            extracted_files = [RAW_DIR / name for name in zf.namelist()]
            logging.info(f"Extracted {len(extracted_files)} files.")
            return extracted_files
    except zipfile.BadZipFile as e:
        logging.error(f"Failed to extract dataset from {zip_path.name} bad zip file: {e}")
        raise FileProcessingError(f"Failed to extract dataset from {zip_path.name} bad zip file: {e}")
    except Exception as e:
        logging.error("An error occurred when extracting dataset from {zip_path.name}: {e}")
        raise FileProcessingError(f"Failed to extract dataset from {zip_path.name}: {e}")


def extract_all_archives_recursive(root_dir: Path) -> list[Path]:
    """
    Recursively extract ALL .zip files found under root_dir.
    Returns a flat list of all files (including those in subfolders).
    """
    all_files = set()
    # First pass: collect files
    for p in root_dir.rglob("*"):
        if p.is_file():
            all_files.add(p)

    # Keep extracting while there are zips
    changed = True
    while changed:
        changed = False
        zips = [p for p in list(all_files) if p.suffix.lower() == ".zip"]
        for z in zips:
            try:
                extracted = extract_dataset(z)
                all_files.remove(z)  # keep or remove as you prefer
                for e in extracted:
                    if (z.parent / e.name).exists():
                        all_files.add(z.parent / e.name)
                    else:
                        all_files.add(e)
                changed = True
            except zipfile.BadZipFile:
                logging.warning(f"Skipping bad zip: {z}")

    # Refresh a full listing after all extractions
    all_files = set(p for p in root_dir.rglob("*") if p.is_file())
    logging.info(f"[extract] Total files after recursive extraction: {len(all_files)}")
    # Log a sample so we can see what’s inside
    sample = list(all_files)[:40]
    logging.info(f"[extract] Sample files: {[p.name for p in sample]}")
    return sorted(all_files)


def find_csv_files(extracted_files: List[Path]) -> List[Path]:
    """
    Finds all CSV files from a list of extracted files.
    :param extracted_files: A list of paths extracted files.
    :return:
    List[Path]: List of paths to CSV files.
    :raises:
    FileNotFoundError: If no CSV files were found.
    """
    tabular = []
    for p in extracted_files:
        suf = p.suffix.lower()
        if suf == ".csv":
            tabular.append(p)
        elif suf == ".gz" and p.name.lower().endswith(".csv.gz"):
            tabular.append(p)
        elif suf in (".xlsx", ".xls"):
            tabular.append(p)
    if not tabular:
        raise FileNotFoundError("No CSV/CSV.GZ files were found in the extracted dataset.")
    logging.info(f"Found {len(tabular)} tabular files: {[p.name for p in tabular]}")
    return sorted(tabular, key=lambda p: p.stat().st_size, reverse=True)


def infer_and_write_schema(csv_path: Path, sample_rows: int = 10000) -> Path:
    """
    Infers SQL schema from a CSV file and writes it to a JSON file in the
    metadata directory.
    :param csv_path: The path of the CSV file to infer.
    :param sample_rows: The number of rows to sample.
    :return: The path of the JSON file with the schema written.

    :raises: FileProcessingError: If there is an issue with file processing.
    """
    file_size_mb = csv_path.stat().st_size / 1e6
    logging.info(f"Processing CSV file {csv_path.name} with size {file_size_mb} MB")

    compression = "infer" if csv_path.suffix.lower() in {".gz"} or csv_path.name.lower().endswith(".csv.gz") else None

    logging.info(f"Inferring schema from {csv_path.name} by sampling {sample_rows} rows")
    try:
        df = pd.read_csv(csv_path, nrows=sample_rows, low_memory=False, compression=compression)
        schema: Dict[str, str] = {}
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DOUBLE PRECISION"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP"
            else:
                # a better solution might involve profiling the columns to set a
                # reasonable varchar length, where a text is a safe default.
                sql_type = "TEXT"
            schema[col] = sql_type
        logging.info(f"Schema inference complete for {csv_path.name}. Found {len(schema)} columns.")

        schema_file_path = METADATA_DIR / f"{csv_path.stem}_schema.json"
        table_name = csv_path.stem.lower().replace("-", "_")
        schema_content = {
            "source_csv": str(csv_path),
            "table_name": table_name,
            "schema": schema,
        }

        with open(schema_file_path, "w") as schema_file:
            json.dump(schema_content, schema_file, indent=4)
        logging.info(f"Schema written to {schema_file_path}")
        return schema_file_path

    except FileNotFoundError as e:
        logging.error(f"CSV file not found for schema inference {csv_path}.")
        raise FileProcessingError(f"CSV file not found for schema inference {csv_path}.")
    except Exception as e:
        logging.error(f"An unknown error occurred when inferring schema from {csv_path}: {e}")
        raise FileProcessingError(f"Unknown error occurred when inferring schema from {csv_path}: {e}")


def main():
    """
    Main execution function to orchestrate the download and preparation
    of kaggle dataset.
    :return:
    """
    load_dotenv()
    setup_directories()

    try:
        kaggle_dataset = os.getenv("KAGGLE_DATASET", "wordsforthewise/lending-club")

        api = get_kaggle_api()
        download_dataset(api, kaggle_dataset)

        dataset_slug_name = kaggle_dataset.split("/")[-1]
        zip_path = RAW_DIR / f"{dataset_slug_name}.zip"

        if not zip_path.exists():
            raise FileProcessingError(f"Expected zip not found: {zip_path}")
        logging.info(f"Extracting files from {zip_path.name}...")
        extract_dataset(zip_path)
        logging.info(f"Top-level extraction done. Now expanding nested archives (if any)...")

        all_files = extract_all_archives_recursive(RAW_DIR)

        csv_paths = find_csv_files(all_files)

        for csv_path in csv_paths:
            infer_and_write_schema(csv_path, sample_rows=10000)
            logging.info(f"Successfully processed {csv_path.name}.")

        logging.info("All done.")
        logging.info(f"All {len(csv_paths)} CSV files were processed.")
        logging.info(f"Raw data is available at {RAW_DIR}.")
        logging.info(f"Schema metadata is stored in {METADATA_DIR}.")

    except (KaggleFetchError, FileProcessingError, FileNotFoundError) as e:
        logging.error(f"A critical error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected and unhandled error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()