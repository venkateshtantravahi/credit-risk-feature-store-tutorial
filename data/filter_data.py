# filter_data.py

import os
import csv
import gzip
import json
import re
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Constants
# It's a good practice to handle the case where PROJECT_ROOT might not be set
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT"))
RAW_DIR = PROJECT_ROOT / "data" / "raw"
FILTERED_DIR = PROJECT_ROOT / "data" / "raw_filtered"
METADATA_DIR = PROJECT_ROOT / "data" / "metadata"


class FileProcessingError(Exception):
    """Custom exception for file processing issues."""

    pass


def ensure_directory_exists(path: Path):
    """Ensure the specified directory exists."""
    path.mkdir(parents=True, exist_ok=True)


def parse_date_to_year(date_str: str) -> Optional[int]:
    """
    Parses various date string formats and returns the year.
    Handles 'Mon-YYYY' and 'YYYY-MM-DD' formats.
    Returns None if the format is invalid.
    """
    if not isinstance(date_str, str) or not date_str:
        return None

    # Handle 'Mon-YYYY' format (e.g., 'Aug-2003')
    match = re.search(r"(\d{4})", date_str)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None

    # Handle 'YYYY-MM-DD' format (e.g., '2007-05-26')
    try:
        # We can just get the first 4 characters
        year_str = date_str.split("-")[0]
        return int(year_str)
    except (ValueError, IndexError):
        return None


def process_file(schema_file_path: Path, start_year: int, end_year: int):
    """
    Reads a source CSV file specified in a schema JSON, filters rows by date,
    and writes the result to a new filtered CSV file. Includes sanity checks.
    """
    try:
        if not schema_file_path.exists():
            raise FileProcessingError(f"Schema file not found: {schema_file_path}")

        with open(schema_file_path, "r") as f:
            schema_data = json.load(f)

        source_csv_path = Path(schema_data["source_csv"])
        if not source_csv_path.exists():
            raise FileProcessingError(f"Source CSV file not found: {source_csv_path}")

        table_name = schema_data["table_name"]
        filtered_csv_path = FILTERED_DIR / f"{table_name}_filtered.csv"

        logging.info(f"Starting to process '{source_csv_path.name}'...")

        date_col = "issue_d" if "accepted" in table_name else "Application Date"

        open_func = gzip.open if source_csv_path.suffix == ".gz" else open

        rows_written = 0
        with open_func(
            source_csv_path, "rt", encoding="utf-8", errors="ignore"
        ) as infile:
            reader = csv.reader(infile)
            try:
                header = next(reader)
                if not header:
                    raise FileProcessingError(
                        f"File {source_csv_path.name} is empty or has a blank header."
                    )
            except StopIteration:
                raise FileProcessingError(f"File {source_csv_path.name} is empty.")

            if header and list(next(reader, None)) == header:
                logging.warning(
                    f"Duplicate header detected in {source_csv_path.name}. Skipping the second header."
                )

            # Find the index of the date column
            try:
                date_col_index = header.index(date_col)
                logging.info(
                    f"Filtering on column '{date_col}' (index {date_col_index})..."
                )
            except ValueError:
                raise FileProcessingError(
                    f"Date column '{date_col}' not found in {source_csv_path.name}. Skipping file."
                )

            # Re-open the file to handle potential duplicate header check from a new stream
            infile.seek(0)
            reader = csv.reader(infile)
            next(reader)  # Skip the first header row again

            with open(filtered_csv_path, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)

                for i, row in enumerate(reader):
                    # Skip the second header if it exists
                    if i == 0 and len(row) == len(header) and row == header:
                        continue

                    if not row or len(row) <= date_col_index:
                        continue

                    date_str = row[date_col_index]
                    year = parse_date_to_year(date_str)

                    if year and start_year <= year <= end_year:
                        writer.writerow(row)
                        rows_written += 1

        logging.info(
            f"Filtering completed for '{source_csv_path.name}'. Wrote {rows_written} rows to {filtered_csv_path.name}"
        )

    except FileProcessingError as e:
        logging.error(f"A file processing error occurred: {e}")
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while processing {schema_file_path.name}: {e}"
        )


def main():
    """Main function to orchestrate the filtering process."""
    ensure_directory_exists(FILTERED_DIR)

    accepted_schema_path = METADATA_DIR / "accepted_2007_to_2018Q4_schema.json"
    rejected_schema_path = METADATA_DIR / "rejected_2007_to_2018Q4_schema.json"

    process_file(accepted_schema_path, 2007, 2011)
    process_file(rejected_schema_path, 2007, 2011)


if __name__ == "__main__":
    main()
