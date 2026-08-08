"""
split_by_year.py
-----------------
One-time local script to split the Superstore dataset into separate
CSV files by Order Date year. This simulates data "arriving" in batches
over time -- each file becomes something we manually upload later to
Blob Storage, one at a time, to test our incremental pipeline.

This does NOT use PySpark -- it's a lightweight one-time prep step,
so plain Python + csv module is enough and avoids spinning up Spark
just to split a file.
"""

import csv
from collections import defaultdict
from datetime import datetime

INPUT_FILE = "superstoreSales_utf8.csv"

def main():
    rows_by_year = defaultdict(list)
    header = None

    with open(INPUT_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        date_idx = header.index("Order Date")

        for row in reader:
            date_str = row[date_idx]
            # Source dates look like "1/15/2012"
            year = datetime.strptime(date_str, "%m/%d/%Y").year
            rows_by_year[year].append(row)

    for year, rows in sorted(rows_by_year.items()):
        out_filename = f"orders_{year}.csv"
        with open(out_filename, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            writer.writerow(header)
            writer.writerows(rows)
        print(f"{out_filename}: {len(rows)} rows")

if __name__ == "__main__":
    main()