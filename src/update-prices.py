#!/usr/bin/env python

import requests
import csv
import sqlite3
from datetime import datetime, time, timedelta
import pytz
import locale

# Set locale and timezone
locale.setlocale(locale.LC_ALL, "et_EE")
tz = pytz.timezone("Europe/Tallinn")


def get_start_and_end():
    now = tz.localize(datetime.now())

    start = datetime.combine(now.date(), time.min)
    end = datetime.combine(now.date() + timedelta(days=3), time.max)

    return tz.localize(start).isoformat(), tz.localize(end).isoformat()


def create_table_if_not_exists(cursor):
    # Create table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            datetime TEXT PRIMARY KEY,
            price REAL
        )
        """
    )


# Main logic
url = "https://dashboard.elering.ee/api/nps/price/csv"
start, end = get_start_and_end()

headers = {
    "Accept": "text/csv",
}

params = {
    "start": start,
    "end": end,
    "fields": "ee",
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    lines = response.text.splitlines()
    rows = []
    csv_reader = csv.reader(lines, delimiter=";", quotechar='"')
    next(csv_reader)  # Skip the first line (header)

    for parts in csv_reader:
        if len(parts) >= 3:
            try:
                rows.append(
                    (
                        datetime.fromtimestamp(int(parts[0]), tz),
                        locale.atof(parts[2]),
                    )
                )
            except ValueError:
                continue

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect("prices.db")
    cursor = conn.cursor()

    create_table_if_not_exists(cursor)

    # Insert rows into the table
    cursor.executemany(
        """
        INSERT OR REPLACE INTO prices (datetime, price) VALUES (?, ?)
        """,
        rows,
    )

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()
else:
    print(f"Failed to download the CSV file. Status code: {response.status_code}")
