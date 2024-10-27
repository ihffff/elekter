#!/usr/bin/env python

import sqlite3
import itertools
import pytz
from datetime import date, time, datetime, timedelta

HOURS_TO_EXCLUDE = 6
CONSECUTIVE = 2


def load_data(start, end):
    conn = sqlite3.connect("prices.db")
    cursor = conn.cursor()

    # Convert the timezone-aware datetime objects to Unix timestamps
    start_timestamp = int(start.timestamp())
    end_timestamp = int(end.timestamp())

    cursor.execute(
        "SELECT timestamp, price FROM prices WHERE timestamp >= ? AND timestamp < ? ORDER BY price DESC, timestamp ASC",
        (start_timestamp, end_timestamp),
    )

    results = {}
    for row in cursor:
        results[row[0]] = float(row[1])

    conn.close()
    return results


def create_table_if_not_exists(cursor):
    # Create table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS high_prices (
            timestamp INTEGER PRIMARY KEY,
            price REAL NOT NULL
        )
        """
    )


timezone = pytz.timezone("Europe/Tallinn")
start = timezone.localize(datetime.combine(date.today() + timedelta(days=0), time.min))
end = start + timedelta(days=1)
results = load_data(start, end)

# Get all keys from the results dictionary
timestamps = list(results.keys())

combinations = list(itertools.combinations(timestamps, HOURS_TO_EXCLUDE))

# Calculate the sum of dictionary values for each combination
totals = [(combo, sum(results[ts] for ts in combo)) for combo in combinations]

# Filter out combinations with consecutive timestamps
filtered_combinations = []
for combo, total in totals:
    combo = sorted(combo)
    valid = True

    for i in range(len(combo) - CONSECUTIVE):
        if (combo[i + CONSECUTIVE] - combo[i]) <= (CONSECUTIVE + 1) * 3600:
            valid = False
            break

    if valid:
        filtered_combinations.append((combo, total))

# Sort combinations by their sums in descending order
filtered_combinations.sort(key=lambda x: x[1], reverse=True)

# Find the highest sum
highest = filtered_combinations[0][1]

# Determine the threshold for sums within 5% of the highest sum
threshold = highest * 0.95

# Filter combinations where the sum is within 5% of the highest sum
top_combinations = [combo for combo in filtered_combinations if combo[1] >= threshold]

# Sort top_combinations by the total difference between timestamps in descending order
top_combinations.sort(
    key=lambda x: sum(x[0][i + 1] - x[0][i] for i in range(len(x[0]) - 1)), reverse=True
)

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("prices.db")
cursor = conn.cursor()

create_table_if_not_exists(cursor)

# Insert rows into the table
cursor.executemany(
    """
    INSERT OR REPLACE INTO high_prices (timestamp, price) VALUES (?, ?)
    """,
    [(x, results[x]) for x in top_combinations[0][0]],
)

# Commit the transaction and close the connection
conn.commit()
conn.close()

exit(0)

print(f"Highest is {highest:.2f}\n")

first = True
for combo, total in top_combinations:
    print(
        f"Combination: {combo}, Sum: {total:7.2f}, Difference: {(highest - total):6.2f} ({((highest - total) * 100 / highest):.1f}%)"
    )

    if first:
        first = False
        for ts in combo:
            print(
                f"    {datetime.fromtimestamp(ts, timezone).time()} = {results[ts]:.2f}"
            )
