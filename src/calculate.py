#!/usr/bin/env python

import sqlite3
import itertools
import pytz
from datetime import datetime

HOURS_TO_EXCLUDE = 6
CONSECUTIVE = 1


def load_data():
    conn = sqlite3.connect("prices.db")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT timestamp, price FROM prices ORDER BY price DESC, timestamp ASC"
    )

    results = {}
    for row in cursor:
        results[row[0]] = row[1]

    conn.close()
    return results


results = load_data()

# Get all keys from the results dictionary
timestamps = list(results.keys())

combinations = list(itertools.combinations(timestamps, HOURS_TO_EXCLUDE))

# Calculate the sum of dictionary values for each combination
sums = [(combo, sum(results[ts] for ts in combo)) for combo in combinations]

filtered_combinations = []
for combo, values in sums:
    combo = sorted(combo)
    valid = True
    for i in range(len(combo) - CONSECUTIVE):
        if (combo[i + CONSECUTIVE] - combo[i]) <= (CONSECUTIVE + 1) * 3600:
            valid = False
            break
    if valid:
        filtered_combinations.append((combo, values))

# Sort combinations by their sums in descending order
filtered_combinations.sort(key=lambda x: x[1], reverse=True)

# Find the highest sum
highest_sum = filtered_combinations[0][1]

# Determine the threshold for sums within 5% of the highest sum
threshold = highest_sum * 0.95

# Filter combinations where the sum is within 5% of the highest sum
top_combinations = [combo for combo in filtered_combinations if combo[1] >= threshold]

# Sort top_combinations by the total difference between timestamps in descending order
top_combinations.sort(
    key=lambda x: sum(x[0][i + 1] - x[0][i] for i in range(len(x[0]) - 1)), reverse=True
)

for combo, values in top_combinations:
    print(f"Combination: {combo}, Sum: {values}")
    for ts in combo:
        print(
            f"  {datetime.fromtimestamp(ts, pytz.timezone('Europe/Tallinn')).isoformat()} = {results[ts]}"
        )
