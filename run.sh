#!/usr/bin/bash

cd /home/iff/elekter
source .venv/bin/activate

src/update-prices.py
src/calculate.py
