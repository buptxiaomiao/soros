#! /usr/bin/bash

source /root/.bashrc
# shellcheck disable=SC2164
cd /root/github/soros/ts

python trade_cal.py
python stock_basic.py
python stock_company.py

python daily.py
python daily_basic.py
python money_flow.py
python adj_factor.py
