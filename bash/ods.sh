#! /usr/bin/bash

source /root/.bashrc
# shellcheck disable=SC2164
cd /root/github/soros/ts

python trade_cal.py & python stock_basic.py & python stock_company.py & python daily.py
python daily_basic.py & python money_flow.py & python adj_factor.py
python stock_holder_num.py & python hk_hold.py & python moneyflow_hsgt.py
python ths_index.py & python ths_daily.py & python ths_member.py

#python fina_main_biz.py
