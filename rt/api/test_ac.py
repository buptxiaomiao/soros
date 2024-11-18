# coding: utf-8

import time
import os
from datetime import datetime
import tushare as ts
TOKEN = os.getenv("TOKEN")
FMT = "%m-%d %H:%M:%S"


ts.set_token(TOKEN)
df = ts.realtime_tick('000001.SZ')

print(df)

print(df.count)
