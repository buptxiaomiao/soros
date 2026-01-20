# coding: utf-8

import os
import sys
import tushare as ts
from utils.setting import TOKEN
from env import TOKEN as ENV_TOKEN

token = TOKEN or ENV_TOKEN

ts.set_token(token)
pro = ts.pro_api()

