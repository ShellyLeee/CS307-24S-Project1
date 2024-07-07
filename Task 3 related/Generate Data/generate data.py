#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 11:49:46 2024
This is a file to increase the amount of data.

@author: shellyli
"""

import json
import random
from datetime import datetime

# 读取原始 JSON 文件
with open('Data_v1/ride.json', 'r') as file:
    original_data = json.load(file)

# 定义新的数据列表
new_data = []

# 根据原始数据生成新数据
for _ in range(200000):  # 生成20万条新数据
    new_entry = {}  # 新数据条目
    for key, value in original_data[0].items():  # 假设原始数据是列表中包含一个字典
        # 根据原始数据的类型生成新值
        if key in ['user', 'start_station', 'end_station']:
            new_entry[key] = random.choice([entry[key] for entry in original_data])
        elif key in ['start_time', 'end_time']:
            # 生成随机时间戳（2020年至今）
            random_timestamp = random.randint(1577836800, int(datetime.now().timestamp()))
            new_entry[key] = datetime.fromtimestamp(random_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        elif key == 'price':
            new_entry[key] = random.randint(1, 20)  # 生成随机价格（示例）
        else:
            new_entry[key] = value  # 其他类型保持不变
    new_data.append(new_entry)

# 将新数据写入新的 JSON 文件
with open('Data_v1/new_ride.json', 'w') as file:
    json.dump(new_data, file, indent=4)



