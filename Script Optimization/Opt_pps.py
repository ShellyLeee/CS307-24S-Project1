#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 27 23:00:11 2024

@author: shellyli
"""

import json
import time
import psycopg2


# 读取 JSON 文件
with open('Data_v1/cards.json') as cards_f:
    card_d = json.load(cards_f)

with open('Data_v1/passenger.json') as passenger_f:
    passenger_d = json.load(passenger_f)
    
with open('Data_v1/ride.json') as ride_f:
    ride_d = json.load(ride_f)
    
with open('Data_v1/stations.json') as stations_f:
    stations_d = json.load(stations_f)
    
    
# 连接到 PostgreSQL 数据库
conn = psycopg2.connect(
    dbname="project1",
    user="checker",
    password="123456",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()


'''将数据插入数据库'''

# key mapping
key_mapping_p = {
    "id_number": "id",
    "name": "name",
    "phone_number": "phone_number",
    "gender": "gender",
    "district": "district"
}

key_mapping_s = {
    "english_name":"English_name",
    "district": "district",
    "intro": "intro",
    "chinese_name":"Chinese_name"
    
}

key_mapping_passenger = {
    "user": "passenger_id",
    "start_station": "start_station",
    "end_station": "end_station",
    "start_time": "start_time",
    "end_time": "end_time",
    "price" : "price"
}

key_mapping_code = {
    "user": "card_code",
    "start_station": "start_station",
    "end_station": "end_station",
    "start_time": "start_time",
    "end_time": "end_time",
    "price" : "price"
}

# 使用预编译语句
card_insert_query = "INSERT INTO card ({}) VALUES ({})".format(
    ','.join(card_d[0].keys()), ','.join(['%s'] * len(card_d[0]))
)
passenger_insert_query = "INSERT INTO passenger({}) VALUES ({})".format(
    ','.join(key_mapping_p.values()), ','.join(['%s'] * len(key_mapping_p))
)


pr_insert_query = "INSERT INTO passenger_ride({}) VALUES ({})".format(
    ','.join(key_mapping_passenger.values()),
    ','.join(['%s'] * len(key_mapping_passenger))
)

cr_insert_query = "INSERT INTO card_ride({}) VALUES ({})".format(
    ','.join(key_mapping_code.values()),
    ','.join(['%s'] * len(key_mapping_code))
)

s_insert_query = "INSERT INTO station({}) VALUES ({})".format(
    ','.join(key_mapping_s.values()),
    ','.join(['%s'] * len(key_mapping_s))
)

# card
start_time_1 = time.time()
for item in card_d:
    cursor.execute(card_insert_query, list(item.values()))

end_time_1 = time.time()
print(f"Table card data insertion time: {end_time_1 - start_time_1} seconds")

# passenger
start_time_2 = time.time()

for item in passenger_d:
    column_values = [item.get(key, None) for key in key_mapping_p.keys()]
    cursor.execute(passenger_insert_query, column_values)

end_time_2 = time.time()
print(f"Table passenger data insertion time: {end_time_2 - start_time_2} seconds")

# station
start_time_4 = time.time() 
tmp_dic = {}
for key, value in stations_d.items():
    tmp_dic['english_name'] = (key,)
    for key, value in value.items():
        tmp_dic[key] = value
    column_values = [tmp_dic.get(key, None) for key in key_mapping_s.keys()]
    cursor.execute(s_insert_query, column_values)

end_time_4 = time.time()
total_time_4 = end_time_4 - start_time_4
print(f"Table station data insertion time: {total_time_4} seconds")

# ride
start_time_6 = time.time() 
for item in ride_d: # item: dic
    if len(item['user']) == 18:
        column_values = [item.get(key, None) for key in key_mapping_passenger.keys()]
        cursor.execute(pr_insert_query, column_values)
        
    elif len(item['user']) == 9:
        column_values = [item.get(key, None) for key in key_mapping_code.keys()]
        cursor.execute(cr_insert_query, column_values)

end_time_6 = time.time()
total_time_6 = end_time_6 - start_time_6
print(f"Table ride data insertion time: {total_time_6} seconds")



# 提交更改并关闭连接
conn.commit()
cursor.close()
conn.close()
