# -*- coding: utf-8 -*-
"""
This is a file to insert data.

@author: shellyli
"""
import json
import time
import psycopg2
import re

# 读取 JSON 文件
with open('Data_v1/cards.json') as cards_f:
    card_d = json.load(cards_f)

with open('Data_v1/passenger.json') as passenger_f:
    passenger_d = json.load(passenger_f)
    
with open('Data_v1/ride.json') as ride_f:
    ride_d = json.load(ride_f)
    
with open('Data_v1/stations.json') as stations_f:
    stations_d = json.load(stations_f)
    
with open('Data_v1/lines.json') as lines_f:
    lines_d = json.load(lines_f)
    
    
# 连接到 PostgreSQL 数据库
conn = psycopg2.connect(
    dbname="project1",
    user="checker",
    password="123456",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

start_time_total = time.time() 

'''将数据插入数据库'''

# card
start_time_1 = time.time() 
for item in card_d:
    keys = ','.join(item.keys())
    values = ','.join(['%s'] * len(item))
    insert_query = "INSERT INTO card ({}) VALUES ({})".format(keys, values) 
    cursor.execute(insert_query, list(item.values()))
    
end_time_1 = time.time()
total_time_1 = end_time_1 - start_time_1
print(f"Table card data insertion time: {total_time_1} seconds")

# passenger
start_time_2 = time.time() 
key_mapping = {
    "id_number": "id",
    "name": "name",
    "phone_number": "phone_number",
    "gender": "gender",
    "district": "district"
}

for item in passenger_d:
    insert_query = "INSERT INTO passenger({}) VALUES ({})".format(
        ','.join(key_mapping.values()),
        ','.join(['%s'] * len(key_mapping))
    )
    
    column_values = [item.get(key, None) for key in key_mapping.keys()]
    cursor.execute(insert_query, column_values)
    
end_time_2 = time.time()
total_time_2 = end_time_2 - start_time_2
print(f"Table passenger data insertion time: {total_time_2} seconds")


# line
start_time_3 = time.time() 
def insert_data(data, table_name, tmp_dic):
    key_mapping = {
        "name": "name",
        "start_time": "start_time",
        "end_time": "end_time",
        "intro": "intro",
        "mileage": "mileage",
        "color" : "color",
        "first_opening": "first_opening",
        "url": "url"
    }

    for key, value in data.items():
        tmp_dic['name'] = (key,)
        
        for key, value in value.items():
                tmp_dic[key] = value
                
        insert_query = "INSERT INTO {}({}) VALUES ({})".format(
            table_name,
            ','.join(key_mapping.values()),
            ','.join(['%s'] * len(key_mapping))
            )
        column_values = [tmp_dic.get(key, None) for key in key_mapping.keys()]
        cursor.execute(insert_query, column_values)

insert_data(lines_d, 'line', {})

end_time_3 = time.time()
total_time_3 = end_time_3 - start_time_3
print(f"Table line data insertion time: {total_time_3} seconds")

# station
start_time_4 = time.time() 
def insert_data(data, table_name, tmp_dic):
    key_mapping = {
        "english_name":"English_name",
        "district": "district",
        "intro": "intro",
        "chinese_name":"Chinese_name"
    
    }

    for key, value in data.items():
        tmp_dic['english_name'] = (key,)
        for key, value in value.items():
                tmp_dic[key] = value
                
        insert_query = "INSERT INTO {}({}) VALUES ({})".format(
            table_name,
            ','.join(key_mapping.values()),
            ','.join(['%s'] * len(key_mapping))
            )
        column_values = [tmp_dic.get(key, None) for key in key_mapping.keys()]
        cursor.execute(insert_query, column_values)

            
insert_data(stations_d, 'station', {})

end_time_4 = time.time()
total_time_4 = end_time_4 - start_time_4
print(f"Table station data insertion time: {total_time_4} seconds")

# line_detail
start_time_5 = time.time() 
tmp_dic = {}
for key, value in lines_d.items():
    tmp_dic[key] = value['stations']

for key, values in tmp_dic.items():
    for value in values:
        if "'" in value:
            value_correct = value.replace("'", "''")
        else:
            value_correct = value
        insert_query = "INSERT INTO line_detail(line_name, station_name) VALUES ('{}', '{}')".format(key, value_correct)
        cursor.execute(insert_query)

end_time_5 = time.time()
total_time_5 = end_time_5 - start_time_5
print(f"Table line_detail data insertion time: {total_time_5} seconds")


# ride
start_time_6 = time.time() 
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

for item in ride_d: # item: dic
    if len(item['user']) == 18:
        insert_query = "INSERT INTO passenger_ride({}) VALUES ({})".format(
            ','.join(key_mapping_passenger.values()),
            ','.join(['%s'] * len(key_mapping_passenger))
        )
    
        column_values = [item.get(key, None) for key in key_mapping_passenger.keys()]
        cursor.execute(insert_query, column_values)
        
    elif len(item['user']) == 9:
        insert_query = "INSERT INTO card_ride({}) VALUES ({})".format(
            ','.join(key_mapping_code.values()),
            ','.join(['%s'] * len(key_mapping_code))
        )
    
        column_values = [item.get(key, None) for key in key_mapping_code.keys()]
        cursor.execute(insert_query, column_values)

end_time_6 = time.time()
total_time_6 = end_time_6 - start_time_6
print(f"Table ride data insertion time: {total_time_6} seconds")


# exit
start_time_7 = time.time() 
for key, value in stations_d.items():
    tmp_dic = {}
    if "'" in key:
        key_correct = key.replace("'", "''")
    else:
        key_correct = key
    query = "SELECT * FROM station WHERE English_name = '{}'".format(key_correct)
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        station_id = row[0]
        
    tmp_dic[station_id] = []
    
    # {district:, bus_info:, out_info:, intro:, chinese_name:}
    for key, value in value.items():
            if key == 'out_info':
                for item in value: # item: dic{outt:xx, textt:xx}
                    tmp_dic[station_id].append(item['outt'].strip())
    
    for key, values in tmp_dic.items():
        for value in values:
            insert_query = "INSERT INTO exit(station_id, number) VALUES ('{}', '{}')".format(key, value)
            cursor.execute(insert_query)

end_time_7 = time.time()
total_time_7 = end_time_7 - start_time_7
print(f"Table exit data insertion time: {total_time_7} seconds")


# buildings
start_time_8 = time.time() 
for key, value in stations_d.items():
    tmp_dic = {}
    if "'" in key:
        key_correct = key.replace("'", "''")
    else:
        key_correct = key
    query = "SELECT * FROM station WHERE english_name = '{}'".format(key_correct)
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        station_id = row[0]
        
    for inner_key, inner_value in value.items():
        if inner_key == 'out_info':
            for item in inner_value:  # item: dic{outt:xx, textt:xx}
                query = "SELECT * FROM exit WHERE number = '{}' and station_id = {}".format(item['outt'].strip(), station_id)
                cursor.execute(query)
                result = cursor.fetchall()

                for row in result:
                    exit_id = row[0]
                    tmp_dic[exit_id] = item['textt'].split('、')

        else:
            continue

    for key, values in tmp_dic.items():
        for value in values:
            insert_query = "INSERT INTO buildings(exit_id, name) VALUES ('{}', '{}')".format(key, value)
            cursor.execute(insert_query)

end_time_8 = time.time()
total_time_8 = end_time_8 - start_time_8
print(f"Table buildings data insertion time: {total_time_8} seconds")


# bus_stop(Corrected in 5.9)

start_time_9 = time.time() 
def update_dict(dic, key, value):
    if key not in dic or (isinstance(dic[key], list) and value not in dic[key]):
        dic[key] = value
    elif isinstance(dic[key], list):
        dic[key].append(value)
    else:
        dic[key] = [dic[key], value]
    return dic

for key, value in stations_d.items():
    tmp_dic = {}
    if "'" in key:
        key_correct = key.replace("'", "''")
    else:
        key_correct = key
    query = "SELECT * FROM station WHERE english_name = '{}'".format(key_correct)
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        station_id = row[0]
        
    for inner_key, inner_value in value.items():
        if inner_key == 'bus_info':
            for item in inner_value:  # item: dic{busOutInfo:[{x:xx, y:yy}], chukou:xx}
                query = "SELECT * FROM exit WHERE number = '{}' and station_id = {}".format(item['chukou'].strip(), station_id)
                cursor.execute(query)
                result = cursor.fetchall()

                for row in result:
                    exit_id = row[0]
                    
                    for key_i, value_i in item.items():
                        if isinstance(value_i, list): 
                            for item_i in value_i:
                                bus_stop_name = item_i['busName']
                                update_dict(tmp_dic, exit_id, bus_stop_name)
                                    
        else:
            continue
        
        "print(tmp_dic)"
        
    for key, value in tmp_dic.items():
        if isinstance(value, list): 
            for item in value: 
                insert_query = "INSERT INTO bus_stop(exit_id, name) VALUES ('{}', '{}')".format(key, item)
                cursor.execute(insert_query)
        else:
            insert_query = "INSERT INTO bus_stop(exit_id, name) VALUES ('{}', '{}')".format(key, value)
            cursor.execute(insert_query)

end_time_9 = time.time()
total_time_9 = end_time_9 - start_time_9
print(f"Table bus_stop data insertion time: {total_time_9} seconds")


# bus_line(Corrected in 5.9)

start_time_10 = time.time() 
for key, value in stations_d.items():
    tmp_dic = {}
    if "'" in key:
        key_correct = key.replace("'", "''")
    else:
        key_correct = key
    query = "SELECT * FROM station WHERE english_name = '{}'".format(key_correct)
    cursor.execute(query)
    result = cursor.fetchall()

    for row in result:
        station_id = row[0]
        
    for inner_key, inner_value in value.items():
        if inner_key == 'bus_info':
            for item in inner_value:  # item: dic{busOutInfo:[{x:xx, y:yy}], chukou:xx}
                query = "SELECT * FROM exit WHERE number = '{}' and station_id = {}".format(item['chukou'].strip(), station_id)
                cursor.execute(query)
                result = cursor.fetchall()

                for row in result:
                    exit_id = row[0]
                    for key_i, value_i in item.items(): 
                        if isinstance(value_i, list): 
                            for item_s in value_i:
                                bus_stop_name = item_s['busName']
                                query = "SELECT * FROM bus_stop WHERE exit_id = '{}' and name = '{}'".format(exit_id,bus_stop_name)
                                cursor.execute(query)
                                result = cursor.fetchall()
                                    
                                for row in result:
                                    bus_stop_id = row[0]
                                    bus_line = item_s['busInfo']
                                    target = re.split(r'[,，;.、\s]+', bus_line) # 引入正则表达式
                                    tmp_dic[bus_stop_id] = target
                                        
        else:
            continue
        
        "print(tmp_dic)"
        for key, values in tmp_dic.items():
            for value in values:
                insert_query = "INSERT INTO bus_line(bus_stop_id, name) VALUES ('{}', '{}')".format(key, value)

                # 在执行插入操作之前，先检查数据库中是否已经存在相同的记录
                check_query = "SELECT * FROM bus_line WHERE bus_stop_id = '{}' AND name = '{}'".format(key, value)
                cursor.execute(check_query)
                existing_record = cursor.fetchone()
                
                # 如果数据库中不存在相同的记录，则执行插入操作
                if not existing_record:
                    cursor.execute(insert_query)

end_time_10 = time.time()
total_time_10 = end_time_10 - start_time_10
print(f"Table bus_line data insertion time: {total_time_10} seconds")



# 提交更改并关闭连接
conn.commit()
cursor.close()
conn.close()

end_time_total = time.time()  # 记录结束时间
total_time = end_time_total - start_time_total
print(f"Total time for data insertion: {total_time} seconds")

