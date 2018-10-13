from __future__ import print_function
import json
from pyspark import SparkContext
import mysql.connector as mysqlcon
import sys
import datetime

def get_data(json_record):
    try:
        json_body = json.loads(json_record)
        sender_id = int(json_body['actor']['id'])
        receiver_id=int(json_body['transactions'][0]['target']['id'])
        timestamp = json_body['created_time']
        year = int(timestamp[0:4])
	    return ((year,sender_id,receiver_id),1)
    except:
        return None

db_config = {
    'host': 'ec2-54-82-188-230.compute-1.amazonaws.com',
    'user': 'nayoon',
    'password': 'haonayoon',
    'database': 'insight_data_user'}

#((year,sender_id,receiver_id), 1)

data_schema2 = """CREATE TABLE IF NOT EXISTS Data_pairs_test(
		year INT,
		sender_id INT,
		receiver_id INT,
		count INT,
		PRIMARY KEY (sender_id,receiver_id)
		);
	     """
#stmt1 = """ insert ignore into Data_year_dayofweek_hour (year,dayofweek,hour,count) VALUES (%s,%s,%s,%s);"""
stmt2 = """insert ignore into Data_pairs_test (year,sender_id,receiver_id,count) VALUES (%s,%s,%s);"""
#stmt3 = """ insert ignore into Data_dayofweek_hour (dayofweek,hour,count) VALUES (%s,%s,%s);"""
#stmt4 = """ insert ignore into Data_hour (hour,count) VALUES (%s,%s);"""

def sql_create_table(sql_create_table_statement):
    try:
        connection = mysqlcon.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(sql_create_table_statement)
	    connection.commit()
	    cursor.close()
	    connection.close()
	    return True
    except Exception as e:
	    print(e)
	    return False

def sql_insert_rdd_to_table(prepared_statement, collected_rdd):
    try:
	    connection = mysqlcon.connect(**db_config)
	    cursor = connection.cursor()
	    cursor.executemany(prepared_statement, collected_rdd)
	    connection.commit()
	    cursor.close()
	    connection.close()
	    return True
    except Exception as e:
	    print(e)
	    return False

def filter_nones(data):
    if data is not None:
	    return True
    return False

def write_pair_data(partition):
    connection = mysqlcon.connect(**db_config)
    cursor = connection.cursor()
    data = []
    results = []
    for x in partition:
        year=x[0]
	    sender_id = x[1]
	    receiver_id= x[2]
	    count = x[3]
	    data.append((year,sender_id, receiver_id,count))
	    results.append(count)
    try:
	    cursor.executemany(stmt2, data)
	    connection.commit()
    except:
	    connection.rollback()
    cursor.close()
    connection.close()
    return results

# get_data(json_record):((year, hour, dayofweek, sender_id,receiver_id), 1)
#stmt1 = """ insert ignore into Data_year_dayofweek_hour (year,dayofweek,hour,count) VALUES (%s,%s,%s,%s);"""
#stmt2 = """ insert ignore into Data_pairs_isbusiness (sender_id,receiver_id,isbusiness,count) VALUES (%s,%s,%s,%s);"""
#stmt3 = """ insert ignore into Data_dayofweek_hour (dayofweek,hour,count) VALUES (%s,%s,%s);"""
#stmt4 = """ insert ignore into Data_hour (hour,count) VALUES (%s,%s);"""

if __name__ == "__main__":
    sc = SparkContext(appName="Venmo")
    read_rdd = sc.textFile("s3a://venmo-json/2015*")
    data_rdd = read_rdd.map(lambda x: get_data(x)).filter(lambda x: filter_nones(x))
    #((year,dayofweek,hour,sender_id, receiver_id), 1)
    data_rdd_pair=data_rdd.map(lambda rdd: ((rdd[0][0],rdd[0][1],rdd[0][2]), rdd[1])).reduceByKey(lambda a, b: a + b).map(lambda rdd:(rdd[0][0],rdd[0][1],rdd[0][2],rdd[1]))
    #year, dayofweek, hour
    #data_rdd_year_dayofweek_hour=data_rdd.map(lambda rdd: ((rdd[0][0],rdd[0][1],rdd[0][2]),rdd[1])).reduceByKey(lambda a,b:a+b).map(lambda rdd : (rdd[0][0],rdd[0][1],rdd[0][2],rdd[1]))
    #dayofweek, hour
    #data_rdd_dayofweek_hour = data_rdd_year_dayofweek_hour.map(lambda rdd: ((rdd[1], rdd[2]), rdd[3])).reduceByKey(lambda a, b: a + b).map(lambda rdd: (rdd[0][0], rdd[0][1], rdd[1]))
    #data_rdd_hour = data_rdd_dayofweek_hour.map(lambda rdd: (rdd[1], rdd[2])).reduceByKey(lambda a, b: a + b)
    #table_created1 = sql_create_table(data_schema1)
    table_created2 = sql_create_table(data_schema2)
    #table_created3 = sql_create_table(data_schema3)
    #table_created4 = sql_create_table(data_schema4)

    #if table_created2:
    #    data_inserted2 = sql_insert_rdd_to_table(prepared_statement=stmt2,collected_rdd=data_rdd_pair.collect())

    written_entries = data_rdd_pair.mapPartitions(write_pair_data)
    print(written_entries.count())


