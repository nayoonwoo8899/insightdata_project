"""
Spark Python script for analyzing batch data from Amazon S3 and writing to MySQL.
Usage:
    $SPARK_HOME/bin/spark-submit --master spark://ec2-107-23-227-201.compute-1.amazonaws.com:7077 --executor-memory 6G spark_batch.py
Author: Nayoon Woo (nayoonwoo8899@gmail.com)
"""


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
        receiver_id = int(json_body['transactions'][0]['target']['id'])
        timestamp = json_body['created_time']
        year = int(timestamp[0:4])
        month = int(timestamp[5:7])
        date = int(timestamp[8:10])
        hour = int(timestamp[11:13])
        dayofweek = int(datetime.datetime(year, month, date).strftime('%w'))
        return ((year, dayofweek, hour, sender_id, receiver_id), 1)
    except:
        return None

db_config = {
    'host': 'ec2-54-82-188-230.compute-1.amazonaws.com',
    'user': 'nayoon',
    'password': 'haonayoon',
    'database': 'insight_data_ventra'}

data_schema1 = """CREATE TABLE IF NOT EXISTS Data_year_dayofweek_hour (
                 year INT(4),
                 dayofweek INT(1),
                 hour INT(2),
                 count INT,
                 PRIMARY KEY (year,dayofweek,hour)
                );
             """
data_schema2 = """CREATE TABLE IF NOT EXISTS Data_dayofweek_hour (
                 dayofweek INT(1),
                 hour INT(2),
                 count INT,
                 PRIMARY KEY (dayofweek,hour)
                );
             """

data_schema3 = """CREATE TABLE IF NOT EXISTS Data_hour (
                 hour INT(2) PRIMARY KEY,
                 count INT
                );
             """

data_schema4 = """CREATE TABLE IF NOT EXISTS Data_year_sender (
                year INT(4),
                sender_id INT,
                count INT,
                PRIMARY KEY (year,sender_id)
                );
                """

data_schema5 = """CREATE TABLE IF NOT EXISTS Data_year_receiver (
                year INT(4),
                receiver_id INT,
                count INT,
                PRIMARY KEY (year,receiver_id)
                );
                """

data_schema6 = """CREATE TABLE IF NOT EXISTS Data_year_user (
                year INT(4),
                user_id INT,
                count INT,
                PRIMARY KEY (year,user_id)
                );
                """

stmt1 = """ insert ignore into Data_year_dayofweek_hour (year,dayofweek,hour,count) VALUES (%s,%s,%s,%s);"""
stmt2 = """ insert ignore into Data_dayofweek_hour (dayofweek,hour,count) VALUES (%s,%s,%s);"""
stmt3 = """ insert ignore into Data_hour (hour,count) VALUES (%s,%s);"""
stmt4 = """ insert ignore into Data_year_sender (year,sender_id,count) VALUES (%s,%s,%s);"""
stmt5 = """ insert ignore into Data_year_receiver (year,receiver_id,count) VALUES (%s,%s,%s);"""
stmt6 = """ insert ignore into Data_year_user (year,user_id,count) VALUES (%s,%s,%s);"""

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

# table by sender or receiver is too large to collect to master node so we write it to database per partition
def write_data(partition, stmt):
    connection = mysqlcon.connect(**db_config)
    cursor = connection.cursor()
    data = []
    results = []
    for x in partition:
        year = x[0][0]
        id = x[0][1]
        count = x[1]
        data.append((year, id, count))
        results.append(count)
    try:
    	cursor.executemany(stmt, data)
    	connection.commit()
    except:
        connection.rollback()
    cursor.close()
    connection.close()
    return results

def write_sender_data(partition):
    write_data(partition, stmt4)
    
def write_receiver_data(partition):
    write_data(partition, stmt5)
    
def write_user_data(partition):
    write_data(partition, stmt6)

if __name__ == "__main__":
    sc = SparkContext(appName="Venmo")
    read_rdd = sc.textFile("s3a://venmo-json/201*")
    
    # ((year, dayofweek, hour, sender_id, receiver_id), 1)
    data_rdd = read_rdd.map(lambda x: get_data(x)).filter(lambda x: filter_nones(x))
    
    # ((year, dayofweek, hour), count)
    data_rdd_year_dayofweek_hour = data_rdd.map(lambda rdd : ((rdd[0][0], rdd[0][1], rdd[0][2]), rdd[1])).reduceByKey(lambda a, b : a + b)
    # ((dayofweek, hour), count)
    data_rdd_dayofweek_hour = data_rdd_year_dayofweek_hour.map(lambda rdd : ((rdd[0][1], rdd[0][2]), rdd[1])).reduceByKey(lambda a, b : a + b)
    # (hour, count)
    data_rdd_hour = data_rdd_dayofweek_hour.map(lambda rdd : (rdd[0][1], rdd[1])).reduceByKey(lambda a, b : a + b)
    
    # ((year, sender_id), count)
    data_rdd_year_sender = data_rdd.map(lambda rdd : ((rdd[0][0], rdd[0][3]), rdd[1])).reduceByKey(lambda a, b : a + b)
    # ((year, receiver_id), count)
    data_rdd_year_receiver = data_rdd.map(lambda rdd : ((rdd[0][0], rdd[0][4]), rdd[1])).reduceByKey(lambda a, b : a + b)
    # ((year, user_id), count)
    data_rdd_year_user = data_rdd_year_sender.union(data_rdd_year_receiver).reduceByKey(lambda a, b : a + b)

    table_created1 = sql_create_table(data_schema1)
    table_created2 = sql_create_table(data_schema2)
    table_created3 = sql_create_table(data_schema3)
    table_created4 = sql_create_table(data_schema4)
    table_created5 = sql_create_table(data_schema5)
    table_created6 = sql_create_table(data_schema6)

    if table_created1:
        data_inserted1 = sql_insert_rdd_to_table(prepared_statement=stmt1, collected_rdd=data_rdd_year_dayofweek_hour.map(lambda rdd : (rdd[0][0], rdd[0][1], rdd[0][2], rdd[1])).collect())
    else:
        print('Cannot create table by year,dayofweek,hour')
        sys.exit(0)

    if table_created2:
        data_inserted2 = sql_insert_rdd_to_table(prepared_statement=stmt2, collected_rdd=data_rdd_dayofweek_hour.map(lambda rdd : (rdd[0][0], rdd[0][1], rdd[1])).collect())
    else:
        print('Cannot create table by dayofweek,hour')
        sys.exit(0)

    if table_created3:
        data_inserted3 = sql_insert_rdd_to_table(prepared_statement=stmt3, collected_rdd=data_rdd_hour.collect())
    else:
        print('Cannot create table by hour')
        sys.exit(0)

    if table_created4:
        written_entries = data_rdd_pair_business.mapPartitions(write_sender_data)
        print(written_entries.count())
    else:
        print('Cannot create table by year and sender')
        sys.exit(0)
        
    if table_created5:
        written_entries = data_rdd_pair_business.mapPartitions(write_receiver_data)
        print(written_entries.count())
    else:
        print('Cannot create table by year and receiver')
        sys.exit(0)
        
    if table_created6:
        written_entries = data_rdd_pair_business.mapPartitions(write_user_data)
        print(written_entries.count())
    else:
        print('Cannot create table by year and user')
        sys.exit(0)
