from __future__ import print_function
import json
from pyspark import SparkContext
import mysql.connector
import sys
import datetime

def get_data(json_record):
    try:
        json_body = json.loads(json_record)
        sender_id = int(json_body['actor']['id'])
        receiver_id=int(json_body['transactions'][0]['target']['id'])
        sender_isbusiness = 0
        if json_body['actor']['is_business'] != None:
            if json_body['actor']['is_business'] == True:
                sender_isbusiness = 2
            else:
                sender_isbusiness = 1
        #sender_isbusiness = int(json_body['actor']['is_business'])
        receiver_isbusiness = 0
        if json_body['transactions'][0]['target']['is_business'] != None:
            if json_body['transactions'][0]['target']['is_business'] == True:
                receiver_isbusiness = 2
            else:
                receiver_isbusiness = 1
        #receiver_isbusiness=int(json_body['transactions'][0]['target']['is_business'])
        timestamp = json_body['created_time']
        year = int(timestamp[0:4])
        month = int(timestamp[5:7])
        date = int(timestamp[8:10])
        hour = int(timestamp[11:13])
        dayofweek = int(datetime.datetime(year, month, date).strftime('%w'))
        return ((year, dayofweek, hour, sender_id,receiver_id,sender_isbusiness,receiver_isbusiness), 1)
    except:
        return None

db_config = {
    'host': 'ec2-54-82-188-230.compute-1.amazonaws.com',
    'user': 'nayoon',
    'password': 'haonayoon',
    'database': 'insight_data_2'}

#((year, dayofweek, hour, sender_id,receiver_id,isbusiness), 1)
data_schema1 = """CREATE TABLE IF NOT EXISTS Data_year_dayofweek_hour (
                 year INT(4),
                 dayofweek INT(1),
                 hour INT(2),
                 count INT,
                 PRIMARY KEY (year,dayofweek,hour)
                );
             """
data_schema3 = """CREATE TABLE IF NOT EXISTS Data_dayofweek_hour (
                 dayofweek INT(1),
                 hour INT(2),
                 count INT,
                 PRIMARY KEY (dayofweek,hour)
                );
             """

data_schema4 = """CREATE TABLE IF NOT EXISTS Data_hour (
                 hour INT(2) PRIMARY KEY,
                 count INT
                );
             """

data_schema2 = """CREATE TABLE IF NOT EXISTS Data_pairs_isbusiness (
                sender_id INT,
                receiver_id INT,
                sender_isbusiness INT,
                receiver_isbusiness INT,
                count INT,
                PRIMARY KEY (sender_id,receiver_id,sender_isbusiness,receiver_isbusiness)
                );
                """

stmt1 = """ insert ignore into Data_year_dayofweek_hour (year,dayofweek,hour,count) VALUES (%s,%s,%s,%s);"""
stmt2 = """ insert ignore into Data_pairs_isbusiness (sender_id,receiver_id,sender_isbusiness,receiver_isbusiness,count) VALUES (%s,%s,%s,%s,%s);"""
stmt3 = """ insert ignore into Data_dayofweek_hour (dayofweek,hour,count) VALUES (%s,%s,%s);"""
stmt4 = """ insert ignore into Data_hour (hour,count) VALUES (%s,%s);"""

def sql_create_table(sql_create_table_statement):
    try:
        connection = mysql.connector.connect(**db_config)
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
        connection = mysql.connector.connect(**db_config)
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

# table by sender, receiver pair is too large to collect to master node so we write it to database per partition
def write_pair_data(partition):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    data = []
    results = []
    for x in partition:
        sender_id = x[0][0]
        receiver_id= x[0][1]
        sender_isbusiness=x[0][2]
        receiver_isbusiness=x[0][3]
        count = x[1]
        print('sender_id: ' + sender_id + ' receiver_id: ' + receiver_id + ' sender_isbusiness: ' + sender_isbusiness + ' receiver_isbusiness: ' + receiver_isbusiness + ' count: ' + count)
        data.append((sender_id, receiver_id,sender_isbusiness,receiver_isbusiness,count))
        results.append(count)
    try:
    	cursor.executemany(stmt2, data)
    	connection.commit()
    except:
        connection.rollback()
    cursor.close()
    connection.close()
    return results

# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://host:7077 --executor-memory 6G spark_batch.py
# get_data(json_record):((year, hour, dayofweek, sender_id,receiver_id,isbusiness), 1)
#stmt1 = """ insert ignore into Data_year_dayofweek_hour (year,dayofweek,hour,count) VALUES (%s,%s,%s,%s);"""
#stmt2 = """ insert ignore into Data_pairs_isbusiness (sender_id,receiver_id,isbusiness,count) VALUES (%s,%s,%s,%s);"""
#stmt3 = """ insert ignore into Data_dayofweek_hour (dayofweek,hour,count) VALUES (%s,%s,%s);"""
#stmt4 = """ insert ignore into Data_hour (hour,count) VALUES (%s,%s);"""

if __name__ == "__main__":
    sc = SparkContext(appName="Venmo")
    read_rdd = sc.textFile("s3a://venmo-json/201*")
    data_rdd = read_rdd.map(lambda x: get_data(x)).filter(lambda x: filter_nones(x))
    #((year,dayofweek,hour,sender_id, receiver_id, isbusiness), 1)

    data_rdd_pair_business=data_rdd.map(lambda rdd: ((rdd[0][3],rdd[0][4],rdd[0][5],rdd[0][6]), rdd[1])).reduceByKey(lambda a, b: a + b)
    #year, dayofweek, hour
    data_rdd_year_dayofweek_hour=data_rdd.map(lambda rdd: ((rdd[0][0],rdd[0][1],rdd[0][2]),rdd[1])).reduceByKey(lambda a,b:a+b).map(lambda rdd : (rdd[0][0],rdd[0][1],rdd[0][2],rdd[1]))
    #dayofweek, hour
    data_rdd_dayofweek_hour = data_rdd_year_dayofweek_hour.map(lambda rdd: ((rdd[1], rdd[2]), rdd[3])).reduceByKey(lambda a, b: a + b).map(lambda rdd: (rdd[0][0], rdd[0][1], rdd[1]))
    data_rdd_hour = data_rdd_dayofweek_hour.map(lambda rdd: (rdd[1], rdd[2])).reduceByKey(lambda a, b: a + b)
    table_created1 = sql_create_table(data_schema1)
    table_created2 = sql_create_table(data_schema2)
    table_created3 = sql_create_table(data_schema3)
    table_created4 = sql_create_table(data_schema4)

    if table_created2:
        #data_inserted2 = sql_insert_rdd_to_table(prepared_statement=stmt2,collected_rdd=data_rdd_pair_business.collect())
        written_entries = data_rdd_pair_business.mapPartitions(write_pair_data)
        print(written_entries.count())
    else:
        print('Cannot create table by pair of users and isbusiness')
        sys.exit(0)


    if table_created1:
        data_inserted1 = sql_insert_rdd_to_table(prepared_statement=stmt1, collected_rdd=data_rdd_year_dayofweek_hour.collect())
    else:
        print('Cannot create table by year,dayofweek,hour')
        sys.exit(0)


    if table_created3:
        data_inserted3 = sql_insert_rdd_to_table(prepared_statement=stmt3, collected_rdd=data_rdd_dayofweek_hour.collect())
    else:
        print('Cannot create table dayofweek,hour')
        sys.exit(0)

    if table_created4:
        data_inserted3 = sql_insert_rdd_to_table(prepared_statement=stmt4, collected_rdd=data_rdd_hour.collect())
    else:
        print('Cannot create table hour')
        sys.exit(0)
