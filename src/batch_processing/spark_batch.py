from __future__ import print_function

# To import next library you have to install some packages
# sudo apt-get update
# sudo apt-get install python-mysqldb
# sudo apt-get install mysql-client-core-5.5

import json
from pyspark import SparkContext
import mysql.connector
import sys
import datetime


def get_data(json_record):
    """maps each json record to ( (sender_id, transaction_date), 1) """
    try:
            json_body = json.loads(json_record)
            sender_id = int(json_body['actor']['id'])
            timestamp = json_body['created_time']
            year=int(timestamp[0:4])
            month = int(timestamp[5:7])
            date=int(timestamp[8:10])
            hour=int(timestamp[11:13])
            dayofweek=int(datetime.datetime(year, month, date).strftime('%w'))
            return ((year,month,date,hour,dayofweek,sender_id), 1)
    except:
            return None

db_config={
	'host':'ec2-54-82-188-230.compute-1.amazonaws.com',
	'user':'nayoon',
	'password':'haonayoon',
	'database':'insight_data'}

#connects to the database and creates table if it does not exist based on sql_create_table_statement.
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

data_schema1= """CREATE TABLE IF NOT EXISTS Data_2015_Ver1 (
                 hour INT(2) PRIMARY KEY,
                 count INT
                );
             """

data_schema2="""CREATE TABLE IF NOT EXISTS Data_2015_Ver1_dayofweek (
                hour INT(2),
                dayofweek INT(1),
                count INT,
                PRIMARY KEY (hour, dayofweek)
               );
               """

data_schema3="""CREATE TABLE IF NOT EXISTS Data_2015_Ver1_user (
                hour INT(2),
                sender_id INT,
                count INT,
                PRIMARY KEY (hour, sender_id)
                );
                """

stmt1= """ insert ignore into Data_2015_Ver1 (hour, count) VALUES (%s, %s);"""
stmt2= """ insert ignore into Data_2015_Ver1_dayofweek (hour, dayofweek,count ) VALUES (%s,%s, %s);"""
stmt3= """ insert ignore into Data_2015_Ver1_user (hour, sender_id,count) VALUES (%s,%s,%s);"""


#uses prepared statement to insert collected rdd to table
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

# table by user is too large to collect to master node so we write it to database per partition
def write_user_data(partition):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    data = []
    results = []
    for x in partition:
        sender_id=x[0][1]
        hour=x[0][0]
        count=x[1]
        data.append(hour,sender_id,count)
        results.append(count)
    try:
        cursor.executemany(stmt3, data)
        connection.commit()
    except:
        connection.rollback()
    cursor.close()
    connection.close()
    return results
    

# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://host:7077 --executor-memory 6G spark_batch.py


if __name__ == "__main__":
    sc = SparkContext(appName="Venmo")
    read_rdd = sc.textFile("s3a://venmo-json/2015*")
    data_rdd = read_rdd.map(lambda x: get_data(x)).filter(lambda x: filter_nones(x))
    data_rdd_hour_dayofweek = data_rdd.map(lambda rdd: ((rdd[0][4],rdd[0][3]),rdd[1])).reduceByKey(lambda a,b:a+b).map(lambda rdd : (rdd[0][1], rdd[0][0], rdd[1]))
    data_rdd_hour_user = data_rdd.map(lambda rdd: ((rdd[0][5],rdd[0][3]),rdd[1])).reduceByKey(lambda a,b:a+b)

    data_rdd_hour = data_rdd_hour_dayofweek.map(lambda rdd: (rdd[0],rdd[2])).reduceByKey(lambda a,b:a+b)

    # test by printing RDD content
    #print('By hour and day:')
    #for x in data_rdd_hour_dayofweek.collect():
    #    print(x)
    #print('By hour:')
    #for x in data_rdd_hour.collect():
    #    print(x)
    # clean json data

    table_created1 = sql_create_table(data_schema1)
    table_created2 = sql_create_table(data_schema2)
    table_created3 = sql_create_table(data_schema3)



    if table_created2:
        data_inserted2 = sql_insert_rdd_to_table(prepared_statement=stmt2,collected_rdd=data_rdd_hour_dayofweek.collect())
    else:
        print('Cannot create table by hour and day of week')
        sys.exit(0)

    if table_created3:
    #    data_inserted3 = sql_insert_rdd_to_table(prepared_statement=stmt3,collected_rdd=data_rdd_hour_user.collect())
        written_entries = data_rdd_hour_user.mapPartitions(write_user_data)
    else:
        print('Cannot create table by hour and user')
        sys.exit(0)

    if table_created1:
        data_inserted1 = sql_insert_rdd_to_table(prepared_statement=stmt1,collected_rdd=data_rdd_hour.collect())
    else:
        print('Cannot create table by hour')
        sys.exit(0)
