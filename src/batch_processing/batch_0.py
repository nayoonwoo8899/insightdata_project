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
	'user':'username',
	'password':'password',
	'database':'database_name'}

#connects to the database and creates table if it does not exist based on sql_create_table_statement.
def sql_create_table(sql_create_table_statement):
	try:
		connection = mysql.connector.connect(db_config)
		cursor = connection.cursor()
		cursor.execute(sql_create_table_statement)
		connection.commit()
		cursor.close()
		connection.close()
		return True

	except:
        return False

data_schema1=
"""
CREATE TABLE IF NOT EXISTS Data_2010_Ver1 (
hour INT(2) PRIMARY KEY,
count INT
);"""

data_schema2=
"""
CREATE TABLE IF NOT EXISTS Data_2010_Ver1_dayofweek (
hour INT(2),
dayofweek INT(1),
count INT,
PRIMARY KEY (hour, dayofweek)
);"""

data_schema3=
"""
CREATE TABLE IF NOT EXISTS Data_2010_Ver1_user (
hour INT(2),
sender_id INT,
count INT,
PRIMARY KEY (hour, sender_id)
);"""

stmt1= """ insert ignore into Data_2010_Ver1 (hour, count) VALUES (%s, %s);"""
stmt2= """ insert ignore into Data_2010_Ver1_dayofweek (hour, dayofweek,count ) VALUES (%s,%s, %s);"""
stmt3= """ insert ignore into Data_2010_Ver1_user (hour, sender_id,count) VALUES (%s,%s,%s);"""


#uses prepared statement to insert collected rdd to table
def sql_insert_rdd_to_table(prepared_statement, collected_rdd):
    try:
		connection = mysql.connector.connect(db_config)
        cursor = connection.cursor()
		cursor.executemany(prepared_statement, collected_rdd)
		connection.commit()
		cursor.close()
		connection.close()
		return True

    except:
        return False



def filter_nones(data):
    if data is not None:
        return True
    return False


# To submit script:
# $SPARK_HOME/bin/spark-submit --master spark://host:7077 --executor-memory 6G spark_batch.py


if __name__ == "__main__":
    sc = SparkContext(appName="Venmo")
    read_rdd = sc.textFile("s3a://venmo-json/2010*")
    data_rdd = read_rdd.map(lambda x: get_data(x)).filter(lambda x: filter_nones(x))
    data_rdd_hour_dayofweek = data_rdd.map(lambda rdd: ((rdd[0][4],rdd[0][3]),rdd[1])).reduceByKey(lambda a,b:a+b)
    data_rdd_hour_user = data_rdd.map(lambda rdd: ((rdd[0][5],rdd[0][3]),rdd[1])).reduceByKey(lambda a,b:a+b)

    data_rdd_hour = data_rdd_hour_dayofweek.map(lambda rdd: (rdd[0][1],rdd[1])).reduceByKey(lambda a,b:a+b)
    # clean json data

    table_created1 = sql_create_table(data_schema1)
    table_created2 = sql_create_table(data_schema2)
    table_created3 = sql_create_table(data_schema3)



    if table_created2:
        data_inserted2 = sql_insert_rdd_to_table(prepared_statement=stmt2,collected_rdd=data_rdd_hour_dayofweek.collect())
    else:
        logging.error("Error in table creation")

    if table_created3:
        data_inserted3 = sql_insert_rdd_to_table(prepared_statement=stmt3,collected_rdd=data_rdd_hour_user.collect())
    else:
        logging.error("Error in table creation")

    if table_created1:
        data_inserted1 = sql_insert_rdd_to_table(prepared_statement=stmt1,collected_rdd=data_rdd_hour.collect())
    else:
        logging.error("Error in table creation")
