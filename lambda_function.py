import json
import pymysql
import datetime
import boto3
import time
import os


def lambda_handler(event, context):

    # Extract
    connection = pymysql.connect(host = os.environ['RDS_HOST'],
                                 user = os.environ['USER_NAME'],
                                 password = os.environ['PASSWORD'],
                                 database = os.environ['DB_NAME'])
    
    cur = connection.cursor(pymysql.cursors.SSCursor)  
    #cur = db.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT * FROM bank LIMIT 100;"
    cur.execute(sql)
    
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S" )
    rows_to_insert = [] 
    # Transform
    #line_count = 0
    #batch_size = 40
    client = boto3.client('redshift-data') 
    for row in cur:
        print(row[1],row[2],timestamp)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }  