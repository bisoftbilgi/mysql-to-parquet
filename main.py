import mysql.connector
import duckdb 
import pandas as pd
import argparse
import csv 
from datetime import datetime
from getpass import getpass

parser = argparse.ArgumentParser()
parser.add_argument("--table-name","-t",help="Table Name To Export",required=True)
parser.add_argument("--target-directory","-d",help="Path of the output directory",required=False)
parser.add_argument("--host","-H",help="MySQL hostaname of IP",required=False, default="localhost")
parser.add_argument("--user","-u",help="MySQL user",required=False, default="root")
parser.add_argument("--password","-p",help="MySQL user password",required=False)
parser.add_argument("--port","-P",help="MySQL port",required=False,default="3306")
parser.add_argument("--database","-D",help="MySQL database to connect",required=False,default="mysql")
parser.add_argument("--where","-w",help="If part of the data will be extracted provide a where condition",required=False,default=" 1 = 1 ")
parser.add_argument("--ask-pass",help="Ask MySQL password during execution",action="store_true", default=argparse.SUPPRESS)

args = parser.parse_args()

duckdb_conn = duckdb.connect()

if "ask_pass" in args:
    password = getpass()
else:
    password = args.password

# Establish connection to MySQL database
conn = mysql.connector.connect(
    host=args.host,
    user=args.user,
    password=password,
    database=args.database,
)

cursor = conn.cursor(dictionary=True)

where = ""

if "where" in args:
    where = f"where {args.where}"

cursor.execute(f"select * from {args.table_name} {where}")

i=0

_list = []
first = True
row_count = cursor.rowcount

print("Starting ",datetime.now())

for line in cursor:
    i += 1
    if i % 50000 == 0:
        print(args.table_name,"Lines Exported",i ,end='\r')
    _list.append(line)
    if i % 100 == 0 :
        df = pd.DataFrame(_list)
        if first:
            df.to_csv(f'{args.target_directory}/{args.table_name}.csv', index=False, header=True,quoting=csv.QUOTE_MINIMAL,escapechar="\\")
            first = False
        else:
            df.to_csv(f'{args.target_directory}/{args.table_name}.csv', mode='a', index=False, header=False,quoting=csv.QUOTE_MINIMAL,escapechar="\\")
        _list=[]

print(args.table_name,"Lines Exported",i )

try:
    duckdb_conn.sql(f" copy (select * from read_csv('{args.target_directory}/{args.table_name}.csv',AUTO_DETECT=TRUE,HEADER=TRUE,PARALLEL=TRUE)) to '{args.target_directory}/{args.table_name}.parquet' (format 'PARQUET' )")
except Exception as e:
    print("Can not convert",args.table_name,' to parquet')
    print(e)