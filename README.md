### MySQL To Parquet 

MySQL to parquet is command line tool to offload any particular MySQL table or part of the table to a parquet file

It is inefficient to fetch all rows and convert to parquet, it uses a streaming cursor to fetch rows. Converts chunks of data to a intermediary CSV file. Then convert CSV file to a parquet file. 

This approach is very memory and CPU effective when dealing with large tables. 

`usage: main.py [-h] --table-name TABLE_NAME [--target-directory TARGET_DIRECTORY] [--host HOST] [--user USER] [--password PASSWORD] [--port PORT] [--database DATABASE] [--where WHERE] [--ask-pass]`

`--table-name or -t` is the name of the table to offload

`--target-directory or -d ` is the directory to offload data. Both CSV and parquet files are kept in this directory. **The target directory should be have enough space to keep both CSV and parquet file.**

`--host or -H ` is the hostname or ip adress of the MySQL instance

`--user or -u` is the username to connect to the MySQL instance 

`--password or - p` is the password for the MySQL user

`--ask-pass` is used in order to achive interactive password input, `--ask-pass` has higher precedence over `--pasword` argument. So if `--ask-pass` is used, it will ask to input a password and use it anyway.

`--port` is the port for the MySQL instance

`--database or -D ` is the schema name in which the table exists

`--where or -w ` is expression if any partial offload is required such as ` createAt < now() - interval 1 year `. There should be no table aliases for the target table, just column names and other expressions. 




