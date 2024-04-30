### MySQL To Parquet 

MySQL to parquet is command line tool to offload any particular MySQL table or part of the table to a parquet file

It is inefficient to fetch all rows and convert to parquet, it uses a streaming cursor to fetch rows. Converts chunks of data to a intermediary CSV file. Then convert CSV file to a parquet file. 

This approach is very memory and CPU effective when dealing with large tables. 

## Installation

``` 
git clone https://github.com/bisoftbilgi/mysql-to-parquet.git

cd mysql-to-parquet

pip install -r requirements.txt

```

## Usage

usage: main.py [-h] --table-name TABLE_NAME [--target-directory TARGET_DIRECTORY] [--host HOST] [--user USER] [--password PASSWORD] [--port PORT] [--database DATABASE] [--where WHERE] [--ask-pass]
               [--analyze-table] [--progress] [--keep-csv]

`--table-name or -t` is the name of the table to offload

`--target-directory or -d ` is the directory to offload data. Both CSV and parquet files are kept in this directory. **The target directory should be have enough space to keep both CSV and parquet file.**

`--host or -H ` is the hostname or ip adress of the MySQL instance

`--user or -u` is the username to connect to the MySQL instance 

`--password or - p` is the password for the MySQL user

`--ask-pass` is used in order to achive interactive password input, `--ask-pass` has higher precedence over `--pasword` argument. So if `--ask-pass` is used, it will ask to input a password and use it anyway.

`--port` is the port for the MySQL instance

`--database or -D ` is the schema name in which the table exists

`--where or -w ` is expression if any partial offload is required such as ` createAt < now() - interval 1 year `. There should be no table aliases for the target table, just column names and other expressions. 

`--analyze-table` is to analyze table to predict number of rows in the MySQL table before export

`--progress` output progress for menu.sh progress bar

`--keep-csv` is to keep CSV file after convert CSV file to parquet

## menu

Alternatively user can use menu.sh bash script to guide export process. It uses whiptail library in linux systems for terminal screens.  

![01-main_screen](https://github.com/bisoftbilgi/mysql-to-parquet/assets/45390009/8ba3ee47-0a7c-4e95-84c5-12ff167f116e)

![02-hostname](https://github.com/bisoftbilgi/mysql-to-parquet/assets/45390009/54edd385-b6e5-47ee-a585-2ba02df70f2e)

![03-user](https://github.com/bisoftbilgi/mysql-to-parquet/assets/45390009/0a701f77-cdba-4771-be55-8a41290ccac5)

![04-password](https://github.com/bisoftbilgi/mysql-to-parquet/assets/45390009/bbbb4896-53de-4903-a923-7de3129176bf)

![05-port](https://github.com/bisoftbilgi/mysql-to-parquet/assets/45390009/9dd81eb0-22e1-47cc-97d9-9d9bb900e737)

![06-export-directory](https://github.com/bisoftbilgi/mysql-to-parquet/assets/45390009/f536f3f1-74ee-40e3-a1bd-d5cd9faa33b9)










