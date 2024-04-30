#!/usr/bin/bash

whiptail --title "MySQL To Parquet" --msgbox "Please Press Enter to Continue" 8 78


MYSQL_HOST=$(whiptail --inputbox "MySQL Hostname or IP" 8 78 localhost --title "MySQL Config" 3>&1 1>&2 2>&3)
MYSQL_USER=$(whiptail --inputbox "MySQL Username" 8 78 root --title "MySQL Config" 3>&1 1>&2 2>&3)
MYSQL_PASSWORD=$(whiptail --passwordbox  "MySQL Password" 8 78  --title "MySQL Config" 3>&1 1>&2 2>&3)
MYSQL_PORT=$(whiptail --inputbox  "MySQL Port" 8 78  3306 --title "MySQL Config" 3>&1 1>&2 2>&3)

TD=$(pwd)
TARGET_DIRECTORY=$( whiptail --inputbox  "Export Directory" 8 78  ${TD}    --title "Export Config" 3>&1 1>&2 2>&3 )




DATABASES=( $(mysql -u${MYSQL_USER} -h${MYSQL_HOST} -p${MYSQL_PASSWORD} -P${MYSQL_PORT} -s -N -e"show databases") )

declare -a DBS=( )
for item in "${DATABASES[@]}"; do
    DBS+=("$item" "-" OFF)
done


DATABASE=$( whiptail --radiolist "Choose a database:" 35 78 20 --title "Export Config" ${DBS[@]} 3>&1 1>&2 2>&3 ) 

TABLES=( $(mysql -u${MYSQL_USER} -h${MYSQL_HOST} -p${MYSQL_PASSWORD} -P${MYSQL_PORT} -D${DATABASE} -s -N -e"show tables") )

declare -a TBS=( )
for item in "${TABLES[@]}"; do
    TBS+=("$item" "-" OFF)
done

TABLE=$( whiptail --checklist "Choose tables:" 35 78 20 --title "Export Config" ${TBS[@]} 3>&1 1>&2 2>&3 )


for item in $TABLE
do 
    TBL=$(echo $item | sed 's/.\{1\}$//''' | sed 's/^.//''')
    mysql -u${MYSQL_USER} -h${MYSQL_HOST} -p${MYSQL_PASSWORD} -P${MYSQL_PORT} -D${DATABASE} -s -N -e"analyze table ${DATABASE}.${TBL}"
    TABLE_ROWS=$( mysql -u${MYSQL_USER} -h${MYSQL_HOST} -p${MYSQL_PASSWORD} -P${MYSQL_PORT} -D${DATABASE} -s -N -e"select table_rows from information_schema.tables where table_schema='${DATABASE}' and TABLE_NAME='${TBL}'" )
    python main.py -H ${MYSQL_HOST}  -u${MYSQL_USER} -p ${MYSQL_PASSWORD} -t ${TBL} -D ${DATABASE} -p ${MYSQL_PASSWORD} \
     -d ${TARGET_DIRECTORY} --progress | whiptail --gauge  "${TBL} with ${TABLE_ROWS} rows"  6 50 0
done

