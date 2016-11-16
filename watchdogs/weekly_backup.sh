#!/bin/bash
# DAILY BACKUP
# Take a full backup of the database and store it in the 
# local backup folder + S3 Storage
#

echo `date` - Delete old backups
#find /home/numascale/BACKUP_MONETDB/daily/ -mtime +56 -delete 

echo `date` - Do a full database dump
DATE=`date +%Y-%m-%d`;
PERIOD='daily'

## Backup a local database structure without data using ‘msqldump’
/opt/numascale/bin/msqldump -p 54321 --database=ctt  --describe | gzip > /home/numascale/BACKUP_MONETDB/$PERIOD/bkp_Structure_TK_$DATE.sql.gz
/opt/numascale/bin/msqldump -p 50000 --database=ctt  --describe | gzip > /home/numascale/BACKUP_MONETDB/$PERIOD/bkp_Structure_VJ_$DATE.sql.gz

## Backup a local database with data
/opt/numascale/bin/mclient -p 54321 -d ctt --dump | gzip > /home/numascale/BACKUP_MONETDB/$PERIOD/bkp_DUMP_TK_$DATE.sql.gz
/opt/numascale/bin/mclient -p 50000 -d ctt --dump | gzip > /home/numascale/BACKUP_MONETDB/$PERIOD/bkp_DUMP_VJ_$DATE.sql.gz


echo `date` - Backup complete
