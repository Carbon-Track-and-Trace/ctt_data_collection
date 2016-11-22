#!/bin/bash
# DAILY BACKUP
# Take a full backup of the database and store it in the 
# local backup folder + S3 Storage
#

echo `date` - Delete old backups
find /home/climathon09/BACKUPs_CTT/daily/ -mtime +365 -delete 

echo `date` - Do a full database dump
DATE=`date +%Y-%m-%d`;
PERIOD='monthly'
mkdir -p /home/climathon09/BACKUPs_CTT/$PERIOD

## Backup a local database structure without data using ‘msqldump’
msqldump -p 54321 --database=ctt  --describe | gzip > /home/climathon09/BACKUPs_CTT/$PERIOD/bkp_Structure_TK_$DATE.sql.gz
msqldump -p 50000 --database=ctt  --describe | gzip > /home/climathon09/BACKUPs_CTT/$PERIOD/bkp_Structure_VJ_$DATE.sql.gz

## Backup a local database with data
mclient -p 54321 -d ctt --dump | gzip > /home/climathon09/BACKUPs_CTT/$PERIOD/bkp_DUMP_TK_$DATE.sql.gz
mclient -p 50000 -d ctt --dump | gzip > /home/climathon09/BACKUPs_CTT/$PERIOD/bkp_DUMP_VJ_$DATE.sql.gz


echo `date` - Backup complete
