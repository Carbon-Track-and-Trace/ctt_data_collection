echo '' >> /home/numascale/BACKUP_DB_scripts/date.txt
echo `date` >> /home/numascale/BACKUP_DB_scripts/date.txt


MONETDBD_TK_PS=`ps -elf | grep "monetdbd start /home/mydbfarm_TK_deploy" | wc -l`
MONETDBD_TK=`ps -elf | grep "monetdbd start /home/mydbfarm_TK_deploy" | awk '{for(i=1;i<15;i++) $i="";print}' | grep -v 'grep'`
if [ $MONETDBD_TK_PS -ge 1 ];
then
    echo $MONETDBD_TK >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo $MONETDBD_TK 
else 
    echo "PROBLEM: MONETDBD_TK process is not running anymore" >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo "PROBLEM: MONETDBD_TK process is not running anymore" 
    monetdbd create /home/mydbfarm_TK_deploy
    monetdbd start /home/mydbfarm_TK_deploy
    monetdbd set port=54321 /home/mydbfarm_TK_deploy
    monetdb -p 54321 create ctt
    monetdb -p 54321 release ctt
    #mail -s "MONETDBD_TK process is not running. Restarting." pm@aiascience.com
fi


MONETDBD_VJ_PS=`ps -elf | grep "monetdbd start /home/mydbfarm/" | wc -l`
MONETDBD_VJ=`ps -elf | grep "monetdbd start /home/mydbfarm/" | awk '{for(i=1;i<15;i++) $i="";print}' | grep -v 'grep'`
if [ $MONETDBD_VJ_PS -ge 1 ];
then
    echo $MONETDBD_VJ >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo $MONETDBD_VJ 
else 
    echo "PROBLEM: MONETDBD_VJ process is not running anymore" >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo "PROBLEM: MONETDBD_VJ process is not running anymore" 
    monetdbd create /home/mydbfarm
    monetdbd start /home/mydbfarm
    monetdbd set port=50000 /home/mydbfarm
    monetdb -p 50000 create ctt
    monetdb -p 50000 release ctt
    #mail -s "MONETDBD_VJ process is not running. Restarting." pm@aiascience.com
fi


TRONDHEIM_PS=`ps -elf | grep python | grep TRONDHEIM | wc -l`
TRONDHEIM=`ps -elf | grep python | grep TRONDHEIM | awk '{print $16}'`
if [ $TRONDHEIM_PS -ge 1 ];
then
    echo $TRONDHEIM >> /home/numascale/BACKUP_DB_scripts/date.txt 
    echo $TRONDHEIM 
else 
    echo "PROBLEM: Trondheim script is not running" >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo "PROBLEM: Trondheim script is not running" 
    screen -S sensor_collect_TRONDHEIM_deploy -d -m /home/climathon09/ctt_data_collection_TRONDHEIM/collectSensorDataCTT.py
    #mail -s "Trondheim script is not running. Restarting." pm@aiascience.com
fi

VEJLE_PS=`ps -elf | grep python | grep VEJLE | wc -l`
VEJLE=`ps -elf | grep python | grep VEJLE | awk '{print $16}'`
if [ $VEJLE_PS -ge 1 ];
then
    echo $VEJLE >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo $VEJLE 
else 
    echo "PROBLEM: VEJLE script is not running" >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo "PROBLEM: VEJLE script is not running" 
    screen -S sensor_collect_VEJLE_deploy -d -m /home/climathon09/ctt_data_collection_VEJLE/collectSensorDataCTT.py
    #mail -s "Vejle script is not running. Restarting." pm@aiascience.com
fi


MOSQUITTO_VJ_PS=`ps -elf | grep "mosquitto_sub.*70B3D57ED00006CE" | wc -l`
MOSQUITTO_VJ=`ps -elf | grep 'mosquitto_sub.*70B3D57ED00006CE' | awk '{for(i=1;i<15;i++) $i="";print}' | grep 'staging'`
if [ $MOSQUITTO_VJ_PS -ge 2 ];
then
    echo $MOSQUITTO_VJ >> /home/numascale/BACKUP_DB_scripts/date.txt 
    echo $MOSQUITTO_VJ 
else 
    echo "PROBLEM The MQTT msg collection with Mosquitto is not running for VEJLE" >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo "PROBLEM The MQTT msg collection with Mosquitto is not running for VEJLE" 
    screen -S mosquitto_VEJLE -d -m /usr/bin/bash /home/climathon09/MQTT_temp/mosquitto_VEJLE.sh
    #mail -s "The MQTT msg collection with Mosquitto stopped running for VEJLE. Restarting..." pm@aiascience.com
fi


MOSQUITTO_TK_PS=`ps -elf | grep "mosquitto_sub.*70B3D57ED0000AD8" | wc -l`
MOSQUITTO_TK=`ps -elf | grep 'mosquitto_sub.*70B3D57ED0000AD8' | awk '{for(i=1;i<15;i++) $i="";print}' | grep 'staging'`
if [ $MOSQUITTO_TK_PS -ge 2 ];
then
    echo $MOSQUITTO_TK >> /home/numascale/BACKUP_DB_scripts/date.txt 
    echo $MOSQUITTO_TK 
else 
    echo "PROBLEM: The MQTT msg collection with Mosquitto is not running for TRONDHEIM" >> /home/numascale/BACKUP_DB_scripts/date.txt
    echo "PROBLEM: The MQTT msg collection with Mosquitto is not running for TRONDHEIM" 
    screen -S mosquitto_TRONDHEIM-DEPLOY -d -m /usr/bin/bash /home/climathon09/MQTT_temp/mosquitto_TRONDHEIM-deploy.sh
    #mail -s "The MQTT msg collection with Mosquitto stopped running for TRONDHEIM. Restarting..." pm@aiascience.com
fi
