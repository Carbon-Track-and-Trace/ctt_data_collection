echo `date` >> /home/numascale/scripts/date.txt
TRONDHEIM_PS=`ps -elf | grep python | grep TRONDHEIM | wc -l`
TRONDHEIM=`ps -elf | grep python | grep TRONDHEIM | awk '{print $16}'`
if [ $TRONDHEIM_PS -ge 1 ];
then
    echo $TRONDHEIM >> /home/numascale/scripts/date.txt 
else 
    echo "Trondheim script is not running" >> /home/numascale/scripts/date.txt
    screen -S sensor_collect_TRONDHEIM_deploy -d -m /home/climathon09/ctt_data_collection_TRONDHEIM/collectSensorDataCTT.py
fi

VEJLE_PS=`ps -elf | grep python | grep VEJLE | wc -l`
VEJLE=`ps -elf | grep python | grep VEJLE | awk '{print $16}'`
if [ $VEJLE_PS -ge 1 ];
then
    echo $VEJLE >> /home/numascale/scripts/date.txt
else 
    echo "VEJLE script is not running" >> /home/numascale/scripts/date.txt
#    screen -S sensor_collect_VEJLE_deploy -d -m /home/climathon09/ctt_data_collection_VEJLE/collectSensorDataCTT.py
fi
