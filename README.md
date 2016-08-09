# Collecting CTT data from its nodes into monetdb


Simply (re)run `collectSensorDataCTT.py` after adding the nodes' ID in the `init_data()` function.

* It first create the tables in the monetdb database,
* then it reads the historical sensor data from TheThingsNetwork REST API, and store them in monetdb,
* for every new type of sensor appearing in the sensor's payload, a new column in monetdb is automatically created,
* finally an infinite loop waits for any single new MQTT message sent by the sensors and stores each one in the database as well.