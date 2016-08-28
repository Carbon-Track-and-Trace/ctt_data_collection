#! /usr/bin/env python
# -*- coding: utf-8 -*-

""" 
  Script to build/update the CTT database 
    ~~~~~~~~
    :copyright: (c) 2016 by Patrick Merlot.
"""

# checking if pymonetdb present
import sys, os
import string
import logging
import pprint
import numpy as np
from datetime import datetime
from dateutil.parser import parse
from collections import defaultdict
from itertools import groupby
from collections import OrderedDict
import json 
import jsonschema
import pytz
import errno
from socket import error as socket_error
try:
    import pymonetdb
except ImportError:
    logging.warning("monetdb python API not found, using local monetdb python API")
    here = os.path.dirname(__file__)
    parent = os.path.join(here, os.pardir)
    sys.path.append(parent)
    import pymonetdb

DEBUG = False
myLogLevel = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'warning': logging.WARNING,
              'error': logging.ERROR,
              'critical': logging.CRITICAL}
logging.basicConfig(level=myLogLevel['debug'])

MAPIPORT = int(os.environ.get('MAPIPORT', 50000))
CTTDB = os.environ.get('CTT_DB', 'ctt')
TSTHOSTNAME = os.environ.get('CTT_HOSTNAME', 'localhost')
TSTUSERNAME = os.environ.get('CTT_USERNAME', 'co2')
TSTPASSWORD = os.environ.get('CTT_PASSWORD', 'ctt')
SYSUSERNAME = os.environ.get('SYS_USERNAME', 'monetdb')
SYSPASSWORD = os.environ.get('SYS_PASSWORD', 'monetdb')
rows = 10

def open_connection(database=CTTDB, port=MAPIPORT, hostname=TSTHOSTNAME,
                    username=TSTUSERNAME, password=TSTPASSWORD,
                    autocommit=False):
    # Connect to the database
    connect_args = ()
    connect_kwargs = dict(database=database, port=port, hostname=hostname,
                          username=username, password=password,
                          autocommit=autocommit)
    db = None
    try:
        db = pymonetdb.connect(*connect_args, **connect_kwargs)
    except socket_error as serr:
        if serr.errno != errno.ECONNREFUSED:
            # Not the error we are looking for, re-raise
            raise serr
            # connection refused
            # handle here
        msg  = "You maybe forgot to start monetdb, try this command:\n"
        msg += "\tmonetdbd start <fullpath/to/monetdb/databases>\n"
        msg += "like this example:\n\tmonetdbd start /home/patechoc/monetdb\n"
        logging.critical(msg)
        sys.exit()
    return db

def backup_DB(backup_path):
    today = datetime.today()
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)

    # msqldump --database=ctt --user=co2 --describe > /tmp/2016-07-19_backupMyDB.sql
    msg = "Dump Schema: enter password for accessing CTT database with user 'co2'"
    logging.info(msg)
    filepath = "{path}/{day}\_backupSchemaDB.sql".format(path=backup_path,
                                                         day=today.strftime("%Y-%m-%d"))
    print filepath
    command = "msqldump --database=ctt --user=co2 --describe > "+ filepath
    print command
    os.system(command)

    # mclient -u co2 -d ctt --dump > /tmp/2016-07-19_dump.sql
    msg = "Dump database: enter password for accessing CTT database with user 'co2'"
    logging.info(msg)
    filepath = "{path}/{day}\_backupDataDB.sql".format(path=backup_path,
                                                       day=today.strftime("%Y-%m-%d"))
    print filepath
    command = "mclient -u co2 -d ctt --dump > " + filepath
    print command
    os.system(command)

    
def table_exists(db, name):
    try:
        cursor = db.cursor()
        cursor.execute('select * from %s where 1=0' % name)
    except:
        connection = db
        connection.rollback()
        return False
    else:
        return True

def close_connection(db):
    connection = db
    connection.close()


def delete_table(db, tableName, doCommit = False):
    """ 
      Delete a table.
    """
    create_table_extra = 'CASCADE'
    if not(table_exists(db, tableName)):
        if DEBUG:
            msg = "table %s does not exist" % (tableName)
            logging.info(msg)
        return False
    cursor = db.cursor()
    cursor.execute('DROP TABLE %s %s' %
                   (tableName,
                    create_table_extra))
    if doCommit:
        connection = db
        connection.commit()
    return True


def create_table(db, tableName, columndefs, doCommit = False):
    """ Create a table using a list of column definitions given in
    columndefs.
    """
    create_table_extra = ''
    if table_exists(db, tableName):
        if DEBUG:
            msg = "table %s already exists" % (tableName)
            logging.info(msg)
        return False
    cursor = db.cursor()
    cursor.execute('CREATE TABLE %s (%s) %s' %
                   (tableName, ',\n'.join(columndefs),
                    create_table_extra))
    if doCommit:
        db.commit()
    return True

def add_table_column(db, tableName, columndef, doCommit = True):
    """ Add a column to a table using a column definition given in
    columndef.
    """
    # e.g. columndef = 'col1 INT'
    # e.g. columndef = 'col2 VARCHAR(255)'
    if not(table_exists(db, tableName)):
        if DEBUG:
            msg = "table %s does not exist: unable to add a column to it" % (tableName)
            logging.info(msg)
        return False
    cursor = db.cursor()
    # alter table a add column b2 double;
    query = 'ALTER TABLE {tab} ADD COLUMN {cDef}'.format(tab=tableName, cDef=columndef)
    try:
        if DEBUG:
            logging.info(query)
        cursor.execute(query)
    except pymonetdb.exceptions.OperationalError:
        if DEBUG:
            msg = 'The colum "{c}" already exists in table {tab}!\n'.format(c=columndef,
                                                                        tab=tableName)
            logging.info(msg)
        db.rollback()
    if doCommit:
        db.commit()
    return True

def generator(row, col):
    """ Generates a suitable data object for insertion
    into the table taking arguments (row_number,
    col_number).
    """
    if col == 0:
        return row
    else:
        return ('%i' % (row%10))*255

def add_entries(db, tableName, listDict, commit=True):
    # sql>INSERT INTO locations(placename, address) VALUES ('Contrexeville', 'rue reine isabelle');
    descp = get_description_table(db, tableName)
    #pprint.pprint(descp)
    tableFields = [t[0] for t in descp]
    for entry in listDict:
        insert_statement  = 'INSERT INTO {table}'.format(table=tableName)
        fields = [f for f in entry.keys() if f in tableFields]
        insert_statement += '({f_list}) '.format(f_list=','.join(s.encode(encoding='UTF-8',errors='strict') for s in fields))
        insert_statement += 'VALUES ({valFields}) '.format(valFields=','.join(["'"+str(entry[f]).encode(encoding='UTF-8',errors='strict')+"'" for f in fields]))
        if DEBUG:
            logging.info("\n" + insert_statement)
        cursor = db.cursor()
        #cursor.executemany(insert_statement, data)
        cursor.execute(insert_statement)
        if commit == True:
            db.commit()

def update_entry(db, tableName, entryDict, whereKey, commit=True):
    # sql>UPDATE locations SET altitude=45.6, country='Norway' WHERE placename='Olavskvartalet';
    descp = get_description_table(db, tableName)
    #pprint.pprint(descp)
    tableFields = [t[0] for t in descp]
    update_query  = 'UPDATE {table} SET'.format(table=tableName)
    fields = [f for f in entryDict.keys() if f in tableFields and entryDict[f] != None and f != whereKey]
    keyValuePairs = [f + "='" + str(entryDict[f]).encode(encoding='UTF-8',errors='strict') + "'" for f in fields]
    update_query += " {keyValuePairs}".format(keyValuePairs=', '.join(keyValuePairs))
    update_query += " WHERE {key}='{val}'".format(key=whereKey, val=str(entryDict[whereKey]).encode(encoding='UTF-8',errors='strict'))
    if DEBUG:
        logging.info("\n" + update_query)
    cursor = db.cursor()
    cursor.execute(update_query)
    if commit == True:
        db.commit()

    
def check_data_integrity(db, columndefs, generatorX):
    tableName = "checkDataIntegrity"
    create_table(db, tableName, columndefs)
    insert_statement = ('INSERT INTO %s VALUES (%s)' %
                        (tableName,
                         ','.join(['%s'] * len(columndefs))))
    data = [ [ generator(i,j) for j in range(len(columndefs)) ]
             for i in range(rows) ]
    cursor = db.cursor()
    cursor.executemany(insert_statement, data)
    db.commit()
    # verify
    cursor.execute('select * from %s' % tableName)
    l = cursor.fetchall()
    msg = "%(nbRows)03d should be equal to %(rows)03d" % {"nbRows":len(l),
                                                          "rows":rows}
    logging.info(msg)
    try:
        for i in range(rows):
            for j in range(len(columndefs)):
                msg = "%(ind)03d should be equal to %(gen)03d" % {"ind":l[i][j],
                                                                  "gen":generator(i,j)}
                logging.info(msg)
    finally:
        cursor.execute('DROP TABLE %s' % (tableName))


def test_DECIMAL(db):
    # DECIMAL
    def generator2(row,col):
        from decimal import Decimal
        return Decimal("%d.%02d" % (row, col))
    check_data_integrity(db,
                         ('col1 DECIMAL(5,2)',),
                         generator2)

def test_create_table(db, tableName):
    columndefs = ( 'col1 INT', 'col2 VARCHAR(255)')
    create_table(db, tableName, columndefs)
    insert_statement = ('INSERT INTO %s VALUES (%s)' %
                        (tableName,
                         ','.join(['%s'] * len(columndefs))))
    data = [ [ generator(i,j) for j in range(len(columndefs)) ]
             for i in range(rows) ]
    if DEBUG:
        pprint.pprint(data)
    cursor = db.cursor()
    cursor.executemany(insert_statement, data)
    # verify
    connection = db
    connection.commit()
    cursor.execute('select * from %s' % tableName)
    l = cursor.fetchall()
    if DEBUG:
        pprint.pprint(l)
        msg = "%(fetchedAll)03d should be equal to %(rows)03d" % {"fetchedAll":len(l),
                                                                  "rows":rows}
        logging.debug(msg)

def test_delete_table(db, tableName):
    delete_table(db, tableName)
    connection = db
    connection.commit()


def test_transactions(db):
    columndefs = ( 'col1 INT', 'col2 VARCHAR(255)')
    tableName = "tutu"
    delete_table(db, tableName)
    create_table(db, tableName, columndefs)
    insert_statement = ('INSERT INTO %s VALUES (%s)' %
                        (tableName,
                         ','.join(['%s'] * len(columndefs))))
    data = [ [ generator(i,j) for j in range(len(columndefs)) ]
             for i in range(rows) ]
    if DEBUG:
        pprint.pprint(data)
    cursor = db.cursor()
    cursor.executemany(insert_statement, data)
    # verify
    connection = db
    connection.commit()
    cursor.execute('select * from %s' % tableName)
    l = cursor.fetchall()
    if DEBUG:
        pprint.pprint(l)
        print "%(fetchedAll)03d should be equal to %(rows)03d" % {"fetchedAll":len(l),
                                                                  "rows":rows}
    #print "fetchall:", l
    for i in range(rows):
        for j in range(len(columndefs)):
            if l[i][j] != generator(i,j):
                if DEBUG:
                    print "{} different from {}".format(l[i][j], generator(i,j))
    delete_statement = 'delete from %s where col1=%%s' % tableName
    cursor.execute(delete_statement, (0,))
    cursor.execute('select col1 from %s where col1=%s' % \
                   (tableName, 0))
    l = cursor.fetchall()
    if l == False:
        "DELETE didn't work"
    connection.rollback()
    cursor.execute('select col1 from %s where col1=%s' % \
                   (tableName, 0))
    l = cursor.fetchall()
    if len(l) != 1:
        print "len(l) = {} != 1".format(len(l))
        print "ROLLBACK didn't work"
    cursor.execute('drop table %s' % (tableName))

def drop_CTT_tables(db, commit=True):
    tables = ['sensors', 'node_msg', 'nodes',
              'gateway_msg', 'gateways', 'locations']
    for tab in tables:
        if table_exists(db, tab):
            delete_table(db, tableName=tab, doCommit=commit)
            #query = "DROP TABLE "+ tab
            #run_SQLquery(db, query, commit=commit)


def create_CTT_tables(db, commit=True):
    """ 
       Create the tables that will store CTT sensor data
    """
    # map of gateways: http://ha-23.eradus.eu/croft.html
    # https://www.thethingsnetwork.org/api/v0/gateways/AA555A0008060252/
    # https://www.thethingsnetwork.org/api/v0/gateways/AA555A0008060353/
    # https://www.thethingsnetwork.org/api/v0/nodes/02032201/
    # https://www.thethingsnetwork.org/api/v0/nodes/02032220/
    # https://www.thethingsnetwork.org/api/v0/nodes/02032221/
    # https://www.thethingsnetwork.org/api/v0/nodes/02032222/
    # http://www.libelium.com/development/waspmote
    TABLES = [{'name':'locations',
               'columndefs': ('id INT AUTO_INCREMENT PRIMARY KEY',
                              'placename VARCHAR(60) NOT NULL',
                              'description VARCHAR(200)',
                              'latitude DECIMAL(11,8) ',
                              'longitude DECIMAL(11,8) ',
                              'altitude DECIMAL ',
                              'address VARCHAR(100) ',
                              'zipcode VARCHAR(60) ',
                              'city VARCHAR(60) ',
                              'country VARCHAR(60) ')},
              {'name':'gateways',
               'columndefs': ('id INT AUTO_INCREMENT PRIMARY KEY',
                              'gateway_eui VARCHAR(30) NOT NULL UNIQUE',
                              'location_id INT NOT NULL')},
              {'name':'gateway_msg',
               'columndefs': ('id INT AUTO_INCREMENT PRIMARY KEY',
                              'gateway_eui VARCHAR(30) NOT NULL UNIQUE',
                              'location_id INT ',
                              'latitude DECIMAL(11,8) NOT NULL',
                              'longitude DECIMAL(11,8) NOT NULL',
                              'altitude DECIMAL NOT NULL',
                              'time VARCHAR(30) NOT NULL',
                              'rxforwarded INT NOT NULL',
                              'rxcount INT NOT NULL',
                              'rxok INT NOT NULL',
                              'ackratio DECIMAL NOT NULL',
                              'datagramssent INT NOT NULL',
                              'datagramsreceived INT NOT NULL')},
              {'name':'nodes',
               'columndefs': ('id INT AUTO_INCREMENT PRIMARY KEY',
                              'node_eui VARCHAR(20) NOT NULL UNIQUE',
                              'location_id INT ',
                              'datarate VARCHAR(10)')},
              {'name':'sensors',
               # http://www.libelium.com/downloads/documentation/data_frame_guide.pdf
               # ... .com/calibrated-air-quality-gas-dust-particle-matter-pm10-smart-cities/
               # ... .com/downloads/documentation/gases_sensor_board_2.0.pdf
               'columndefs': ('id INT AUTO_INCREMENT PRIMARY KEY',
                              'SensorTAG VARCHAR(30) NOT NULL UNIQUE',
                              'SensorID_ASCII  VARCHAR(60) NOT NULL UNIQUE',
                              'SensorREF  VARCHAR(60) NOT NULL UNIQUE',
                              'SensorID_BINARY INT NOT NULL UNIQUE',
                              'SensorNAME VARCHAR(60) NOT NULL',
                              'SensorUnit VARCHAR(10) NOT NULL',
                              'SensorBinType VARCHAR(10) DEFAULT NULL',
                              'SensorBinNbFields INT DEFAULT NULL',
                              'SensorBinBytesPerField INT DEFAULT NULL',
                              'description STRING DEFAULT NULL')},
              {'name':'node_msg',
               'columndefs': ('id INT AUTO_INCREMENT PRIMARY KEY',
                              'node_eui VARCHAR(20) NOT NULL',
                              'gateway_eui VARCHAR(30) NOT NULL',
                              'timestring VARCHAR(30) NOT NULL',
                              'timestamptz TIMESTAMP  NOT NULL',
                              #'data_base64 STRING NOT NULL',
                              #'data_decoded STRING NOT NULL',
                              'datarate VARCHAR(10)',
                              'frequency DECIMAL',
                              'snr DECIMAL',
                              'rssi DECIMAL',
                              'serial_id VARCHAR(30)',
                              'waspmote_id VARCHAR(60)',
                              'sequence_Num INT ',)}]
                              #'SENSOR_GP_CO2 DECIMAL',
                              #'SENSOR_GP_NO2 DECIMAL',
                              #'SENSOR_BAT INT ')}]

    for tab in TABLES:
        #if not(table_exists(db, tab['name'])):
        create_table(db, tab['name'], tab['columndefs'], doCommit=commit)

    # Add foreign keys to link the tables
    add_constraint(db, tableName="gateways", nameConstraint="gateways_location_id_fkey",
                   columnName="location_id", typeTableConstraint="FOREIGN KEY",
                   refTableName="locations", refColumnName="id")
    add_constraint(db, tableName="gateway_msg", nameConstraint="gateway_msg_location_id_fkey",
                   columnName="location_id", typeTableConstraint="FOREIGN KEY",
                   refTableName="locations", refColumnName="id")
    add_constraint(db, tableName="nodes", nameConstraint="nodes_location_id_fkey",
                   columnName="location_id", typeTableConstraint="FOREIGN KEY",
                   refTableName="locations", refColumnName="id")
    # add_constraint(db, tableName="node_msg", nameConstraint="node_msg_node_eui_fkey",
    #                columnName="node_eui", typeTableConstraint="FOREIGN KEY",
    #                refTableName="nodes", refColumnName="node_eui")
    # add_constraint(db, tableName="node_msg", nameConstraint="node_msg_gateway_eui_fkey",
    #                columnName="gateway_eui", typeTableConstraint="FOREIGN KEY",
    #                refTableName="gateways", refColumnName="gateway_eui")
    add_constraint(db, tableName="node_msg", nameConstraint="node_msg_uq",
                   typeTableConstraint="UNIQUE", uniqueCols=["node_eui", "timestring"])


def get_constraints():
    db_sys = open_connection(database=CTTDB, port=MAPIPORT, hostname=TSTHOSTNAME,
                             username=SYSUSERNAME, password=SYSPASSWORD,
                             autocommit=True)
    query = "select * from keys"
    consts = get_result_SQLquery(db_sys, query, commit=False)
    close_connection(db_sys)
    return consts

def constraint_exists(nameConstraint):
    consts = get_constraints()
    return any(d['name'] == nameConstraint for d in consts)


def add_constraint(db, tableName, nameConstraint, typeTableConstraint,
                   columnName="", refTableName="", refColumnName="",
                   uniqueCols=None, commit=True):
    """
      e.g. ALTER TABLE "node_msg" ADD CONSTRAINT "node_msg_gateway_eui_fkey" FOREIGN KEY ("gateway_eui") REFERENCES gateways(gateway_eui)'
    """
    if constraint_exists(nameConstraint) == False:
        query  = "ALTER TABLE " + tableName + " "
        query += "ADD CONSTRAINT " + nameConstraint + " "
        query += typeTableConstraint 
        if string.lower(typeTableConstraint).strip() == "foreign key":
            query += " (" + columnName + ") "
            query += "REFERENCES " + refTableName + " (" + refColumnName + ")"
        elif string.lower(typeTableConstraint).strip() == "unique":
            query += " (" + ", ".join(uniqueCols) + ") "
        if DEBUG:
            logging.info(query)
        cursor = db.cursor()
        cursor.execute(query)
        if commit == True:
            db.commit()
    else:
        if DEBUG:
            msg = "constraint '{const}' already exists in table '{table}'".format(const=nameConstraint,
                                                                                  table=tableName)
            logging.info(msg)

def add_location(db, placename, description=None,
                 latitude=None, longitude=None, altitude=None,
                 address=None, zipcode=None, city=None, country=None):
    location = {'placename':placename,
                'description':description,
                'latitude':latitude, 'longitude':longitude, 'altitude':altitude,
                'address':address, 'zipcode':zipcode, 'city':city, 'country':country}
    fieldsNotNone = [f for f in location.keys() if location[f] != None]
    locations = [dict((k, location[k]) for k in fieldsNotNone)]
    if DEBUG:
        msg = "add location: "
        logging.info(msg)
        logging.info(locations[0])
    add_entries(db, tableName="co2.locations", listDict=locations, commit=True)


def upsert_location(db, placename, description=None,
                    latitude=None, longitude=None,
                    altitude=None, address=None, zipcode=None, city=None, country=None):
    """
    Either update a row on the basis of the row already existing, or insert a row instead.
    """   
    query = ""
    if latitude != None and longitude !=None:
        geoPrec = 5e-05
        query = "SELECT * FROM co2.locations WHERE (latitude BETWEEN {lat1} AND {lat2}) AND (longitude BETWEEN {long1} AND {long2}) LIMIT 1".format(lat1=latitude-geoPrec,
                                                                                                                                                    lat2=latitude+geoPrec,
                                                                                                                                                    long1=longitude-geoPrec,
                                                                                                                                                    long2=longitude+geoPrec)
    else:
        query = "SELECT * FROM co2.locations WHERE placename='{m}'  LIMIT 1".format(m=placename)
    if DEBUG:
        logging.info(query)
    res = get_result_SQLquery(db, query, commit=True)
    # add the new entry
    if len(res) == 0:
        add_location(db, placename=placename, description=description,
                     latitude=latitude, longitude=longitude, altitude=altitude,
                     address=address, zipcode=zipcode, city=city, country=country)
    # update the existing entry
    else:
        newLocation = {'placename':placename, 'description':description,
                       'latitude':latitude, 'longitude':longitude,
                       'altitude':altitude, 'address':address, 'zipcode':zipcode, 'city':city, 'country':country}
        update_entry(db, tableName='co2.locations', entryDict=newLocation, whereKey='placename', commit=True)
    loc = get_result_SQLquery(db, query, commit=True)
    return loc


def add_gateway(db, gateway_eui, placename="", latitude=None, longitude=None,
                altitude=None, address=None, zipcode=None, city=None, country=None):
    location_id = None
    # find existing location in DB
    res = upsert_location(db, placename=placename, description="gateway_"+gateway_eui,
                          latitude=latitude, longitude=longitude, altitude=altitude,
                          address=address, zipcode=zipcode, city=city, country=country)
    gateways=[{'gateway_eui':gateway_eui,
              'location_id':res[0]['id']}]
    if DEBUG:
        msg = "add gateway: "
        logging.info(msg)
        pprint.pprint(gateways[0])
    try:
        add_entries(db, tableName="co2.gateways", listDict=gateways, commit=True)
    except pymonetdb.exceptions.IntegrityError:
        if DEBUG:
            msg = 'The gateway "{g}" already exists!\n'.format(g=gateway_eui)
            logging.info(msg)
        db.rollback()
        
    
def add_gateway_message():
    pass
    

def add_node(db, node_eui, placename="", datarate=None,
             latitude=None, longitude=None,
             altitude=None, address=None, zipcode=None, city=None, country=None):
    location_id = None
    # find existing location in DB
    res = upsert_location(db, placename=placename, description="node_"+node_eui,
                          latitude=latitude, longitude=longitude, altitude=altitude,
                          address=address, zipcode=zipcode, city=city, country=country)
    node={'node_eui':node_eui,
          'datarate':datarate,
          'location_id':res[0]['id']}
    fieldsNotNone = [f for f in node.keys() if node[f] != None]
    nodes = [dict((k, node[k]) for k in fieldsNotNone)]
    if DEBUG:
        msg = "add node: "
        logging.info(msg)
        pprint.pprint(nodes[0])
    try:
        add_entries(db, tableName="co2.nodes", listDict=nodes, commit=True)
    except pymonetdb.exceptions.IntegrityError:
        if DEBUG:
            msg = "The node '{n}' already exists".format(n=node_eui)
            logging.info(msg)
        db.rollback()


def add_node_message(db, msg, commit=False):
    timestring = msg['gateway_time']
    try:
        timestamptz = datetime.strptime(timestring, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        try:
            timestamptz = datetime.strptime(timestring, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            no_nanoseconds = timestring[:-4]+"Z"
            timestamptz = datetime.strptime(no_nanoseconds, '%Y-%m-%dT%H:%M:%S.%fZ')

    msg['timestamptz'] = timestamptz
    msg['timestring'] = timestring

    #data_base64 = str(msg['data'])
    #data_decoded = msg['data_decoded']
    # gateway_eui = msg['gateway_eui']
    # datarate = msg['datarate']
    # frequency = msg['frequency']
    # lsnr = msg['lsnr']
    # rssi = msg['rssi']
    #serial_id    = msg['serial_id']
    #waspmote_id  = msg['waspmote_id']
    #sequence_Num = msg['sequence_Num']
    #sensors      = msg['sensors']
    #sensorKeys   = sensors.keys()


    # check if all keys exist as column names in table 'node_msg'
    # if not create them
    labels = msg.keys()
    descp = get_description_table(db, tableName='node_msg')
    tableFields = [t[0] for t in descp]
    missingCols = [sk for sk in labels if sk not in tableFields]
    for col in missingCols:
        colType = type(msg[col])
        if colType == int:
            mdbColType = "INT"
        elif colType == float:
            mdbColType = "DECIMAL"
        else:
            mdbColType = "STRING"
        columndef = col + " " + mdbColType
        print "Missing column: '", col, "', of type: ", mdbColType
        add_table_column(db, tableName='node_msg', columndef=columndef, doCommit=True)

    # update 'node' info (datarate) for each new message
    # eventually add 'gateway' if new gateway_eui detected
    
    # add the node_message as new entry
    # node_msg = {'node_eui':node_eui, 'gateway_eui':gateway_eui,
    #             'timestring':timestring, 'timestamptz':timestamptz,
    #      #       'data_base64':data_base64, 'data_decoded':data_decoded, 
    #             'snr':snr, 'rssi':rssi, 'datarate':datarate, 'frequency':frequency,
    #             'serial_id':serial_id, 'waspmote_id':waspmote_id, 'sequence_Num':sequence_Num}
    #node_msg.update(sensors)
    #fieldsNotNone = [f for f in node_msg.keys() if node_msg[f] != None]
    fieldsNotNone = [f for f in msg.keys() if msg[f] != None]
    node_messages = [dict((k.lower(), msg[k]) for k in fieldsNotNone)]
    if DEBUG:
        msg = "add node message: "
        logging.info(msg)
        pprint.pprint(node_messages[0])
    try:
        add_entries(db, tableName="co2.node_msg", listDict=node_messages, commit=True)
    except pymonetdb.exceptions.IntegrityError:
        if DEBUG:
            msg = "The node '{n}' already exists".format(n=node_eui)
            logging.info(msg)
        db.rollback()


    
def get_sensor_value_type():
    """ a function to deduce dynamically the type of new sensor detected in the payload
    INT?, DECIMAL?, STRING otherwise.
    # <=>#397C530E695B4A72#NTNU_CTT_22#1#GP_CO2:616.688#GP_NO2:0.000#PM1:-9999.0000#PM2_5:-9999.0000#PM10:-9999.0000#
    return the name (GP_NO2 >> SENSOR_GP_NO2), the datatype (INT, DECIMAL, STRING) and
    the value to be stored in monetdb
    """
    pass

def get_columnNames(tableName):
    pass

def add_column(tableName, columnName, columnDataType, options="DEFAULT NULL"):
    pass
        
def get_gateway_messages():
    pass

def get_node_messages():
    pass

def get_foreign_keys(db):
    """ returns a list of the foreign keys 
    """
    # https://www.monetdb.org/Documentation/SQLcatalog/ObjectsKeysIndices
    # sql>select name from keys where type=2;
    query = 'select * from keys where type=2'
    res = get_result_SQLquery(db, query)
    return [r['name'] for r in res]

def run_SQLquery(db, query, commit=False):
    """ executes an SQL query
    """
    cursor = db.cursor()
    cursor.execute(query)
    if commit == True:
        db.commit()


def get_result_SQLquery(db, query, commit=False):
    """ returns an SQL query as a dictionary  
    """
    cursor = db.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    descp = cursor.description
    if commit == True:
        db.commit()
    fields = [field[0] for field in cursor.description]
    lDict = []
    for row in result:
        nDict={}
        for i, field in enumerate(fields):
            nDict[field]=row[i]
        lDict.append(nDict)
    return lDict

def get_description_table(db, tableName):
    """ returns the list of fields of a table, with their type, size, ... 
    """
    cursor = db.cursor()
    query = "select * from "+tableName
    cursor.execute(query)
    descp = cursor.description
    db.commit()
    return descp

def tests():
    db_ctt = open_connection()
    drop_CTT_tables(db_ctt)
    create_CTT_tables(db_ctt)
    cursor = db_ctt.cursor()
    if DEBUG:
        msg = "Table 'locations' exists? ", table_exists(db_ctt, "locations")
        logging.info(msg)
    tableName = "node_msg"
    if DEBUG:
        msg = "Description table 'node_msg'"
        logging.info(msg)
        pprint.pprint( get_description_table(db_ctt, tableName))
    query = "select * from co2.node_msg WHERE 1=0"
    if DEBUG:
        logging.info(get_result_SQLquery(db_ctt, query))
        logging.info(get_foreign_keys(db_ctt))
    locations = [{'placename':'Studentersamfundet',
                  'description':'gateway_AA555A0008060252',
                  'latitude':63.422511, 'longitude':10.395165},
                 {'placename':'Olavskvartalet',
                  'description':'gateway_AA555A0008060353',
                  'latitude':63.433737, 'longitude':10.403894}]

    # CTT GATEWAYS
    add_gateway(db=db_ctt, gateway_eui='AA555A0008060353', 
                placename='Olavskvartalet', latitude=63.433737, longitude=10.403894)
    add_gateway(db=db_ctt, gateway_eui='AA555A0008060252', 
                placename='Studentersamfundet', latitude=63.422511, longitude=10.395165,
                country='Norway')
    
    add_gateway(db=db_ctt, gateway_eui='nummer3', 
                placename='Studentersamfundet', latitude=63.422511, longitude=10.395165,
                country='France')
    # CTT NODES
    add_node(db=db_ctt, node_eui='02032201', placename='node_02032201')
    add_node(db=db_ctt, node_eui='02032220', placename='node_02032220')
    add_node(db=db_ctt, node_eui='02032221', placename='node_02032221')
    add_node(db=db_ctt, node_eui='02032222', placename='node_02032222')

    nodeChange={'location_id':5, 'node_eui':"02032201", 'datarate':"SF4334"}
    update_entry(db=db_ctt, tableName="nodes", entryDict=nodeChange,
                 whereKey="node_eui", commit=True)
    
    db_ctt.commit()
    close_connection(db_ctt)
    
    db_sys = open_connection(database=CTTDB, port=MAPIPORT, hostname=TSTHOSTNAME,
                             username=SYSUSERNAME, password=SYSPASSWORD,
                             autocommit=True)
    myRes = get_foreign_keys(db_sys)
    close_connection(db_sys)
    

if __name__ == "__main__": 
    test()
