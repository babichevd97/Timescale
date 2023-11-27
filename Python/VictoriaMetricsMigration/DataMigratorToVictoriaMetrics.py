import time
import json
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
import sys
from io import StringIO
import connections
import requests

#------ Prerequisites on VM server-side to perform migration: ------#
#Create necessary tables for metrics (you can just redirect (or duplicate) for several minutes your TimescaleDB traffic to newly created VM - cluster to create VM-storage-objects)

#You can get data from each datanode in parallel, if you have an multicluster - setup
#Run this code on each datanode - server

##Initialize our class. Get connection from connections.py
class DataMigration:
    def __init__(self, metric_name, itersize=500000):
        self.metric_name = metric_name
        self.itersize = itersize
        self.connAN = connections.connAN_STAGE_DC1
        self.connDN = connections.connDN1_STAGE_DC1
        self.VM_HOST = connections.VM_HOST
        self.VM_PORT = connections.VM_PORT
        self.VM_INSERT_QUERY = connections.VM_INSERT_QUERY
        self.metric_id = None

    def get_metric_id(self):
        # Get metric ID from TimescaleDB
        print('Working with metric: %s' % self.metric_name)
        print("----------------------------------------------------")
        with self.connAN.cursor() as cur:
            cur.execute('SELECT id FROM _prom_catalog.metric WHERE metric_name = %s', (self.metric_name,))
            self.metric_id = cur.fetchone()[0]
        cur.close()

    def create_history_schema(self, conn): #Creating a history schema for temporary tables in order to reduce the load from calling the labels method
        with conn.cursor() as cur:
            print("Check if hictory schema is created")
            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name='history';")
            schema_result = cur.fetchall()
            if not schema_result:
                print("Creating hictory schema")
                print("----------------------------------------------------\n")
                cur.execute("CREATE SCHEMA history")
                conn.commit()
            else: 
                print("Schema history already exists")
                print("----------------------------------------------------\n")
        cur.close()

    def create_tmp_table(self, conn): #Creating a temporary table in order to reduce the load from calling the labels method
    #The history-scheme must first be created
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'history' AND table_name = %s",
                        ('temp_table_' + str(self.metric_id),))
            table_result = cur.fetchall()
            if not table_result:
                print("Creating temp table for metric_id - temp_table_%s" % (self.metric_id))
                print("----------------------------------------------------\n")
                cur.execute("CREATE TABLE history.temp_table_%s (series_id int8 PRIMARY KEY, json_data jsonb NOT NULL);" %
                            (self.metric_id,))
            else: 
                print("Table temp_table_%s already exists" % (self.metric_id))
                print("----------------------------------------------------\n")
                conn.commit()
        cur.close()

    def insert_into_tmp_table(self, conn): #Insert data into a temporary table in order to reduce the load from calling the labels method
    #The history-scheme must first be created
        with conn.cursor() as cur:
            print("Check if AN table is empty or not")
            print("Inserting series data into table")
            cur.execute("SELECT count(*) FROM history.temp_table_%s;" % (self.metric_id,))
            table_rows_result = cur.fetchone()
            if table_rows_result[0] == 0:
                cur.execute("INSERT INTO history.temp_table_%s (series_id,json_data) SELECT id, jsonb(labels) "
                            "FROM _prom_catalog.series WHERE metric_id = %s;" % (self.metric_id, self.metric_id))
                conn.commit()
                print("Table filled")
                print("----------------------------------------------------\n")
            else: 
                print("Table temp_table_%s is already prefilled" % (self.metric_id))
                print("----------------------------------------------------\n")
        cur.close()

    def copy_history_table_from_an_to_dn(self): #Copies created history-table from AN to DN. Stand-alone foreign tables are forbidden in TSDB
        # Check and create history schema on AN and DN
        self.create_history_schema(self.connAN)
        self.create_history_schema(self.connDN)

        # Check and create history tables on DN
        self.create_tmp_table(self.connDN)
        self.create_tmp_table(self.connAN)
        self.insert_into_tmp_table(self.connAN)

        # Copy data from AN to DN
        with self.connAN.cursor() as cur1, self.connDN.cursor() as cur2:
            print("Check if DN table is empty or not")
            cur2.execute("SELECT count(*) FROM history.temp_table_%s;" % (self.metric_id,))
            table_rows_result = cur2.fetchone()
            if table_rows_result[0] == 0:
                print("Inserting series data into DN table")
                text_stream = StringIO()
                cur1.copy_expert('COPY (SELECT * FROM history.temp_table_%s) TO STDOUT' % (self.metric_id,), text_stream)
                text_stream.seek(0)
                cur2.copy_expert('COPY history.temp_table_%s FROM STDOUT' % (self.metric_id,), text_stream)
                self.connDN.commit()
                print("DN table filled")
                print("----------------------------------------------------\n")
            else: 
                print("DN Table temp_table_%s is already prefilled" % (self.metric_id))
                print("----------------------------------------------------\n")
        cur1.close()
        cur2.close()

    def generate_jsons_output_file(self): #Iterate fithrough sizeID pack, to generate json for them    
        with open("jsons_for_%s.json" % (self.metric_name), 'a') as f: #Create json file for generation
            for series_id in self.get_series_ids(self.connDN):
                with self.connDN.cursor() as selectCursor:
                    
                    selectCursor.itersize = self.itersize 
                    #The number of rows that the client will pull down at a time from the server side cursor. 
                    #The value should balance number of network calls versus memory usage on the client
                    #For example, if result count is three million, an itersize value of 2000 (the default value) will result in 1500 network calls

                    selectCursor.execute('DECLARE super_cursor BINARY CURSOR FOR select '
                                        '(EXTRACT(EPOCH from (timestamptz(time))) * 1000)::text, '
                                        'value::text, tsl.json_data::text '
                                        'from prom_data."%s" pd inner join history.temp_table_%s tsl '
                                        'on tsl.series_id=pd.series_id where tsl.series_id = %s' %
                                        (self.metric_name, self.metric_id, series_id))
                    
                    while True: #Iterate untill no more fetched rows are left  
                        selectCursor.execute("FETCH %s FROM super_cursor" % (self.itersize))
                        rows = selectCursor.fetchall()

                        #If no fetched rows returned - break
                        if not rows:
                            break
                        res_dict = { #Resulted generated dict for one specificID and specified number of fetched rows
                            "metric": {},
                            "values": [],
                            "timestamps": []
                        }
                        chunk_json = json.loads(rows[0][2])
                        res_dict["metric"] = chunk_json #add unique label, that will be pre-filled for further filling
                        for chunk in list(rows):

                            #Fill in dictionary
                            if (chunk[1] == 'NaN'):
                                res_dict["values"].append(None)
                            else:
                                res_dict["values"].append(float(chunk[1]))
                            res_dict["timestamps"].append(int(chunk[0]))
                        json.dump(res_dict, f) #Generate json from dict and write to file
                        f.write("\n")
                    selectCursor.execute('CLOSE super_cursor')
                    selectCursor.close()

    #Use series from DataNodes
    def get_series_ids(self, conn):
        series_ids = []
        with conn.cursor() as selectSeriesCursor:
            selectSeriesCursor.execute('SELECT distinct series_id FROM prom_data."%s";' % (self.metric_name,)) #get sorted distinct SeriesIds
            for row in selectSeriesCursor:
                series_ids.append(row[0])
        selectSeriesCursor.close()
        return series_ids

    def import_json_to_vm(self, json_file_name): #Import Json to VM
        url = f'{self.VM_HOST}:{self.VM_PORT}/{self.VM_INSERT_QUERY}'
        headers = {'Content-Type': 'application/json'}
        payload = open(json_file_name)
        response = requests.post(url, data=payload, headers=headers)
        return response.reason, response.status_code

    def drop_tmp_table(self, conn): #Drop tmp tables
        with conn.cursor() as cur_drop:
            cur_drop.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'history' "
                             "AND table_name='temp_table_%s';" % (self.metric_id,))
            table_result = cur_drop.fetchall()
            if not table_result:
                print(f'Table history.temp_table_{self.metric_id} is already dropped')
            else:
                print(f'Dropping table history.temp_table_{self.metric_id}')
                cur_drop.execute("DROP TABLE history.temp_table_%s;" % (self.metric_id,))
                conn.commit()
                print(f'Table history.temp_table_{self.metric_id} dropped!')
        cur_drop.close()

    def run_migration(self):  #Main Run Script
        # Get metric ID
        self.get_metric_id()

        # Copy history table from AN to DN
        self.copy_history_table_from_an_to_dn()

        # Generate JSONs
        self.generate_jsons_output_file()

        # Import JSONs to VM
        json_name = self.import_json_to_vm(f'jsons_for_{self.metric_name}.json')
        print(json_name)

        # Drop created tables
        self.drop_tmp_table(self.connAN)
        self.drop_tmp_table(self.connDN)
