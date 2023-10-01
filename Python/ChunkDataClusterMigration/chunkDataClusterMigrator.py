from sshtunnel import SSHTunnelForwarder
import time
import json
import math
import pandas as pd
import subprocess
import connections

metrics = ["node_cpu_seconds_total",
"protoBlockedBytesByApp",
"protoRecognizedBytes",
"protoBlockedSessionsSum",
"blockedSessionsByDpiList",
"econatSfpTxPower",
"trflog_received_logs_total",
"econatSfpVendorSerialNumber",
"econatSfpVendorPartNumber",
"ifHCOutPacketRateByInstance",
"ifHCInPacketRateByInstance",
"ifHCInMulticastPkts",
"node_network_receive_frame_total",
"node_network_up",
"node_network_transmit_fifo_total",
"node_network_transmit_drop_total",
"node_network_transmit_errs_total",
"node_network_transmit_compressed_total",
"ifHCInOctetsSum",
"recognizedSessionsByApp",
"protoRecognizedSessions",
"blockedSessionsByApp",
"ifHCOutOctetsSum",
"writers_ring_full_dropped_pkts",
"fanSpeedFront",
"rxErrors",
"up",
"ifHCOutRate",
"ifHCInRate",
"trflog_time_between_clickhouse_sends_seconds_bucket",
"trflog_logs_sent_to_clickhouse_total",
"trflog_endpoint_incorrect_record_total",
"hardwareVersion",
"deviceBypassFactorTime",
"LinkNetwork",
"node_disk_write_time_seconds_total",
"node_filesystem_files_free",
"node_filesystem_free_bytes",
"node_disk_io_now",
"node_disk_writes_completed_total",
"node_disk_read_bytes_total",
"econatTcpSynSentIpv6",
"econatDpiRKNactualDate",
"econatTcpSynCookieSentIpv6",
"econatTcpSynCookieSentIpv4",
"econatDnsRequestsTypeHttps",
"econatDnsIpv4Requests",
"econatTcpSynCookieEstablishedIpv6",
"econatFailedDownloadRknDelta",
"econatDpiRKNDumpPartitionUsed",
"econatTcpSynSentIpv4",
"econatTcpSynCookieInvalidIpv4",
"econatDnsRequestsTypeMx",
"econatDnsRequestsTypeAaaa",
"econatNixRevision",
"econatDpiRKNDumpPartitionSize",
"econatDpiRKNLastLoad",
"econatSerialNumber",
"econatCpFreeMemory",
"econatDpiPathBufsUsed",
"econatDnsRequestsTypePtr",
"econatDpiRKNupdateTime",
"econatTcpStateEstablishedPerSec",
"econatDnsRequestsTypeNs",
"econatPSUInformationStatus",
"econatTcpSynCookieEstablishedIpv4",
"econatCpTotalMemory",
"econatDpiHostBufsTotal",
"econatDpiBannedEgressTcp",
"econatTcpStateEstablished",
"econatDpCpuBurst",
"econatDpiRKNLastParse",
"econatDpFreeMemory",
"ifHCOutOctetsPort",
"ifHCInOctetsPort",
"qsfpTemperature",
"stateIsBypass",
"deviceBypassFactor",
"qsfpPartNumber",
"psuOutputVoltage",
"qsfpSerialNumber",
"passiveState",
"trflog_logs_ready_for_clickhouse_total",
"ifInErrors",
"ifOperStatus",
"ifOutErrors",
"ifHCOutUcastPkts",
"qsfpRxPowerDb",
"ifHCOutOctets",
"ifHCInOctets"]


##Get connections from connections.py
connAN_STAGE_DC1=connections.connAN_STAGE_DC1
connDN1_STAGE_DC1=connections.connDN1_STAGE_DC1

#PROD_DC1
print ('Start ssh')
serverAN_TMP = SSHTunnelForwarder(
    ('', 22),
    ssh_username='',
    ssh_password='',
    remote_bind_address=('', 5432),
    local_bind_address=('', 5454)
)
serverAN_TMP.start()
print ('Done ssh')

#STAGE_DC2
'''
serverAN_STAGE_DC2 = SSHTunnelForwarder(('', 22),
         ssh_username='',
         ssh_password='',
         remote_bind_address=('', 5432),
         local_bind_address=('', 5432))
serverAN_STAGE_DC2.start()
'''


    connAN_TMP = psycopg2.connect(
        database='',
        user='',
        host='',
        port=5432,
        password='')

import psycopg2
#conn AN_TMP
print ('Start postgres connect')
connAN_TMP = psycopg2.connect(
    database='',
    user='',
    host=serverAN_TMP.local_bind_host,
    port=serverAN_TMP.local_bind_port,
    password='')
print ('Done postgres connect')
#conn AN_DC2
'''
connAN_STAGE_DC2 = psycopg2.connect(
    database='promscale_devmon',
    user='',
    host=serverAN_STAGE_DC2.local_bind_host,
    port=serverAN_STAGE_DC2.local_bind_port,
    password='')
'''


#with open("Inserts_for_history.txt", 'w') as f: 
#with connAN_TMP.cursor() as cur:
rows_count=0
with connAN_TMP.cursor() as cur:
    res_queries = ''
    rows_count=''
    rows_per_file=100000
    cur.execute('select count(*) from prom_data."fanState"')
    rows_count=cur.fetchone()[0]

iterations_count=math.ceil(rows_count/rows_per_file)

print ('Total rows: {}'.format(rows_count))
print ('For rows limit set to: {}, there will be {} files.'.format(rows_per_file,iterations_count))

connAN_TMP_PROM = psycopg2.connect(
database='',
user='',
host='',
port=5432,
password='')

series_cache={}
def makeValue(row):
    if row[2] in series_cache: #Если такой лейбл уже был
        seriesId = series_cache[row[2]] #row[2] - ключ, jsonb. Значение - серия (series_id)
    else: #Если новый лейбл
        connAN_TMP_PROM = psycopg2.connect(
        database='',
        user='',
        host='',
        port=5432,
        password='')
        print ('Opening cursor for production create_id')
        with connAN_TMP_PROM.cursor() as selectidCursor:
            selectidCursor.execute('select * from _prom_catalog.get_or_create_series_id(\'%s\')' % (row[2]))
            seriesId=selectidCursor.fetchone()[0]
            series_cache[row[2]]=seriesId
            print ('Closing cursor for production create_id')
            selectidCursor.close()
            connAN_TMP_PROM.close()
    return "(TIMESTAMPTZ \'%s\', \'%s\', \'%s\')" % (row[0], row[1], seriesId)


def divide_chunks(l, n):
    row_list=list(l)
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

        
        '''
        for j in range(len(l[i:i + n])):
            print (l[j][0])
            row_list[j][0]='TIMESTAMPTZ {}'.format(l[j][0])
            print (row_list[j][0])
            #hypertables.append('TIMESTAMPTZ {}'.format(l[j][0]))
            print (hypertables)
            print(l[j][2])
            time.sleep(3) '''


for metric in metrics:
    try:
        connAN_TMP = psycopg2.connect(
            database='',
            user=''
            host='',
            port=5432,
            password='')
        print("Working with metric %s\n" % (metric))
        #print("Geting metric id: %s\n" % (metric))
        #metric_id=0
        #with connAN_TMP.cursor() as cur:
            #metric_id=''
            #cur.execute('select id from _prom_catalog.metric where metric_name = \'%s\'' % (metric))
            #metric_id=cur.fetchone()[0]
            #print("Metric id is: %s\n" % (metric_id))
            #print("Creating temp table for metric - temp_table_%s" % (metric))
            #cur.execute("CREATE TABLE history.temp_table_%s (series_id int8 PRIMARY KEY, json_data jsonb NOT NULL);" % (metric))
            #print("Inserting series data into table")
            #cur.execute("INSERT INTO history.temp_table_%s (series_id,json_data) select id, jsonb(labels) from _prom_catalog.series where metric_id = '%s';" % (metric,metric_id))
            #connAN_TMP.commit()
            #cur.close()


        print("Start copying insert commands to file Inserts_for_history_%s" % (metric))
        start_global = time.time()
        with connAN_TMP.cursor() as selectCursor:
            selectCursor.itersize = 150000
            start = time.time()
            selectCursor.execute('DECLARE super_cursor BINARY CURSOR FOR select pd.time::text,pd.value::text,tsl.json_data::text from prom_data."%s" pd inner join history.temp_table_%s tsl on tsl.series_id=pd.series_id' % (metric,metric))
            end = time.time()
            print("Total select time: ",(end-start),"sec")

            while True:
                #start = time.time()
                selectCursor.execute("FETCH 150000 FROM super_cursor")
                rows = selectCursor.fetchall()
                #end = time.time()
                if not rows:
                    break
                #print("Total fetch time: ",(end-start),"sec")
                #start_insert_time = time.time()
                with open("Inserts_for_history_DN1_cache_%s.txt" % (metric), 'a') as f:
                    for chunk in list(divide_chunks(rows, 150000)):
                        print ('Opening connect for PROD')
                        connAN_TMP_PROM = psycopg2.connect(
                            database='',
                            user='',
                            host='',
                            port=5432,
                            password='')
                        
                        res_query='INSERT INTO prom_data."%s" values ' % (metric)
                        insValues = map(makeValue, chunk)
                        f.write(res_query)
                        f.write(", ".join(insValues))
                        f.write(";\n")
                        print ('Closing connect for PROD')
                        connAN_TMP_PROM.close()
                    #end_insert_time = time.time()
                    #print("Total insert time: ",(end_insert_time-start_insert_time),"sec")
            selectCursor.close()

        end_global = time.time()
        connAN_TMP.close()
        print("Total global time: ",(end_global-start_global),"sec")
        print("Finished writing to Insert file for metric %s" % (metric))
        print("Drop temp_table history.temp_table_%s" % (metric))

        subprocess.call("./drop_table.sh history.temp_table_%s" % metric, shell=True)
        print("<========================================================================>\n")

        connAN_TMP.close()
    except Exception as e:
        print ("Problem with metric %s\n" % (metric))
        print(e)
        pass

'''
    print("Start insert uploading for file Inserts_for_history_%s.txt" % (metric))
    subprocess.call("./run_inserts.sh Inserts_for_history_%s.txt" % metric, shell=True)
    print("Finished copying metric %s" % metric)
    print("<========================================================================>\n")
'''


#Whole select
'''
with connAN_TMP.cursor() as cur:
    print ('Start writing to file')
    cur.execute('select time,value, jsonb(labels(series_id)) from prom_data."up"')
    with open("Inserts_for_history.txt", 'w') as f:
        for row in cur:
            res_query='INSERT INTO prom_data."trflog_endpoint_recv_fatal_error_total" values (TIMESTAMPTZ \'{}\', {}, _prom_catalog.get_or_create_series_id(\'{}\'));'.format(row[0],row[1],row[2])
            f.write('%s\n' % (res_query))

end = time.time()
print("Total time: ",(end-start),"sec")
'''




#Whole select
'''
with connAN_TMP.cursor() as cur:
#with connAN_TMP.cursor() as cur:
    cur.itersize = 100 # chunk size
    print ('Start writing to file')
    #cur.execute('select time,value,jsonb(labels(series_id)) from prom_data."up" limit 10')
    start = time.time()
    cur.execute('select pd.time,pd.value,tsl.json_data from prom_data."fanState" pd inner join temp_series_labels tsl on tsl.series_id=pd.series_id')
    end = time.time()
    print("Total time: ",(end-start),"sec")
    with open("Inserts_for_history.txt", 'w') as f:
        for row in cur:
            res_query='INSERT INTO prom_data."up" values (TIMESTAMPTZ \'{}\', {}, _prom_catalog.get_or_create_series_id(\'{}\'));'.format(row[0],row[1],row[2])
            f.write('%s\n' % (res_query))
            '''
