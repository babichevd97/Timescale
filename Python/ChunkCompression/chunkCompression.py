from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import psycopg2
import os
import connections

load_dotenv()

##Get connections from connections.py
connAN_STAGE_DC1=connections.connAN_STAGE_DC1
connDN1_STAGE_DC1=connections.connDN1_STAGE_DC1

class TSDB():
    def __init__(self, server):
        self.server = server
    
    def GetHtWithDisabledCompression(self):
        hypertables = []
        with self.server.cursor() as cur:
            cur.execute("select table_name, schema_name, compression_state from _timescaledb_catalog.hypertable where schema_name='prom_data' limit 10;")
            for row in cur:
                if row[2]:
                    hypertables.append(row[0])
            cur.close()
        return(hypertables)
    
    def GetCompressionRulesForHt(self,hypertables):
        ht_rules_dic = {}
        for hypertable in hypertables:
            with self.server.cursor() as cur:
                cur.execute("SELECT * FROM timescaledb_information.compression_settings where hypertable_name = '%s' and orderby_column_index is not null order by orderby_column_index" % (hypertable))
                resDic={'orderby_attr':[],'segmentby_attr':[]}
                #cursor for orderby select
                for row in cur:
                    if row[5]: #Check if orderby_asc field is true
                        resDic['orderby_attr']+=[row[2]]
                    elif row[6]: #Check if orderby_nullsfirst field is true
                        resDic['orderby_attr']+=[row[2]+' DESC']
                cur.execute("SELECT * FROM timescaledb_information.compression_settings where hypertable_name = '%s' and segmentby_column_index is not null order by segmentby_column_index" % (hypertable))
                #cursor for segmentby select
                for row in cur:
                    resDic['segmentby_attr']+=[row[2]]
            cur.close()
            ht_rules_dic[hypertable]=resDic
        return(ht_rules_dic)
    
    def FormCompressionRulesQueries(self,hypertables_rules):
        with open("AlterTableQueries.txt", 'w') as f: 
            res_query = ''
            for hypertable in hypertables_rules:
                res_query='ALTER TABLE prom_data."{}" SET (timescaledb.compress, '.format(hypertable)
                print (res_query)
                if hypertables_rules[hypertable]['segmentby_attr']:
                    print ('segmentby_attr exists')
                    for ordertby in hypertables_rules[hypertable]['orderby_attr']:
                        if ordertby: 
                            print (ordertby)
                print ('\n')





#Start ssh_tunnel  if needed
#AN server
serverAN = SSHTunnelForwarder(
    (os.getenv('SSH_HOST_FORWARD'), int(os.getenv('SSH_AN_PORT'))),
    ssh_username=os.getenv('SSH_USERNAME'),
    ssh_password=os.getenv('SSH_PASSWORD'),
    remote_bind_address=(os.getenv('SSH_AN_HOST_REMOTE_BIND'), int(os.getenv('SSH_AN_PORT_REMOTE_BIND'))),
    local_bind_address=(os.getenv('SSH_AN_HOST_LOCAL_BIND'), int(os.getenv('SSH_AN_PORT_LOCAL_BIND'))))
serverAN.start()

#DN server
serverDN = SSHTunnelForwarder((os.getenv('SSH_HOST_FORWARD'), int(os.getenv('SSH_DN_PORT'))),
         ssh_username=os.getenv('SSH_USERNAME'),
         ssh_password=os.getenv('SSH_PASSWORD'),
         remote_bind_address=(os.getenv('SSH_DN_HOST_REMOTE_BIND'), int(os.getenv('SSH_DN_PORT_REMOTE_BIND'))),
         local_bind_address=(os.getenv('SSH_DN_HOST_LOCAL_BIND'), int(os.getenv('SSH_DN_PORT_LOCAL_BIND'))))
serverDN.start()

#Open connection to Database
#AN server
connAN = psycopg2.connect(
    database=os.getenv('DB_AN_DATABASE'),
    user=os.getenv('DB_AN_USERNAME'),
    host=serverAN.local_bind_host,
    port=serverAN.local_bind_port,
    password=os.getenv('DB_AN_PASSWORD'))

#DN server
connDN = psycopg2.connect(
    database=os.getenv('DB_DN_DATABASE'),
    user=os.getenv('DB_DN_USERNAME'),
    host=serverDN.local_bind_host,
    port=serverDN.local_bind_port,
    password=os.getenv('DB_DN_PASSWORD'))

AnServer=TSDB(connAN)
DnServer=TSDB(connDN)

hypertablesAN=AnServer.GetHtWithDisabledCompression()
print('List HT AN:')
print(hypertablesAN)

hypertablesDN=DnServer.GetHtWithDisabledCompression()
print('List HT DN:')
print(hypertablesDN)

hypertables_rules=AnServer.GetCompressionRulesForHt(hypertablesAN)
print('List AN rules:')
print(hypertables_rules)

hypertable_queries=AnServer.FormCompressionRulesQueries(hypertables_rules)


