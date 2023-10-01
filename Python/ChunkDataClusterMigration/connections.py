from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import psycopg2
import os

#Loading envs
load_dotenv()

#------------ CONNECTION VARIABLES ------------#
##SSH - tunnels example, if needed
#STAGE_DC1 AN SSH tunnel 
serverAN_SSH = SSHTunnelForwarder(
    (os.getenv('SSH_HOST_FORWARD'), int(os.getenv('SSH_AN_PORT'))),
    ssh_username=os.getenv('SSH_USERNAME'),
    ssh_password=os.getenv('SSH_PASSWORD'),
    remote_bind_address=(os.getenv('SSH_AN_HOST_REMOTE_BIND'), int(os.getenv('SSH_AN_PORT_REMOTE_BIND'))),
    local_bind_address=(os.getenv('SSH_AN_HOST_LOCAL_BIND'), int(os.getenv('SSH_AN_PORT_LOCAL_BIND'))))
serverAN_SSH.start()

#STAGE_DC1 DN SSH tunnel
serverDN1_SSH = SSHTunnelForwarder(
    (os.getenv('SSH_HOST_FORWARD'), int(os.getenv('SSH_DN_PORT'))),
    ssh_username=os.getenv('SSH_USERNAME'),
    ssh_password=os.getenv('SSH_PASSWORD'),
    remote_bind_address=(os.getenv('SSH_DN_HOST_REMOTE_BIND'), int(os.getenv('SSH_DN_PORT_REMOTE_BIND'))),
    local_bind_address=(os.getenv('SSH_DN_HOST_LOCAL_BIND'), int(os.getenv('SSH_DN_PORT_LOCAL_BIND'))))
serverDN1_SSH.start()

##DB - connections via ssh - tunnel example
#STAGE_DC1 Connect
#AccessNode connection via SSH
connAN_STAGE_DC1 = psycopg2.connect(
    database=os.getenv('DB_AN_DATABASE'),
    user=os.getenv('DB_AN_USERNAME'),
    host=serverAN_SSH.local_bind_host, #Our created ssh - tunnel socket. Runs on the machine, where script is running
    port=serverAN_SSH.local_bind_port, #Our created ssh - tunnel socket. Runs on the machine, where script is running
    password=os.getenv('DB_AN_PASSWORD')
)

#DataNode1 connection via SSH
connDN1_STAGE_DC1 = psycopg2.connect(
    database=os.getenv('DB_DN_DATABASE'),
    user=os.getenv('DB_DN_USERNAME'),
    host=serverDN1_SSH.local_bind_host,  #Our created ssh - tunnel socket. Runs on the machine, where script is running
    port=serverDN1_SSH.local_bind_port,  #Our created ssh - tunnel socket. Runs on the machine, where script is running
    password=os.getenv('DB_DN_PASSWORD')
)


##Straight DB - connections example
#STAGE_DC1 Connect

'''
#AccessNode straight connection
connAN_STAGE_DC1 = psycopg2.connect(
    database=os.getenv('DB_AN_DATABASE'),
    user=os.getenv('DB_AN_USERNAME'),
    host=os.getenv('DB_AN_HOST'), #Our created ssh - tunnel socket. Runs on the machine, where script is running
    port=os.getenv('DB_AN_PORT'), #Our created ssh - tunnel socket. Runs on the machine, where script is running
    password=os.getenv('DB_AN_PASSWORD')
)

#DataNode1 straight connection
connDN1_STAGE_DC1 = psycopg2.connect(
    database=os.getenv('DB_DN_DATABASE'),
    user=os.getenv('DB_DN_USERNAME'),
    host=os.getenv('DB_DN_HOST'),  #Our created ssh - tunnel socket. Runs on the machine, where script is running
    port=os.getenv('DB_DN_PORT'),  #Our created ssh - tunnel socket. Runs on the machine, where script is running
    password=os.getenv('DB_DN_PASSWORD')
)
'''

##Victoria-metrics
VM_HOST=os.getenv('VM_HOST')
VM_PORT=os.getenv('VM_PORT')
VM_INSERT_QUERY=os.getenv('VM_INSERT_QUERY')