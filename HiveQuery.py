import os
import sys
import getpass

# davej23 11/03/21

#
# Script to output results of a Hive query in Hadoop cluster to a file
# This file then gets pushed back to Host machine via scp 
#

#
# Current machine user and hostname (NOT NEEDED)
#
host_name = '{}@{}'.format(getpass.getuser(), os.uname()[1])

def usage():
    print("Usage -- e.g. python3 HiveQuery.py --query 'select * from table1' --dbname 'DBNAME' --output 'output-file.txt' --server 'USER@IPADDRESS'", '\n')

if len(sys.argv) == 1 or len(sys.argv) < 9:
    usage()

#
# Extract arguments
#
hive_query = sys.argv[2]
db_name = sys.argv[4]
file_name = sys.argv[6]
server_name = sys.argv[8]

#
# Function to output correct command and output to a file
#
def commandGenerator(query, db, filename):
    return "use {}; {};".format(db, query)

#
# Creates hive command line query
#
def hiveQuery(command_input):
    return 'hive -e "{}" >> ../{}'.format(command_input, file_name)

#
# Compose final queries (hive query -> file -> scp)
#
final_command = hiveQuery(commandGenerator(hive_query, db_name, file_name))
return_to_host = 'scp {}:/home/{}/{} .'.format(server_name, server_name.split('@')[0], file_name)

#
# Execute command on server
#
ssh_command = "ssh {} 'cd apache-hive-2.3.8-bin; {}'".format(server_name, final_command)
os.system(ssh_command)

#
# Return file to host
#
os.system(return_to_host)

#
# Debugging (prints commands to screen)
#
#print(ssh_command)
#print(return_to_host)
