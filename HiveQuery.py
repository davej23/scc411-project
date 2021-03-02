import os
import sys

# davej23 02/03/21

#
# Runs Hive Query, outputs results to file 
# and copies via ssh to another computer
#

def usage():
    print("Usage -- e.g. python HiveQuery --query 'select * from table1' --dbname 'DBNAME' --output 'output-file.txt' --scp 'USER@IPADDRESS'", '\n')

if len(sys.argv) == 1 or len(sys.argv) < 9:
    usage()

#
# Extract arguments
#
hive_query = sys.argv[2]
db_name = sys.argv[4]
file_name = sys.argv[6]
scp = sys.argv[8]

#
# Function to output correct command and output to a file
#
def commandGenerator(query, db, filename):
    return "insert overwrite local directory '{}.txt' use {}; {};".format(filename, db, query)

#
# Creates hive command line query
#
def hiveQuery(command_input):
    return 'hive -e "{}"'.format(command_input)

#
# Compose final query (hive query -> file -> scp)
#
final_command = hiveQuery(commandGenerator(hive_query, db_name, file_name)) + ' && ' + 'scp {}.txt {}:/home/{}/'.format(file_name, scp, scp.split('@')[0])

#
# Execute command (requires password of user being scp'd)
#
os.system(final_command)
