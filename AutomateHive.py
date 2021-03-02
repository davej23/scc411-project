import os
import numpy as np
import pandas as pd
import sys

#
# davej23 28/02/21
#

#
# Outputs how to use program
#
def usage():
    print('Usage (e.g. python AutomateHive.py --input text.csv --dbname firstdb --tablename hellotable)\n', '--input -> Flag to input csv to new table\n', '--dbname -> Name of database table goes in\n', '--tablename -> Specify name of table\n')
 
#
# Generates column names for table
#
def colNameExtractor(file):
    file = pd.read_csv(file, index_col=0) # read csv with headers, remove index column
    column_names = file.columns # extract column names
    column_types = [type(file.to_numpy()[1][i]) for i in range(len(column_names))] # find data type for first entry in each column

    for i in range(len(column_types)): # for each column type, change to string, int or double where required
        if column_types[i] == str:
            column_types[i] = 'string'
        elif column_types[i] in [int, np.int, np.int32, np.int64]:
            column_types[i] = 'int'
        elif column_types[i] in [float, np.float, np.float32, np.float64]:
            column_types[i] = 'double'

    return column_names, column_types

#
# Generates command for hive
#
def generator(datafile, table_name, db_name):
    colnames, coltypes = colNameExtractor(datafile) # find column names and data types

    create_table = 'CREATE TABLE IF NOT EXISTS {} '.format(table_name) # beginning string for creating hive table
    features = [] # hold entries for table
    for i in range(len(colnames)): # append to features the column names and types with commas and brackets when needed
        if i == 0:
            features.append('({} {}, '.format(colnames[i], coltypes[i]))
        if 0 < i < len(colnames)-1:
            features.append('{} {}, '.format(colnames[i], coltypes[i]))
        elif i == len(colnames)-1:
            features.append('{} {}) '.format(colnames[i], coltypes[i]))


    table_style = ''.join(features) # join to a string (e.g. (id INT, age INT, gender String))
    other_params = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY {};".format(repr('\n')) # rest of command string

    final_command = create_table + table_style + other_params # compose command

    return final_command

#
# Function initial call, checks sys args
#

if len(sys.argv) == 1 or len(sys.argv) > 7 or len(sys.argv) == 6: # if not correct number of arguments, print usage
    usage()

else: # if correct format, set appropriate names
    if sys.argv[1] == '--input' and sys.argv[3] == '--dbname' and sys.argv[5] == '--tablename':
        table_name = sys.argv[6]
        db_name = sys.argv[4]
        datafile = sys.argv[2]

    else: # if anything incorrect, print usage
        usage()

#
# Start composing commands as strings
#

hive_command = 'hive -e'
enter_database = 'USE {}'.format(db_name)

#
# Debugging -- if needed
#

#generator([['index','filename','age'],[1,2.2,3]], 'hello', 'hello')
#colNameExtractor([['index','filename','age'],[1,2.2,3]])
#a = [['index','filename','age'],[1,2.2,3]]

#
# Create final Hive command
#

final_command = hive_command + ' ' + '"{}; {} '.format(enter_database,generator(datafile, table_name, db_name)) + 'LOAD DATA LOCAL INPATH "{}/{}" INTO TABLE {};'.format(os.getcwd(),datafile,table_name) + '"'

#
# Print final statement if needed
#
# print(final_command)

#
# Execute command in shell
#
os.system(final_command)

