# SCC411 University Project
Python code for Data Pre-Processing and Analytics

## Steps to reproduce cleaned, merged, datasets
+ Create folders 'Google-Data-411' and 'Pre-processing'
+ Place all cluster trace datasets in 'Google-Data-411'
+ Place 'pre-processing.ipynb' in 'Pre-processing' folder
+ Run all code blocks within 'pre-processing.ipynb'
+ Cleaned, pre-processed, data can be found within the .csv files in the 'Pre-processing' folder
+ Also adds a processID column to datasets where taskIndex and jobID is present (represents unique software being run)

## Cleaner.py
+ Pre-processes file (job-event, task-event, task-usage, machine-event) appropriately
+ Example usage 'python3 Cleaner.py --jobevents jobfile.csv' or 'python Cleaner.py --taskevents taskfile.csv'
+ Outputs file 'out-jobfile.csv' (or 'out-taskfile.csv') to same directory
+ File must be in same directory as Cleaner.py file

## AutomateHive.py
+ Run file with 'python3 AutomateHive.py' to get usage guidance
+ Must be run in same directory as Hive binary
+ Creates Hive shell command for creating table and ingesting data
+ Must have input csv file with first row as column names
+ Must use full system path to input data file e.g. '/home/user/file.txt' NOT '../file.txt'
+ Example of what is run in terminal: 'hive -e "use DB1; select * from TABLE1; load data local inpath '/home/user/myfile.txt' into table TABLE1;"'

## HiveQuery.py
+ Run file with 'python3 HiveQuery.py --query 'select * from table1' --dbname 'mydatabase' --output 'results' --server 'user@192.168.1.1'
+ Uses ssh to carry out hive query and pushes results to a file, then copies over scp to host machine

# To Do
+ Output datasets without index column and without column names
