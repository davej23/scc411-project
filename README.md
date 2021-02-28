# SCC411 University Project
Python code for Data Pre-Processing and Analytics

## Steps to reproduce cleaned, merged, datasets
+ Create folders 'Google-Data-411' and 'Pre-processing'
+ Place all cluster trace datasets in 'Google-Data-411'
+ Place 'pre-processing.ipynb' in 'Pre-processing' folder
+ Run all code blocks within 'pre-processing.ipynb'
+ Cleaned, pre-processed, data can be found within the .csv files in the 'Pre-processing' folder

## Cleaner.py
+ This file can be run like 'python Cleaner.py --jobevents jobfile.csv'
+ Outputs file 'out-jobfile.csv' to same directory
+ File must be in same directory as Cleaner.py file

## AutomateHive.py
+ Run file with 'python AutomateHive.py' to get usage guidance
+ Must be run in same directory as file going into Hive
+ Creates Hive shell command for creating table and ingesting data
+ Must have input csv file with first row as column names
