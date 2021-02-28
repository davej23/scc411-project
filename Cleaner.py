import sys
import numpy as np 
import pandas as pd 
import os

#
# Pre-processes log files with e.g. (python Cleaner.py --jobevents jobfile.csv)
# File must be in same directory
#


if sys.argv[1] == '--jobevents': # IF ARG IS FOR JOBEVENTS, PROCESS JOBEVENTS FILE
    job_events = pd.read_csv(sys.argv[2], header=None)

    #
    # Remove NaN column
    #

    job_events = job_events.drop(columns=1)
    job_events.columns = ['timestamp', 'jobID', 'eventType', 'userName', 'schedClass', 'jobName', 'logicalJobName'] # set column names appropriately


    factorised_names, original_names = pd.factorize(job_events['userName'])
    factorised_job_names, original_job_names = pd.factorize(job_events['jobName'])
    factorised_logical_job_names, original_logical_job_names = pd.factorize(job_events['logicalJobName'])

    job_events['userName'] = factorised_names
    job_events['jobName'] = factorised_job_names
    job_events['logicalJobName'] = factorised_logical_job_names

    job_events.to_csv('out-{}'.format(sys.argv[2]))

elif sys.argv[1] == '--taskevents': # IF ARG IS TASKEVENTS PROCESS TASKEVENTS
    task_events = pd.read_csv(sys.argv[2], header=None)
    #
    # Remove first missing column
    #

    task_events = task_events.drop(columns=1)
    task_events.columns = ['timestamp', 'jobID', 'taskIndex', 'machineID', 'eventType', 'userName', 'schedulingClass', 'priority', 'CPU', 'RAM', 'Disk', 'machineConstraint'] # set column names appropriately


    #
    # Factorise all machine IDs (make note of NaN ID), userName, 
    #
    factorised_machine_ids, original_machine_ids = pd.factorize(task_events['machineID'])
    task_events['machineID'] = factorised_machine_ids
    factorised_usernames, original_usernames = pd.factorize(task_events['userName'])
    task_events['userName'] = factorised_usernames


    #
    # Create separate dataframe without NaN
    #

    nan_values = (task_events['machineID'] != -1).values  # boolean vector for NaNs in machine ID column
    task_events_NA = task_events
    task_events_clean = task_events.iloc[nan_values]

    task_events_clean.to_csv('out-{}'.format(sys.argv[2]))

elif sys.argv[1] == '--taskusage': # IF ARG IS TASKUSAGE PROCESS TASKUSAGE
    task_usage = pd.read_csv(sys.argv[2], header=None)
    #
    # Task usage
    #

    task_usage.columns = ['start', 'end', 'jobID', 'taskIndex', 'machineID', 'cpuMeanUsage', 'canonicalMemUsage', 'assignedMemUsage', 'unmappedCacheMemUsage', 'totalCacheMemUsage', 'maxMemUsage', 'meanDiskTime', 'meanDiskSpaceUsed', 'cpuMaxUsage', 'maxDiskTime', 'cyclesPerInstruction', 'memAccessPerInstruction', 'samplePortion', 'aggType', 'cpuSampledUsage']

    #
    # Remove NaN values
    #

    task_usage = task_usage.dropna()

    task_usage.to_csv('out-{}'.format(sys.argv[2]))

elif sys.argv[1] == '--machineevents': # IF ARG IS MACHINEEVENTS PROCESS FOR MACHINE EVENTS
    machine_events = pd.read_csv(sys.argv[2], header=None)
    #
    # Machine events
    #

    machine_events.columns = ['timestamp', 'machineID', 'eventType', 'platformID', 'capacityCPU', 'capacityMem']

    # Factorise platformID
    machine_events['platformID'] = pd.factorize(machine_events['platformID'])[0]

    machine_events.to_csv('out-{}'.format(sys.argv[2]))


else:
    print('Arguments (e.g. python Cleaner.py --jobevents jobfile.csv)\n', '--jobevents -> For job event files\n', '--taskevents -> For task event files\n', '--taskusage -> For task usage files\n', '--machineevents -> For machine event files\n', 'Filename after argument | Only supports one at a time')