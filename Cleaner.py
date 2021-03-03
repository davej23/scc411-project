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
    job_events.columns = ['time', 'jobID', 'eventType', 'userName', 'schedClass', 'jobName', 'logicalJobName'] # set column names appropriately

    factorised_names, original_names = pd.factorize(job_events['userName'])
    factorised_job_names, original_job_names = pd.factorize(job_events['jobName'])
    factorised_logical_job_names, original_logical_job_names = pd.factorize(job_events['logicalJobName'])
    factorised_job_ids, original_job_ids = pd.factorize(job_events['jobID'])

    job_events['userName'] = factorised_names
    job_events['jobName'] = factorised_job_names
    job_events['logicalJobName'] = factorised_logical_job_names
    job_events['jobID'] = factorised_job_ids
    job_events['time'] = pd.to_datetime(job_events['time'], unit='us', origin='2011-05-01')

    job_events.to_csv('out-{}'.format(sys.argv[2]))

elif sys.argv[1] == '--taskevents': # IF ARG IS TASKEVENTS PROCESS TASKEVENTS
    task_events = pd.read_csv(sys.argv[2], header=None)
    #
    # Remove first missing column
    #

    task_events = task_events.drop(columns=1)
    task_events.columns = ['time', 'jobID', 'taskIndex', 'machineID', 'eventType', 'userName', 'schedulingClass', 'priority', 'CPU', 'RAM', 'Disk', 'machineConstraint'] # set column names appropriately


    #
    # Factorise all machine IDs (make note of NaN ID), userName, 
    #
    factorised_machine_ids, original_machine_ids = pd.factorize(task_events['machineID'])
    task_events['machineID'] = factorised_machine_ids
    factorised_usernames, original_usernames = pd.factorize(task_events['userName'])
    task_events['userName'] = factorised_usernames
    factorised_jobid_task, original_jobid_task = pd.factorize(task_events['jobID'])
    task_events['jobID'] = factorised_jobid_task


    #
    # Create separate dataframe without NaN
    #

    nan_values = (task_events['machineID'] != -1).values  # boolean vector for NaNs in machine ID column
    task_events_NA = task_events
    task_events_clean = task_events.iloc[nan_values].reset_index()
    task_events_clean = task_events_clean.reset_index().drop(columns=['index', 'level_0'])

    task_events['processID'] = (task_events['jobID']+0.00001*task_events['taskIndex'])
    task_events_clean['processID'] = (task_events_clean['jobID']+0.00001*task_events_clean['taskIndex'])

    task_combos_full, task_combo_unique_full = pd.factorize(task_events['processID'])
    task_combos, task_combo_unique = pd.factorize(task_events_clean['processID'])
    task_events_clean['processID'] = task_combos
    task_events['processID'] = task_combos_full
    task_events['time'] = pd.to_datetime(task_events['time'], unit='us', origin='2011-05-01')

    task_events.to_csv('out-{}'.format(sys.argv[2]))

elif sys.argv[1] == '--taskusage': # IF ARG IS TASKUSAGE PROCESS TASKUSAGE
    task_usage = pd.read_csv(sys.argv[2], header=None)
    #
    # Task usage
    #

    task_usage.columns = ['first', 'last', 'jobID', 'taskIndex', 'machineID', 'cpuMeanUsage', 'canonicalMemUsage', 'assignedMemUsage', 'unmappedCacheMemUsage', 'totalCacheMemUsage', 'maxMemUsage', 'meanDiskTime', 'meanDiskSpaceUsed', 'cpuMaxUsage', 'maxDiskTime', 'cyclesPerInstruction', 'memAccessPerInstruction', 'samplePortion', 'aggType', 'cpuSampledUsage']

    #
    # Remove NaN values
    #

    task_usage = task_usage.dropna()
    # # Find unique pairs of taskIndex and jobID (only in task_events_clean)
    # Quickest way to find unique pairs is to find sum of 0.01*taskIndex and jobID
    # This way takes less than 5 seconds compared to 30 minutes any other way
    # Number generated is almost certainly going to be unique to that specific process

    task_usage['processID'] = (task_usage['jobID']+0.01*task_usage['taskIndex'])
    usage_combos_df, usage_unique_combos = pd.factorize(task_usage['processID'])
    task_usage['processID'] = usage_combos_df

    #
    # Change first and last to timestamp
    #
    task_usage['first'] = pd.to_datetime(task_usage['first'], unit='us', origin='2011-05-01')
    task_usage['last'] = pd.to_datetime(task_usage['last'], unit='us', origin='2011-05-01')

    task_usage.to_csv('out-{}'.format(sys.argv[2]))

elif sys.argv[1] == '--machineevents': # IF ARG IS MACHINEEVENTS PROCESS FOR MACHINE EVENTS
    machine_events = pd.read_csv(sys.argv[2], header=None)
    #
    # Machine events
    #

    machine_events.columns = ['time', 'machineID', 'eventType', 'platformID', 'capacityCPU', 'capacityMem']

    # Factorise platformID
    machine_events['platformID'] = pd.factorize(machine_events['platformID'])[0]
    # Change time to timestamp
    machine_events['time'] = pd.to_datetime(machine_events['time'], unit='us', origin='2011-05-01')

    machine_events.to_csv('out-{}'.format(sys.argv[2]))


else:
    print('Arguments (e.g. python Cleaner.py --jobevents jobfile.csv)\n', '--jobevents -> For job event files\n', '--taskevents -> For task event files\n', '--taskusage -> For task usage files\n', '--machineevents -> For machine event files\n', 'Filename after argument | Only supports one at a time')