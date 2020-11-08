"""
Put this script in the directory containing your results as CSV files and the
informations about the test files and it will plot your results so they can be
put in the final report.
"""

import pandas as pd
import matplotlib.pyplot as plt

# adapt with the names of your results files
data_testGroup = pd.read_csv('nestedLoopsMemory100000_testGroup_mean100.csv',sep = ';')
data_testSize = pd.read_csv("nestedLoopsMemory100000_testSize_mean100.csv", sep = ';')

# these files need to correspond to the parameters you have chosen for the test files generation
info_size = pd.read_csv('info_size.csv', sep= ';', header=None)
info_group = pd.read_csv('info_group.csv', sep= ';')

# We first remove the header
data_testGroup = data_testGroup[1:]

file_size = info_size[1].values

# The number of records is fixed and we change the number of groups
group_size = info_group[' groupsize / filesize'].values

# plot for the variation in the number of records
fig = plt.figure(1, figsize=(15, 10))
plt.plot(file_size, data_testSize[' Single'].values, '-', marker='o', label='Single threaded')
plt.plot(file_size, data_testSize[' Multi'].values, '-',marker='o', label="Multi threaded")
plt.plot(file_size, data_testSize[' Spark'], '-', marker='o', label="Spark")
plt.legend()
plt.title("Runtime of the Nested Loops algorithms as a function of the number of records")
plt.xlabel('Number of records')
plt.ylabel('Runtime in ms')
plt.show()

# plot for the variation in the number of groups
fig = plt.figure(1, figsize=(15, 10))
plt.plot(group_size, data_testGroup[' Single'].values, '-', marker='o', label='Single threaded')
plt.plot(group_size, data_testGroup[' Multi'].values, '-',marker='o', label="Multi threaded")
plt.plot(group_size, data_testGroup[' Spark'], '-', marker='o', label="Spark")
plt.legend()
plt.title("Runtime of the Nested Loops algorithms as a function of the number "
          + "of groups for a fixed number of records (400 000)")
plt.xlabel('Number of groups / Number of records')
plt.ylabel('Runtime in ms')
plt.show()

