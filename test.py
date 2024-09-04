import os

files = ['dbt.log.1', 'dbt.log']

for f in files:
    # print(os.path.splitext(f))
    # print(f.endswith('.log'))
    # print(f.endswith('.1'))
    print('log' in f)
    # print(f.endswith(['.1', '.2', '.3', '.4']))



