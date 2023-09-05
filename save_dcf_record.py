# -*- coding: utf-8 -*-
'''
LOad a DCF Calculation Log and save the parsed results to a csv file.

Created on Wed Jul 10 14:23:49 2019
@author: gsalomon
'''
from pathlib import Path
from datetime import date
from datetime import datetime
import pandas as pd
from load_dcf_records import load_dcf_history, load_dcf_status, concurrent_jobs


work_path = Path(r'\\krcc-3\home\PHYSICS\Treatment Planning System')
dcf_path = r'DCF Monitoring and Configuration'
DCF_history_file_name='dcf_analysis.csv'
DCF_history_file = work_path / dcf_path / DCF_history_file_name

status_file_name='dcf_status.csv'
status_file = work_path / dcf_path / status_file_name

jobs_file_name='dcf_jobs.csv'
jobs_file = work_path / dcf_path / jobs_file_name


status_table = load_dcf_status()
status_table['StatusTime'] = datetime.now()
status_table.to_csv(status_file, mode='a',header=False, index=False)

# time_span options are: All, Month, Week, Day, Hour, Now
calc_history = load_dcf_history(time_span='All', table_name='History')
DCF_history_file = work_path / dcf_path / DCF_history_file_name
calc_history.to_csv(DCF_history_file, mode='a',header=False, index=False)
# Remove duplicates from the cumulative record
calc_history = pd.read_csv(DCF_history_file)
calc_history.drop_duplicates(inplace=True)
calc_history.to_csv(DCF_history_file, mode='w',header=True, index=False)

job_count = concurrent_jobs(calc_history)
job_count.to_csv(jobs_file, mode='a',header=False, index=False)
# Remove duplicates from the cumulative record
job_count = pd.read_csv(jobs_file)
job_count.drop_duplicates(inplace=True)
job_count.to_csv(jobs_file, mode='w',header=True, index=False)


#sheet_name='DCF_Data_' + str(date.today())
#file_name='dcf_analysis.xlsx'
#file = work_path / dcf_path / file_name
#calc_history = Load_dcf_record(time_span='Week', table_name='History')
#with pd.ExcelWriter(str(file), engine='openpyxl', mode='a') as writer:
#    calc_history.to_excel(writer, sheet_name=sheet_name)
#calc_history = Load_dcf_record(time_span='Day', table_name='History')
# TODO Allow for adding to existing csv file
