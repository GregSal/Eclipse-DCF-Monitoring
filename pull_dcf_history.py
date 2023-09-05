'''Query the DCF service for DCF History records.
'''
# %% Imports
from pathlib import Path
import pandas as pd
from load_dcf_records import load_dcf_history


# %% File Paths
work_path = Path(r'\\krcc-3\home\PHYSICS\Treatment Planning System')
dcf_path = r'DCF Monitoring and Configuration'
DCF_history_file_name='dcf_analysis.csv'
DCF_history_file = work_path / dcf_path / DCF_history_file_name


# %% Add History records to __.csv__ file
# time_span options are: All, Month, Week, Day, Hour, Now
calc_history = load_dcf_history(time_span='Day', table_name='History')
calc_history.to_csv(DCF_history_file, mode='a',header=False, index=False)

# Remove duplicates from the cumulative record
calc_history = pd.read_csv(DCF_history_file)
calc_history.drop_duplicates(inplace=True)
calc_history.to_csv(DCF_history_file, mode='w',header=True, index=False)
