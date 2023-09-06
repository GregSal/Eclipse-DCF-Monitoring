# -*- coding: utf-8 -*-
'''
Query DCF Calculation Logs and parse the results.

Classes:
    TextSplit(NamedTuple):
        Named Tuple with parameters for splitting single entry into multiple
        entries.

    TimeSet(NamedTuple):
        Named Tuple with parameters for converting date strings into Date and
        Time or time delta.

    TextParse(NamedTuple):
        Named Tuple with parameters for extracting one portion of a text entry.


Created on Wed Jul 10 14:23:49 2019
@author: Greg Salomons
'''
from pathlib import Path
from typing import NamedTuple, List
import pandas as pd
import requests
from bs4 import BeautifulSoup
import xlwings as xw


# %% Utility Functions
class TextSplit(NamedTuple):
    '''Named Tuple with parameters for splitting single entry into multiple
        entries.
    Attributes:
        column {str} -- The name of the DataFrame column to partition.
        partition {str} -- The character separating the label and value
            portions of the partition.
        token {str} -- The character separating each partition.
            (default: {';'})
        reverse {bool} -- Indicates that the value precedes the label.
            (default: {False})
        prefix {str} -- A string to prefix the partition label with.
            (default: {''})
        suffix {str} -- A string to append to the partition label.
            (default: {''})
    '''
    column: str
    partition: str
    token: str = ';'
    reverse: bool = False
    prefix: str = ''
    suffix: str = ''

class TimeSet(NamedTuple):
    '''Named Tuple with parameters for converting date strings into Date and
        Time or time delta.
    Attributes:
        column {str} -- The name of the DataFrame column containing the
            date/time data.
        delta {bool} -- Convert the data to a TimeDelta.
            (default: {False})
        unit {str} -- If data contains a number, the unit of that number.
            e.g. 'D' for 1 day.  (default: {None})
        zero {str} -- If data contains a number, the date of 'zero time'
            e.g. '1900-01-01'.  (default: {None})
    '''
    column: str
    delta: bool = False
    unit: str = None
    zero: str = None

class TextParse(NamedTuple):
    '''Named Tuple with parameters for extracting one portion of a text entry.
    Attributes:
        column {str} -- The name of the DataFrame column to clean.
        token {str} -- The character(s) to use for splitting of the text.
        keep_split {int} -- The portion of the text to keep after applying the
            split.
        max_splits {int} -- The maximum number of occurrences of token to use
            for splitting.  (default: {1})
        right {bool} -- Begin applying split from the right instead of the
            left.  (default: {False})
    '''
    column: str
    token: str
    keep_split: int
    max_splits: int = 1
    right: bool = False

def split_items(data: pd.DataFrame,
                split_settings: List[TextSplit])->pd.DataFrame:
    '''Split text variable data into multiple variables.
        The text is partitioned into variables based on the token.
        Each variable is assumes to contain a label and a value separated by
        partition.
    Arguments:
        data {pd.DataFrame} -- The DataFrame containing the column to be
            partitioned.
        split_settings {List[TextSplit]} -- A list containing instructions for
            each column to be split. Settings are:
                column {str} -- The name of the DataFrame column to partition.
                partition {str} -- The character separating the label and
                    value portions of the partition.
                token {str} -- The character separating each partition.
                    (default: {';'})
                reverse {bool} -- Indicates that the value precedes the label.
                    (default: {False})
                prefix {str} -- A string to prefix the partition label with.
                    (default: {''})
                suffix {str} -- A string to append to the partition label.
                    (default: {''})
    Returns:
        pd.DataFrame -- The supplied DataFrame with the additional columns
            obtained from the splitting process.
    '''
    for (name, partition, token, reverse, prefix, suffix) in split_settings:
        column_names = ['name', 'value']
        if reverse:
            column_names.reverse()
        item_set = data[name].str.split(token, expand=True)
        # Converts the partitions found into a single column
        # This way, if different elements in the original column contain
        # different groups or are in a different order, the split values will
        # be assigned to the correct new variable.
        items_listing = item_set.stack().str.strip()
        # Split the partitions into label and value.
        items = items_listing.str.split(partition, n=1, expand=True)
        # Assign 'name' and 'value' to the resulting split columns.
        items.columns = column_names
        # Modify the Labels with the prefix and suffix values.
        items['name'] = prefix + items['name'] + suffix
        # Convert the name and value columns into multiple variables based on
        # the Label.
        items.reset_index(level=1, drop=True, inplace=True)
        new_columns = items.pivot(columns='name', values='value')
        # Remove any blank columns
        if '' in new_columns.columns:
            new_columns.drop('', axis=1, inplace=True)
        data = data.join(new_columns)
    return data


def set_times(data: pd.DataFrame,
              time_settings: List[TimeSet])->pd.DataFrame:
    '''Convert Time variable to DateTime or TimeDelta types.
    Split text variable data into multiple variables.
        The text is partitioned into variables based on the token.
        Each variable is assumes to contain a label and a value separated by
        partition.
    Arguments:
        data {pd.DataFrame} -- The DataFrame containing the column to be
            partitioned.
    split_settings {List[TimeSet]} -- A list containing instructions for
            each column to be converted. Settings are:
        column {str} -- The name of the DataFrame column containing the
            date/time data.
        delta {bool} -- Convert the data to a TimeDelta.
            (default: {False})
        unit {str} -- If data contains a number, the unit of that number.
            e.g. 'D' for 1 day.  (default: {None})
        zero {str} -- If data contains a number, the date of 'zero time'
            e.g. '1900-01-01'.  (default: {None})
    Returns:
        pd.DataFrame -- The supplied DataFrame with the converted columns.
    '''
    def to_seconds(time_str, unit='seconds'):
        time_value = pd.Timedelta(time_str)
        return time_value.total_seconds()

    for (name, delta, unit, zero) in time_settings:
        if delta:
            data[name] = data[name].apply(to_seconds)
            #data[name] = pd.TimedeltaIndex(data[name], unit=unit)
        elif zero:
            data[name] = pd.to_datetime(data[name], unit=unit, origin=zero)
        else:
            data[name] = pd.to_datetime(data[name], unit=unit)
    return data


def trim_text(data: pd.DataFrame,
              text_settings: List[TextParse])->pd.DataFrame:
    '''Extract one portion of a text variable based on a delimiter.
    Arguments:
        data {pd.DataFrame} -- The DataFrame containing the column to be
            partitioned.
        text_settings {List[TextParse]} -- A list containing instructions for
            extracting one portion of the text in each column to be cleaned.
                Settings are:
            column {str} -- The name of the DataFrame column to clean.
            token {str} -- The character(s) to use for splitting of the text.
            keep_split {int} -- The portion of the text to keep after applying
                the split.
            max_splits {int} -- The maximum number of occurrences of token to
                use for splitting.  (default: {1})
            right {bool} -- Begin applying split from the right instead of the
                left.  (default: {False})
    Returns:
        pd.DataFrame -- The supplied DataFrame with the cleaned columns.
    '''
    for (name, tkn, keep, num, right) in text_settings:
        if right:
            parsed_text = data[name].str.rsplit(tkn, num, expand=True)
        else:
            parsed_text = data[name].str.split(tkn, num, expand=True)
        data[name] = parsed_text[keep]
    return data


# %% Data Processing Functions
def convert_excel_dates(data: pd.DataFrame):
    '''Convert excel dates read in as strings to DateTime data.
    Arguments:
        data {pd.DataFrame} -- The DataFrame containing the columns to be
            converted.
    Returns:
        pd.DataFrame -- The supplied DataFrame with the converted columns.
    '''
    time_settings = [
        TimeSet('Client connect time', unit='D', zero='1900-01-01'),
        TimeSet('Agent connect time', unit='D', zero='1900-01-01'),
        TimeSet('Service start time', unit='D', zero='1900-01-01'),
        TimeSet('Service end time', unit='D', zero='1900-01-01'),
        TimeSet('Client connect time', unit='D', zero='1900-01-01'),
        #TimeSet('Client wait time', unit='D', delta=True),
        #TimeSet('Call duration', unit='D', delta=True),
        ]
    data = set_times(data, time_settings)
    return data


def identify_jobs(calc_history):
    def time_dif(df):
        offset = df.StartTime - df.StartTime.shift(1).fillna(method='backfill')
        df["StartOffset"] = offset.dt.seconds
        df["Job"] = df.StartOffset.cumsum().astype('string')
        return df

    calc_history.sort_values(['ClientConnect', 'StartTime', 'EndTime'], inplace=True)
    calc_history = calc_history.groupby(['Client', 'Algorithm', 'Slices']).apply(time_dif)

    calc_history["StartOffset"] = calc_history.StartOffset.fillna(-1)
    calc_history["Job"] = calc_history.Job.fillna('-1')

    calc_history['Job_Id'] = calc_history.Client.str.cat(calc_history[['Algorithm', 'Slices', 'Job']], sep='_')
    calc_history['Job_Id'] = calc_history.Job_Id.astype('category')
    calc_history.drop(columns = ["StartOffset","Job"], inplace=True)

    calc_history.sort_values(['ClientConnect', 'StartTime', 'EndTime'], inplace=True)
    job_id = [n for n, j in enumerate(calc_history.Job_Id.unique())]
    calc_history['Job_Id'] = calc_history.Job_Id.cat.rename_categories(job_id)
    return calc_history


def concurrent_jobs(calc_history, id_vars=None, value_vars=None):
    if not id_vars:
        id_vars = ['Agent', 'Client', 'Algorithm', 'Job_Id', 'WaitTime', 'Duration']
    if not value_vars:
        value_vars = ['StartTime', 'EndTime']
    job_times = calc_history.melt(id_vars=id_vars, value_vars=value_vars,
                                  value_name='Time', var_name='Event')
    job_times.sort_values(['Time'], inplace=True)
    start_job = job_times.Event.str.contains('StartTime')
    end_job = job_times.Event.str.contains('EndTime')
    job_times['new_job'] = 0
    job_times.loc[start_job,'new_job']  = 1
    job_times.loc[end_job,'new_job']  = -1
    job_times['job_count'] = job_times.new_job.cumsum()
    return job_times


# %% Query & Retrieve Functions
def get_dcf_table(query='History', time='Day',
                  table_reference=2,
                  dcf_server=r'http://ariadicompv1:57580')->BeautifulSoup:
    '''Query the DCF status logs and extract the requested table.
    Keyword Arguments:
        query {str} -- The name for the desired table.
            (default: {'History'})
        time {str} -- The time span (where applicable) to use for modifying
            the table query.  (default: {'Day'})
        table_reference {int} -- Query results contain multiple tables.
            The table to select and return. (default: {2})
    Returns:
        BeautifulSoup -- The requested table.
    '''
    def query_dcf(dcf_url: str, table_reference: int)->BeautifulSoup:
        '''Query the DCF page and return the table.
        Arguments:
            dcf_url {str} -- The URL to use for the query.
            table_reference {int} -- The index to the table to return.
        Returns:
            BeautifulSoup -- The requested table.
        '''
        page = requests.get(dcf_url, timeout=30)
        soup = BeautifulSoup(page.content, 'html.parser')
        # TODO Verify the table index for each possible page
        return soup('table')[table_reference]

    dcf_query_limit =r'?AgeLimit='
    # The possible DCF pages to query.
    dcf_query_type = {'System': 'DistributorSystemInfo',
                      'Status': 'DistributorStatusSummary',
                      'Agents': 'AgentSystemInfoOverview',
                      'AgentStatus': 'AgentServiceStatusOverview',
                      'AgentsActive': 'AgentWorkStatusOverview',
                      'ClientStatus': 'ClientServiceStatusOverview',
                      'ActiveJobs': 'CallsInProgress',
                      'WaitingJobs': 'CallsWaiting',
                      'History': 'CallHistory',
                      'Workload': 'CallDistribution',
                      'HistorySummary': 'ExecutionHistory',
                      'ServiceNames': 'CurrentServices'}
    # Available time spans for tables. (Others may also be possible.)
    dcf_query_time_options = {'All': 'None',
                              'Month': '30d',
                              'Week': '7d',
                              'Day': '24h',
                              'Hour': '1h',
                              'Now': '10m'}
    # Select a page and time span to append to the query.
    # TODO exclude time span for pages that it does not apply to.
    dcf_query_name = dcf_query_type[query]
    dcf_query_time = dcf_query_time_options[time]
    dcf_url = dcf_server + '/' + dcf_query_name + '.html'
    dcf_url += dcf_query_limit + dcf_query_time
    dcf_table = query_dcf(dcf_url, table_reference)
    return dcf_table


def parse_dcf_query(dcf_table: BeautifulSoup, token=';')->pd.DataFrame:
    '''Convert the BeautifulSoup table into a Pandas DataFrame.
    Arguments:
        dcf_table {BeautifulSoup} -- THe table to convert.
    Keyword Arguments:
        token {str} -- Text to use as a marker for a line break.
            (default: {';'})
    Returns:
        pd.DataFrame -- THe table converted to a DataFrame.
    '''
    def replace_breaks(row: BeautifulSoup, token: str)->BeautifulSoup:
        '''Convert line breaks (<br/>) into the desired token.
        Arguments:
            row {BeautifulSoup} -- A row from the table.
            token {str} -- Text to use as a marker for a line break.
        Returns:
            BeautifulSoup -- The supplied table row with all instances of
                <br/> replaced with the designated token.
        '''
        for brk in row('br'):
            brk.replace_with(token)
        return row

    headers = [hd.get_text() for hd in dcf_table('th')]
    rows = dcf_table('tr')[1:-1]
    dcf_data_list = list()
    for row in rows:
        # Break tags (<br/>) are lost when .get_text() is used
        # Replace them with a text token that will be preserved.
        clean_row = replace_breaks(row, token)
        data_dict = {name:value.get_text()
                     for name, value in zip(headers, clean_row('td'))}
        dcf_data_list.append(data_dict)
    return pd.DataFrame(dcf_data_list)


def save_data(data: pd.DataFrame, sheet: str, directory: Path,
              file_name='dcf_analysis.xlsx'):
    '''Save the DCF data into an Excel spreadsheet.
    Arguments:
        data {pd.DataFrame} -- The DataFrame containing the DCF data to be
            saved.
        sheet {str} -- The name of the worksheet to save the data in.
        directory {Path} -- Path to the folder where the Excel file will be.
        file_name {str} -- The name of the Excel file to save the data in.
            (default: {'dcf_analysis.xlsx'})
        output_format {optional, Dict[str, str]} -- Dictionary with keys
            referencing the names of columns in the DataFrame and values
            containing Excel format strings for that column. Example:
                {'Service start time': 'dd-mmm-yyyy HH:mm',
                 'Service end time': 'dd-mmm-yyyy HH:mm',
                 'Call duration': 'HH:mm',
                 'Client wait time': 'HH:mm'}
    '''
    save_file = directory / file_name
    workbook = xw.Book(save_file)
    worksheet = workbook.sheets.add(sheet)
    worksheet.range('A1').value = data


def read_dcf_data(directory: Path, file_name: str,
                  sheet_name: str)->pd.DataFrame:
    '''Read DCF data from an Excel spreadsheet.
    Arguments:
        directory {Path} -- Path to the folder where the Excel file is located.
        file_name {str} -- The name of the Excel file containing the data.
        sheet_name {str} -- The name of the worksheet containing the data.
    Returns:
        DataFrame -- The requested data as a DataFrame.
    '''
    excel_file = directory / file_name
    workbook = xw.Book(excel_file)
    worksheet = workbook.sheets[sheet_name]
    table_range = worksheet.range('A1').expand()
    dcf_data = table_range.options(pd.DataFrame, header=1).value
    return dcf_data


def load_dcf_history(time_span='Week', table_name='History'):
    '''Query the DCF status logs and extract the requested table.
    Keyword Arguments:
        table_name {str} -- The name for the desired table.
            (default: {'History'})
        time_span {str} -- The time span (where applicable) to use for
            modifying the table query.  Options are:
                - All,
                - Month,
                - Week,
                - Day,
                - Hour,
                - Now
            (default: {'Week'})
    Returns:
        pd.DataFrame -- The requested data as a DataFrame.
'''
    # Name conversion for history data columns
    column_names = {
        'Agent': 'Agent',
        'Client': 'Client',
        'Service name': 'Algorithm',
        'Service_Type': 'CalcType',
        'Client wait time': 'WaitTime',
        'Active MB at start time': 'MemoryInUse',
        'Active jobs at start time': 'ActiveJobs',
        'Active procs at start time': 'ProcsInUse',
        'Service start time': 'StartTime',
        'Service end time': 'EndTime',
        'Call duration': 'Duration',
        'Status': 'Status',
        'Agent connect time': 'AgentConnect',
        'Client connect time': 'ClientConnect',
        'FieldTechnique': 'Technique',
        'NumberOfFields': 'Fields',
        'CalculationAreaXSizeInMM': 'CalcX',
        'CalculationAreaYSizeInMM': 'CalcY',
        'CalculationAreaZSizeInMM': 'CalcZ',
        'CalculationGridSize': 'GridSize',
        'FieldLengthInMM': 'FieldLengthInMM',
        'FieldXSizeInMM': 'FieldXSizeInMM',
        'FieldYSizeInMM': 'FieldYSizeInMM',
        'ImageXSizeInMM': 'ImageX',
        'ImageYSizeInMM': 'ImageY',
        'ImageYSizeInPixels': 'ImageY_P',
        'ImageXSizeInPixels': 'ImageX_P',
        'NumberOfPoints': 'Points',
        'NumberOfSlices': 'Slices',
        'SliceIntervalInMM': 'SliceSpacing',
        'Expected MB': 'ExpectedMemory',
        'Expected procs': 'ExpectedProcs',
        'Number of processors to use': 'ExpectedProcessors',
        'Ranking': 'Ranking',
        'Agent bytes transmitted': 'Agent bytes transmitted',
        'Client bytes transmitted': 'Client bytes transmitted',
        'Service request attributes': 'Service request attributes',
        'Expected resource usage': 'Expected resource usage',
        'Active work at start time': 'Active work at start time'
        }
    # Partition settings for DCF History
    split_settings = [
        TextSplit('Active work at start time', partition=' ', reverse=True,
                  prefix='Active ', suffix= ' at start time'),
        TextSplit('Expected resource usage', partition=' ', reverse=True,
                  prefix='Expected '),
        TextSplit('Service request attributes', '=')
        ]
    # Time conversion settings for DCF History
    time_settings = [
        TimeSet('Client connect time'),
        TimeSet('Agent connect time'),
        TimeSet('Service start time'),
        TimeSet('Service end time'),
        TimeSet('Client wait time', unit='seconds', delta=True),
        TimeSet('Call duration', unit='seconds', delta=True)
        ]
    # Text cleaning settings for DCF History
    text_settings = [
        TextParse('Service name', token='.', keep_split=0, max_splits=1),
        #TextParse('Service_Type', token='.', keep_split=1, max_splits=2),
        TextParse('Agent', token='@', keep_split=1)
        ]
    # Read in the data
    dcf_table = get_dcf_table(time=time_span, query=table_name)
    calc_history = parse_dcf_query(dcf_table)
    # Clean the data
    calc_history = set_times(calc_history, time_settings)
    calc_history = trim_text(calc_history, text_settings)
    # Get two components from 'Service name'
    #calc_history['Service_Type'] = calc_history['Service name']
    calc_history = split_items(calc_history, split_settings)
    calc_history.rename(columns=column_names, inplace=True)
    drop_list = [
        'Service request attributes',
        'Active work at start time',
        'Expected resource usage'
        ]
    calc_history.drop(columns=drop_list, inplace=True)
    calc_history = identify_jobs(calc_history)
    return calc_history


def load_dcf_execution(time_span='Week', table_name='HistorySummary'):
    '''Query the DCF status logs and extract the requested table.
    Keyword Arguments:
        table_name {str} -- The name for the desired table.
            (default: {'History'})
        time_span {str} -- The time span (where applicable) to use for
            modifying the table query.  (default: {'Day'})
    Returns:
        pd.DataFrame -- The requested data as a DataFrame.
'''
    # Name conversion for Execution history data columns
    column_names = {
        'Servant': 'Servant',
        'Client': 'Client',
        'Service name': 'ServiceReference',
        'Start time': 'JobStartTime',
        'End time': 'JobEndTime',
        'Duration': 'JobDuration',
        'Processor time used': 'ProcessorTime',
        'Expected processor usage': 'ExpectedProcessorUsage',
        'Expected memory usage [MB]': 'ExpectedMemoryUsage',
        'Expected GPU usage': 'ExpectedGPUusage',
        'Expected GPU memory usage [MB]': 'ExpectedGPUmemoryUsage',
        'Assigned GPU device IDs': 'GPUs',
        'Real processor usage': 'ActualProcessorUsage',
        'Real memory usage [MB]': 'ActualMemoryUsage'
        }
    # Time conversion settings for Execution History
    time_settings = [
        TimeSet('Start time'),
        TimeSet('End time'),
        TimeSet('Duration', unit='seconds', delta=True),
        TimeSet('Processor time used', unit='seconds', delta=True)
        ]
    # Text cleaning settings for DCF History
    text_settings = [
        TextParse('Service name', token='.', keep_split=0, max_splits=1)
        ]
    # Read in the data
    dcf_table = get_dcf_table(time=time_span, query=table_name)
    calc_history = parse_dcf_query(dcf_table)
    # Clean the data
    calc_history = set_times(calc_history, time_settings)
    calc_history = trim_text(calc_history, text_settings)
    calc_history.rename(columns=column_names, inplace=True)
    return calc_history


def load_dcf_services(time_span='Week'):
    '''Query the available DCF algorithm services and extract the results.
    Keyword Arguments:
        time_span {str} -- The time span (where applicable) to use for
            modifying the table query.  (default: {'Week'})
    Returns:
        pd.DataFrame -- The requested data as a DataFrame.
'''
    table_name='ServiceNames'
    # Name conversion for Execution history data columns
    column_names = {
        'Algorithm': 'Algorithm',
        'Calculation': 'Calculation',
        'Version': 'Version',
        'Agents': 'AgentCount',
        'Clients': 'ClientCount',
        'Calls in progress': 'ActiveJobs',
        'Calls waiting': 'WaitingJobs'
        }
    # Split Service name into three parts  Algorithm, Calculation, Version
    text_settings = [
        TextParse('Algorithm', token='.', keep_split=0, max_splits=1),
        TextParse('Calculation', token='.', keep_split=1, max_splits=2),
        TextParse('Version', token='.', keep_split=2, max_splits=2),
        TextParse('Version', token='x', keep_split=0, max_splits=1, right=True)
        ]
    # Read in the data
    dcf_table = get_dcf_table(time=time_span, query=table_name)
    dcf_services = parse_dcf_query(dcf_table)

    # Clean the data
    dcf_services['Algorithm'] = dcf_services['Service name']
    dcf_services['Calculation'] = dcf_services['Service name']
    dcf_services['Version'] = dcf_services['Service name']
    dcf_services = trim_text(dcf_services, text_settings)
    dcf_services.rename(columns=column_names, inplace=True)
    column_order = list(column_names.values())
    return dcf_services[column_order]


def load_dcf_status(table_name='Agents'):
    '''Query the DCF status logs and extract the requested table.
    Keyword Arguments:
        table_name {str} -- The name for the desired table.
            (default: {'Agents'})
    Returns:
        pd.DataFrame -- The requested data as a DataFrame.
'''
    # Name conversion for Execution history data columns
    column_names = {
        'Agent': 'AgentFullName',
        'Host name': 'Agent',
        'IP address': 'IPaddress',
        'Connection established': 'ConnectionTime',
        'User name': 'UserName',
        'Software version': 'SoftwareVersion',
        'Operating system': 'OperatingSystem',
        'CPU type': 'CPUtype',
        'CPU count': 'CPUcount',
        'CPU info': 'CPUinfo',
        'CPU layout': 'CPUlayout',
        'CPU score': 'CPUscore',
        'Memory size (MB)': 'MemorySize',
        'LAAAA space (MB)': 'LAAAAspace',
        'GPUs': 'GPUs',
        'Link speed (Mbps)': 'LinkSpeed'
        }
    # Text cleaning settings for DCF History
    text_settings = [
        TextParse('GPUs', token=',', keep_split=1, max_splits=2)
        ]
    # Read in the data
    dcf_table = get_dcf_table(time='Now', query=table_name)
    agent_status = parse_dcf_query(dcf_table)
    # Clean the data
    agent_status = trim_text(agent_status, text_settings)
    agent_status.rename(columns=column_names, inplace=True)
    return agent_status
