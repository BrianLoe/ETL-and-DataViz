import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from kaggle.api.kaggle_api_extended import KaggleApi

data_location = 'koluit/human-resource-data-set-the-company' # kaggle data location
download_path = 'data' # path to where we want to place the data in
log_file_name = 'logfile.txt' # name of the log file
state_data_url = 'https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971' # url for US states data
api_exrate_url = 'https://v6.exchangerate-api.com/v6/bbcdd4717012c0fb7b20f062/latest/USD' # url for exchange rate data in USD
date_format = '%m/%d/%Y'

default_args = {
    'owner':'Brian',
    'start_date':datetime(2023,7,25),
    'email':['brianloe7@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

def _downloadFrom_kaggle(data_location, download_path):
    """
    Download the data from kaggle using data_location into download_path
    Args:
    data_location: string object of location of data to be downloaded
    download_path: string object of local path to store the downloaded data
    Return:
    N/A
    """
    # Authenticate Kaggle using usrename and password stored in kaggle.json file (hidden)
    api = KaggleApi()
    api.authenticate()
    # Donwload
    api.dataset_download_files(data_location, path=download_path)

def log(message, log_file=log_file_name):
    """
    Write the message provided into a file with the format "%Timestamp% message" and prints the message into stdout
    Args:
    message: string object for message to be written into log_file
    log_file: string object containing the log file name
    Return:
    N/A
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp+', '+message+'\n')
    print(message)
    
def webscrape_stateData(state_data_url):
    """
    Scrape the US states data from state_data_url. Change code if url is different from provided.
    Args:
    state_data_url: The url string containing US states data (provided).
    Return:
    A dataframe containing the full state name and the alpha code (abbreviation) as columns. 
    """
    html_data = requests.get(state_data_url).text
    soup = BeautifulSoup(html_data, 'html5lib')
    table = soup.find_all('table')[0] # The located table
    # initialise table dictionary
    table_dict = {}
    table_dict['StateFull']=[]
    table_dict['Alpha code']=[]

    i = 0
    for row in table.find_all('td'): # Iterate rows
        if i%3==0 and i!=0: # There are 3 columns in 'td' element, we need to iterate every 3 column to get to the new row
            i=0 # reset i
        # Extract state name located in first column
        if i==0: 
            table_dict['StateFull'].append(' '.join(row.contents))
        # Extract state code located in third column
        if i==2:
            table_dict['Alpha code'].append(' '.join(row.contents))
        i+=1
        
    state_df = pd.DataFrame(table_dict)
    return state_df
   
def get_current_exchangerate(api_exrate_url):
    """
    Get the latest exchange rate data in USD currency (conversion rates).
    Args:
    api_exrate_url: The url string for the api exchange rate
    Return:
    Dictionary object with currency code as key and conversion rates as value 
    """
    jsondata = requests.get(api_exrate_url).json() # get json data
    exrates_df = jsondata['conversion_rates'] # get only the conversion rates
    return exrates_df

def convert_to_datetime(data, date_column, kwargs=None):
    """
    Convert date_column type to pandas datetime according to date_format if specified otherwise
    preserve the formatting
    Args:
    data: the dataframe object
    date_column: string object of the date column name
    args: dictionary object containing extra arguments that can be specified. Defaults to None (optional)
    Return:
    Series object with converted date column
    """
    if kwargs is not None:
        converted_dt = pd.to_datetime(data[date_column], **kwargs)
    else:
        converted_dt = pd.to_datetime(data[date_column])
        
    return converted_dt
      
def extract_data():
    """
    Executes data extraction process. Returns extracted data when successfull, otherwise log an error.
    """
    log('Extracting Data...')
    start_time = time.time()
    # read data from txt files
    try:
        company_data = pd.read_csv('data/2021.06_COL_2021.txt', sep='\t')
        job_data = pd.read_csv('data/2021.06_job_profile_mapping.txt', sep='\t')
        full_data = pd.read_csv('data/CompanyData.txt', sep='\t', encoding='utf_16_le')
        demographic_data = pd.read_csv('data/Diversity.txt', sep='\t')
        survey_data = pd.read_csv('data/EngagementSurvey.txt', sep='\t')
        log('Extraction done in --- %s seconds ---' % (time.time()-start_time))
        return company_data,job_data,full_data,demographic_data,survey_data
    except Exception as error:
        log('Extracting Data failed, error caught: '+repr(error))
        log('\n')
        raise
        
def transform_data(company_data, job_data, full_data, demographic_data, survey_data):
    """
    Executes data transformation process. Returns transformed data when successfull, otherwise log an error.
    Args:
    company_data, job_data, full_data, demographic_data, survey_data: Dataframe objects to be transformed
    Return:
    The transformed data of all arguments or throws an error when failure.
    """
    log('Transforming Data...')
    start_time = time.time()
    try:
        # transform extracted datasets
        print('The company data has {0} columns and {1} rows'.format(company_data.shape[1],company_data.shape[0]))
        print('The job data has {0} columns and {1} rows'.format(job_data.shape[1],job_data.shape[0]))
        print('The full data has {0} columns and {1} rows'.format(full_data.shape[1],full_data.shape[0]))
        print('The demographic data has {0} columns and {1} rows'.format(demographic_data.shape[1],demographic_data.shape[0]))
        print('The survey data has {0} columns and {1} rows'.format(survey_data.shape[1],survey_data.shape[0]))
        # make a deep copy of dataframes so we do not apply changes to the original data
        comp_df = company_data.copy(deep=True)
        job_df = job_data.copy(deep=True)
        full_df = full_data.copy(deep=True)
        demo_df = demographic_data.copy(deep=True)
        survey_df = survey_data.copy(deep=True)
        ### Transforming company data
        comp_df.drop('COL Amount', axis=1, inplace=True) # drop the 'COL Amount' column
        ### Transforming job data
        job_df.rename(columns={' Compensation ':'Compensation'}, inplace=True) # stripping extra whitespace
        job_df['Compensation'] = job_df['Compensation'].str.strip().str.replace(',','') # stripping whitespace and removing ','
        job_df['Compensation'] = pd.to_numeric(job_df['Compensation']) # convert to numerical
        ## Validating job profile
        job_df.rename(columns={'Bonus %':'Bonus_pct', 'Level':'level'}, inplace=True) # renaming columns
        col_to_drop = list(job_df.columns) # columns to be dropped
        col_to_drop.remove('Compensation') # except compensation
        log('--- Transforming job data...')
        job_copy = job_df.copy() # make a copy to test equality
        if not job_df.equals(job_copy): # check if the copied job equal to the original df. raise error and abort if not equal
            msg = 'Copied job_df does not contain the exact same information as the original dataframe.'
            log(f'{msg} Aborting...')
            raise ValueError(msg)
        ## Resolving consistency of job profile in job data and full data
        job_copy = job_copy[['Job_Profile']] # take the job profiles
        job_copy = job_copy.merge(full_df[col_to_drop], on='Job_Profile', how='left').drop_duplicates() # drop any duplicated job profiles
        job_copy.reset_index(drop=True,inplace=True) 
        job_copy.drop('Job_Profile',axis=1, inplace=True)
        job_copy['Job_Profile']=['JP_'+str(i) for i in range(1000,1000+job_copy.shape[0])] # create new job profiles based on full data
        df_len_jp = len(job_copy[job_copy.duplicated(subset='Job_Profile')].index) # record length of duplicated job profiles (if any)
        # check if there are still duplicated job profiles
        if df_len_jp!=0:
            msg = f'{df_len_jp} duplicates still found after the job profiles are created.'
            log(f'{msg} Aborting...')
            raise ValueError(msg)
        # merge back the job data
        col_to_drop.append('Compensation') # we need `compensation` for merging
        col_to_drop.remove('Job_Profile') # except job profile
        col_copy = col_to_drop.copy() 
        col_copy.remove('Compensation') # we do not need 'compensation' for column names
        job_copy = job_copy.merge(job_df[col_to_drop], on=col_copy, how='left')
        full_df.drop('Job_Profile', axis=1, inplace=True) # drop the job profile in fulldata
        col_to_drop.remove('Compensation') # we do not need 'compensation' in the full data
        full_df = full_df.merge(job_copy, on=col_to_drop, how='left')
        job_df = job_copy
        log('--- Transforming done in job data')
        ### Transforming full data
        ## Validating termination date
        # convert to datetime
        log("--- Transforming full data...")
        full_df['Start_Date'] = convert_to_datetime(full_df, 'Start_Date', {'format':date_format})
        log("------ Replacing termination date value of 12/12/2999 to NaN...")
        full_df['Termination_Date'] = convert_to_datetime(full_df, 'Termination_Date', {'errors':'coerce', 'format':date_format}) # this turns invalid date into NaT/NaN
        # check if there are any data that has start date > termination date
        df_len_termination = len(full_df.loc[full_df['Termination_Date']<full_df['Start_Date'], :].index)
        # record errors if there are still rows having start date > termination date
        replace_flag1 = False
        if df_len_termination!=0:
            log(f"(------ Replacing termination date value failed. Continuing...")
            replace_flag1 = True
        # check if there are any data that has termination date > today's date, otherwise we will transform them to today's date
        log("------ Converting violating termination date values to today's date")
        if len(full_df.loc[full_df['Termination_Date']>datetime.today(),:].index)!=0:
            full_df.loc[full_df['Termination_Date']>datetime.today(), 'Termination_Date']=datetime.today()
        # drop 'Notes' column if exists
        try:
            log("------ Dropping 'notes' column")
            full_df.drop('Notes', axis=1, inplace=True)
        except KeyError:
            log("------ 'notes' column is not found. Continuing...")
            pass
        ## deriving date features
        log("------ Adding start_year and termination_year")
        full_df['start_year']=full_df['Start_Date'].dt.strftime('%Y')
        full_df['termination_year']=full_df['Termination_Date'].dt.strftime('%Y')
        full_df['termination_year'].fillna(0, inplace=True) # Fill null values in termination year with 0
        full_df = full_df.astype({'start_year':int,'termination_year':int})
        ## validating active status
        # this checks if there are any active employees but also has been terminated
        log("------ Inactivating status for terminated employees")
        if len(full_df.loc[(full_df['Active Status']==1) & (full_df['termination_year']>0)].index)!=0: 
            # in this case we want to change it to 0
            full_df.loc[(full_df['Active Status']==1) & (full_df['termination_year']>0), 'Active Status']=0 
        log("------ Adding tenure_year and tenure_months")
        full_df['diff_in_days']=None # this is for calculating tenure
        # for former employees
        
        full_df.loc[full_df['Active Status']==0, 'diff_in_days'] = (full_df['Termination_Date'] - full_df['Start_Date'])
        # for current employees
        full_df.loc[full_df['Active Status']==1, 'diff_in_days'] = (datetime.today()-full_df['Start_Date'])
        full_df['tenure_months'] = full_df['diff_in_days']/timedelta(days=30)
        full_df['tenure_years'] = full_df['diff_in_days']/timedelta(days=365)
        full_df.drop('diff_in_days', axis=1, inplace=True)
        ## cleaning inconsistent state abbreviation
        # check states for country besides US
        log("------ Cleaning state column")
        null_len = full_df.loc[full_df['Country']!='US']['State'].isna().sum()
        ## webscraping state data and merged it into full data
        state_df = webscrape_stateData(state_data_url)
        full_df=full_df.merge(state_df, on='StateFull', how='left')
        # chcek if we correctly change the abbreviated state only for US based states
        null_len2 = full_df.loc[full_df['Country']!='US']['State'].isna().sum()
        clean_flag1 = False
        if null_len2!=null_len:
            log(f"------ Cleaning state column failed. Continuing...")
            clean_flag1 = True
        full_df.drop('State', axis=1, inplace=True)
        full_df.rename(columns={'Alpha code':'State_code'}, inplace=True) # rename alpha code to state_code
        # manually handle Washington and Washington DC state
        full_df.loc[full_df['StateFull']=='Washington DC', 'State_code']='DC'
        full_df.loc[full_df['State_code']=='DC', 'StateFull']='District of Columbia'
        ## updating age based on today's date
        log("------ Updating age values")
        full_df['DOB']=convert_to_datetime(full_df, 'DOB')
        full_df['Age']=round((datetime.today()-full_df['DOB'])/timedelta(days=365))
        ## convert currencies to USD based on current rates
        log("------ Converting salary to USD")
        exrates_df = get_current_exchangerate(api_exrate_url)
        full_df['Salary(USD)'] = full_df.apply(lambda x: x.Salary/exrates_df[x.Currency], axis=1)
        full_df.drop('Salary', axis=1, inplace=True) # drop unconverted salary column
        # track changes that were done in full data
        if replace_flag1 or clean_flag1:
            log("------ Transformation that failed in full data on {0} :".format(datetime.today()))
            if replace_flag1:
                log(f"------ Replacing termination date value failed. {df_len_termination} rows have start date > termination date.")
            if clean_flag1:
                log(f"------ Cleaning state column failed. Inconsistent null values found from {null_len} now {null_len2}")
        log("--- Transformation complete in full data")
        # Transforming demographic data
        demo_df['Race/Ethnicity'].fillna('Prefer not to say', inplace=True) # fill nulls with 'prefer not to say' 
        # Transforming survey data
        survey_df['survey_quarter'] = survey_df['Survey'].str[-2:]
        survey_df['survey_year'] = survey_df['Survey'].str[:4]
        # drop null values
        survey_df.dropna(axis=1, inplace=True)
        survey_df.drop('Survey', axis=1, inplace=True)
        # Validating active status
        survey_df = survey_df.merge(full_df[['EmployeeID', 'Active Status']], on='EmployeeID', how='left')
        survey_df = survey_df[survey_df['Active Status']==1] # only keep active employees
        
        survey_df.drop('Active Status', axis=1, inplace=True)
        log('Transformation done in --- %s seconds ---' % (time.time()-start_time))
        return comp_df,job_df,full_df,demo_df,survey_df
    except Exception as error:
        log('Transforming Data failed, error caught: '+repr(error))
        log('\n')
        raise

def load(comp_df,job_df,full_df,demo_df,survey_df):
    log('Loading Data...')
    start_time = time.time()
    try:
        comp_df.to_csv('cleanData/company_details.csv', index=False)
        job_df.to_csv('cleanData/job_details.csv', index=False)
        full_df.to_csv('cleanData/main_data.csv', index=False)
        demo_df.to_csv('cleanData/employee_details.csv', index=False)
        survey_df.to_csv('cleanData/survey_data.csv', index=False)
        log('Loading done in --- %s seconds ---' % (time.time()-start_time))
        log('\n')
        return
    except Exception as error:
        log('Loading Data failed, error caught: '+repr(error))
        log('\n')
        raise
    
def performETL_data():
    """
    Function to call the ETL processes in order.
    """
    company_data,job_data,full_data,demographic_data,survey_data = extract_data() 
    comp_df,job_df,full_df,demo_df,survey_df = transform_data(company_data, job_data, full_data, demographic_data, survey_data)
    load(comp_df,job_df,full_df,demo_df,survey_df)    

if __name__ == '__main__':
    # dag = DAG(
    #     'hr_ETL_process',
    #     default_args=default_args,
    #     schedule='@daily',
    #     description='ETL process for HR Data from Kaggle',
    # )
    # download_data = PythonOperator(
    #     task_id = 'Download_datafromKaggle',
    #     python_callable = _downloadFrom_kaggle,
    #     dag=dag,
    # )
    # unzip_data = BashOperator(
    #     task_id = 'unzip_downloadedData',
    #     bash_command = 'unzip data/human-resource-data-set-the-company.zip > rm -f data/human-resource-data-set-the-company.zip',
    #     dag=dag,
    # )
    # extract_transform_load = PythonOperator(
    #     task_id = 'ETL_process',
    #     python_callable = performETL_data(),
    #     dag=dag,
    # )
    # # sequence initialisation
    # download_data >> unzip_data >> extract_transform_load
    _downloadFrom_kaggle(data_location, download_path)
    subprocess.run('powershell -command "Expand-Archive -Force E:\Documents\ETL-and-DataViz\human_resource\HR\data\human-resource-data-set-the-company.zip E:\Documents\ETL-and-DataViz\human_resource\HR\data"', shell=True)
    subprocess.run('del /f "E:\Documents\ETL-and-DataViz\human_resource\HR\data\human-resource-data-set-the-company.zip"', shell=True)
    
    performETL_data()
    