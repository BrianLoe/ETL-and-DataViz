import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from kaggle.api.kaggle_api_extended import KaggleApi

data_location = 'koluit/human-resource-data-set-the-company' # kaggle data location
download_path = 'data' # path to where we want to place the data in

default_args = {
    'owner':'Brian',
    'start_date':datetime(2023,7,25),
    'email':['brianloe7@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

def _downloadFrom_kaggle():
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(data_location, path=download_path)

def log(message):
#     timestamp_format = '%Y-%h-%d-%H:%M:%S'
#     now = datetime.now()
#     timestamp = now.strftime(timestamp_format)
#     with open("logfile.txt",'a') as f:
#         f.write(timestamp+', '+message+'\n')
    print(message)
    
def webscrape_stateData():
    state_data_url = 'https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971'
    html_data = requests.get(state_data_url).text
    soup = BeautifulSoup(html_data, 'html5lib')
    table = soup.find_all('table')[0] # The located table
    table_dict = {}

    table_dict['StateFull']=[]
    table_dict['Alpha code']=[]

    i = 0
    for row in table.find_all('td'): # Iterate rows
        if i%3==0 and i!=0: # There are 3 columns in 'td' element, we need to iterate every 3 column to get to the new row
            i=0 # reset i
        # Extract state name
        if i==0: 
            table_dict['StateFull'].append(' '.join(row.contents))
        # Extract state code
        if i==2:
            table_dict['Alpha code'].append(' '.join(row.contents))
        i+=1
    state_df = pd.DataFrame(table_dict)
    return state_df
   
def get_current_exchangerate():
    api_url = 'https://v6.exchangerate-api.com/v6/bbcdd4717012c0fb7b20f062/latest/USD'
    jsondata = requests.get(api_url).json()
    exrates_df = jsondata['conversion_rates']
    return exrates_df
      
def extract_data():
    log('Extracting Data ...')
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
        return
        
def transform_data(company_data, job_data, full_data, demographic_data, survey_data):
    log('Transforming Data ...')
    start_time = time.time()
    try:
        # transform extracted datasets
        print('The company data has {0} columns and {1} rows'.format(company_data.shape[1],company_data.shape[0]))
        print('The job data has {0} columns and {1} rows'.format(job_data.shape[1],job_data.shape[0]))
        print('The full data has {0} columns and {1} rows'.format(full_data.shape[1],full_data.shape[0]))
        print('The demographic data has {0} columns and {1} rows'.format(demographic_data.shape[1],demographic_data.shape[0]))
        print('The survey data has {0} columns and {1} rows'.format(survey_data.shape[1],survey_data.shape[0]))
        # make a copy of dataframes
        comp_df = company_data.copy()
        job_df = job_data.copy()
        full_df = full_data.copy()
        demo_df = demographic_data.copy()
        survey_df = survey_data.copy()
        # Transforming company data
        comp_df.drop('COL Amount', axis=1, inplace=True)
        # Transforming job data
        job_df.rename(columns={' Compensation ':'Compensation'}, inplace=True)
        job_df['Compensation'] = job_df['Compensation'].str.strip().str.replace(',','')
        job_df['Compensation'] = pd.to_numeric(job_df['Compensation'])
        # Validating job profile
        job_df.rename(columns={'Bonus %':'Bonus_pct', 'Level':'level'}, inplace=True)
        col_to_drop = list(job_df.columns)
        col_to_drop.remove('Compensation')
        job_copy = job_df.copy()
        assert job_df.equals(job_copy), 'copied job_df does not contain the exact same information as the original dataframe.'
        job_copy = job_copy[['Job_Profile']]
        job_copy = job_copy.merge(full_df[col_to_drop], on='Job_Profile', how='left').drop_duplicates()
        job_copy.reset_index(drop=True,inplace=True)
        job_copy.drop('Job_Profile',axis=1, inplace=True)
        job_copy['Job_Profile']=['JP_'+str(i) for i in range(1000,1000+job_copy.shape[0])] # create new job profiles based on full data
        df_len = len(job_copy[job_copy.duplicated(subset='Job_Profile')].index)
        assert df_len==0, f'{df_len} duplicates still found after the job profiles are created' # Check if there are still duplicates
        col_to_drop.append('Compensation')
        col_to_drop.remove('Job_Profile')
        col_copy = col_to_drop.copy()
        col_copy.remove('Compensation')
        job_copy = job_copy.merge(job_df[col_to_drop], on=col_copy, how='left')
        full_df.drop('Job_Profile', axis=1, inplace=True) # drop the job profile in fulldata
        col_to_drop.remove('Compensation')
        full_df = full_df.merge(job_copy, on=col_to_drop, how='left')
        job_df = job_copy
        # Transforming full data
        # Validating termination date
        full_df['Start_Date']=pd.to_datetime(full_df['Start_Date'])
        full_df['Termination_Date']=pd.to_datetime(full_df['Termination_Date'], errors='coerce')
        # check if there are any data that has start date > termination date
        df_len = len(full_df.loc[full_df['Termination_Date']<full_df['Start_Date'], :].index)
        assert df_len==0, f'{df_len} rows have start date > termination date'
        # check if there are any data that has termination date > today's date, otherwise we will transform them to today's date
        if len(full_df.loc[full_df['Termination_Date']>datetime.today(),:].index)!=0:
            full_df['Termination_Date'].loc[full_df['Termination_Date']>datetime.today()]=datetime.today()
        full_df.drop('Notes', axis=1, inplace=True)
        # deriving date features
        full_df['start_year']=full_df['Start_Date'].dt.strftime('%Y')
        full_df['termination_year']=full_df['Termination_Date'].dt.strftime('%Y')
        full_df['termination_year'].fillna(0, inplace=True) # Fill null values in termination year with 0
        full_df = full_df.astype({'start_year':int,'termination_year':int})
        # validating active status
        if df_len(full_df.loc[(full_df['Active Status']==1) & (full_df['termination_year']>0)]).indexn!=0:
            full_df['Active Status'].loc[(full_df['Active Status']==1) & (full_df['termination_year']>0)]=0
        full_df['diff_in_days']=0
        full_df['diff_in_days'].loc[full_df['Active Status']==0]=(full_df['Termination_Date']-full_df['Start_Date'])
        full_df['diff_in_days'].loc[full_df['Active Status']==1]=(datetime.today()-full_df['Start_Date'])
        full_df['tenure_months']=full_df['diff_in_days']/timedelta(days=30)
        full_df['tenure_years']=full_df['diff_in_days']/timedelta(days=365)
        full_df.drop('diff_in_days', axis=1, inplace=True)
        # cleaning inconsistent state abbreviation
        null_len = full_df.loc[full_df['Country']!='US']['State'].isna().sum()
        print('Null values for states beside US =', null_len)
        # webscraping state data
        state_df = webscrape_stateData()
        full_df=full_df.merge(state_df, on='StateFull', how='left')
        full_df.head()
        # chcek if we correctly change the abbreviated state only for US based states
        null_len2 = full_df.loc[full_df['Country']!='US']['State'].isna().sum()
        assert null_len2==df_len, f'Inconsistent null values found from {df_len} now {null_len2}'
        full_df.drop('State', axis=1, inplace=True)
        full_df.rename(columns={'Alpha code':'State_code'}, inplace=True)
        # manually handle Washington and Washington DC state
        full_df['State_code'].loc[full_df['StateFull']=='Washington DC']='DC'
        full_df['StateFull'].loc[full_df['State_code']=='DC']='District of Columbia'
        # updating age based on today's date
        full_df['DOB']=pd.to_datetime(full_df['DOB'])
        full_df['Age']=round((datetime.today()-full_df['DOB'])/timedelta(days=365))
        # convert currencies to USD based on current rates
        exrates_df = get_current_exchangerate()
        full_df['Salary(USD)'] = full_df.apply(lambda x: x.Salary/exrates_df[x.Currency], axis=1)
        full_df.drop('Salary', axis=1, inplace=True) # drop uncoverted salary column
        # track changes that were done in full data
        log("Transformation that were done in full data on {0} :".format(datetime.today()))
        log("- Replacing termination date value of 12/12/2999 to NaN")
        log("- Converting violating termination date values to today's date")
        log("- Drop 'notes' column")
        log("- Adding start_year and termination_year")
        log("- Inactivating status for terminated employees")
        log("- Adding tenure_year and tenure_months")
        log("- Cleaning state column")
        log("- Updating age values")
        log("- Converting salary to USD")
        # Transforming demographic data
        demo_df['Race/Ethnicity'].fillna('Prefer not to say', inplace=True) # fill nulls with 'prefer not to say' 
        # Transforming survey data
        survey_df['survey_quarter'] = survey_df['Survey'].str[-2:]
        survey_df['survey_year'] = survey_df['Survey'].str[:4]
        # drop null values
        survey_df.dropna(inplace=True)
        # Validating active status
        survey_df = survey_df.merge(full_df[['EmployeeID', 'Active Status']], on='EmployeeID', how='left')
        survey_df = survey_df[survey_df['Active Status']==1]
        survey_df.drop('Active Status', axis=1, inplace=True)
        log('Transformation done in --- %s seconds ---' % (time.time()-start_time))
        return comp_df,job_df,full_df,demo_df,survey_df
    except Exception as error:
        log('Transforming Data failed, error caught: '+repr(error))
        return
        
def ETL_data():
    company_data,job_data,full_data,demographic_data,survey_data = extract_data() 
    comp_df,job_df,full_df,demo_df,survey_df = transform_data(company_data, job_data, full_data, demographic_data, survey_data)
    load(comp_df,job_df,full_df,demo_df,survey_df)    

def load(comp_df,job_df,full_df,demo_df,survey_df):
    log('Loading Data ...')
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
        return

if __name__ == '__main__':
    dag = DAG(
        'hr_ETL_process',
        default_args=default_args,
        schedule='@daily',
        description='ETL process for HR Data from Kaggle',
    )
    download_data = PythonOperator(
        task_id = 'Download_datafromKaggle',
        python_callable = _downloadFrom_kaggle,
        dag=dag,
    )
    unzip_data = BashOperator(
        task_id = 'unzip_downloadedData',
        bash_command = 'unzip data/human-resource-data-set-the-company.zip > rm -f data/human-resource-data-set-the-company.zip',
        dag=dag,
    )
    extract_transform_load = PythonOperator(
        task_id = 'ETL_process',
        python_callable = ETL_data(),
        dag=dag,
    )
    # sequence initialisation
    download_data >> unzip_data >> extract_transform_load
    