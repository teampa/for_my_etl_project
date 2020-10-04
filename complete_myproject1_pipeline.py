# -*- coding: utf-8 -*-
"""
Created on Sat Sep 26 15:00:50 2020

@author: Taeam
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import requests
from lxml import html
import pandas as pd

def scrape_cmc_top3():
    coin_list = ['bitcoin','ethereum','xrp']
    topcoin = []
    list_df = []
    
    
    for coin in coin_list: 
        url ='https://coinmarketcap.com/currencies/'+coin+'/historical-data/?start=20190920&end=20200920'
        
    
        resp = requests.get(url, headers={
            'User-Agent': 'your-user-agent'
        })
    
        tree = html.fromstring(html=resp.content)
        
    
        d = {
         "Name":tree.xpath("//div[@class='cmc-details-panel-header__name']/h1/text()")[0],
         "Date":tree.xpath("//td[@class='cmc-table__cell cmc-table__cell--sticky cmc-table__cell--left']/div/text()"),
         "Open":tree.xpath("//td[2]/div/text()"),
         "High":tree.xpath("//td[3]/div/text()"),
         "Low":tree.xpath("//td[4]/div/text()"),
         "Close":tree.xpath("//td[5]/div/text()"),
         "Volume":tree.xpath("//td[6]/div/text()"),
         "Market cap":tree.xpath("//td[7]/div/text()"),
         }
    
        topcoin.append(d)
        
    
    for i in range(0, len(topcoin)):    
        df = pd.DataFrame.from_dict(topcoin[i])
        list_df.append(df)
        coin_name = list_df[i]['Name'][0].strip().upper()
        filename = "OHLCV_CMC_"+coin_name+".csv"
        list_df[i].to_csv("/home/airflow/gcs/data/"+filename, index=False)
        
def find_btc_return():
    df_btc = pd.read_csv('/home/airflow/gcs/data/OHLCV_CMC_BITCOIN.csv')
    df_btc['Date'] = pd.to_datetime(df_btc['Date']).dt.date
    df_btc.drop(columns = ['Name','Open', 'High', 'Low', 'Volume', 'Market cap'], axis=1, inplace=True)
    for i in range(0, len(df_btc)):
        df_btc['Close'][i] = df_btc['Close'][i].replace(",","")
            
    df_btc.sort_values('Date',inplace = True)
    df_btc['Close'] = df_btc['Close'].astype(float)
    df_btc['btc_return']  = df_btc['Close'].pct_change()
     
    df_btc.drop('Close', axis=1, inplace=True)
    df_btc.to_csv("/home/airflow/gcs/data/btc_daily_return.csv", index=False)

def find_eth_return():
    df_eth = pd.read_csv('/home/airflow/gcs/data/OHLCV_CMC_ETHEREUM.csv')
            
    df_eth['Date'] = pd.to_datetime(df_eth['Date']).dt.date
    df_eth.drop(columns = ['Name','Open', 'High', 'Low', 'Volume', 'Market cap'], axis=1, inplace=True)
            
    df_eth.sort_values('Date',inplace = True)
    df_eth['Close'] = df_eth['Close'].astype(float)
        
    df_eth['eth_return']  = df_eth['Close'].pct_change()
    df_eth.drop('Close', axis=1, inplace=True)
    df_eth.to_csv("/home/airflow/gcs/data/eth_daily_return.csv", index=False)
    
def find_xrp_return():
    df_xrp = pd.read_csv('/home/airflow/gcs/data/OHLCV_CMC_XRP.csv')
            
    df_xrp['Date'] = pd.to_datetime(df_xrp['Date']).dt.date
    df_xrp.drop(columns = ['Name','Open', 'High', 'Low', 'Volume', 'Market cap'], axis=1, inplace=True)
        
    df_xrp.sort_values('Date',inplace = True)
    df_xrp['Close'] = df_xrp['Close'].astype(float)
        
    df_xrp['xrp_return']  = df_xrp['Close'].pct_change()
    df_xrp.drop('Close', axis=1, inplace=True)
    df_xrp.to_csv("/home/airflow/gcs/data/xrp_daily_return.csv", index=False)
    
def combine_top3():
    df_btc = pd.read_csv('/home/airflow/gcs/data/btc_daily_return.csv')
    df_eth = pd.read_csv('/home/airflow/gcs/data/eth_daily_return.csv')
    df_xrp = pd.read_csv('/home/airflow/gcs/data/xrp_daily_return.csv')
        
    dfs = [df.set_index('Date') for df in [df_btc, df_eth, df_xrp]]        
    df_all_top3 = pd.concat(dfs, axis=1).reset_index()
    df_all_top3.dropna(inplace=True)
    df_all_top3.to_csv("/home/airflow/gcs/data/result_cmc.csv", index=False)

# Default Args    
default_args = {
    'owner': 'parinya.s',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow1@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}

# Create DAG

dag = DAG(
    'top3_cmc_pipeline_final',
    default_args=default_args,
    description='Pipeline for ETL top3_Coinmarketcap data',
    schedule_interval=timedelta(days=1),
)

# scraping_data 
t1 = PythonOperator(
    task_id='scraping_data_cmc',
    python_callable=scrape_cmc_top3,
    dag=dag,
)

#manipulate_btc
t2 = PythonOperator(
    task_id='clean_btc',
    python_callable=find_btc_return,
    dag=dag,
)

#manipulate_eth
t3 = PythonOperator(
    task_id='clean_eth',
    python_callable=find_eth_return,
    dag=dag,
)

#manipulate_xrp
t4 = PythonOperator(
    task_id='clean_xrp',
    python_callable=find_xrp_return,
    dag=dag,
)

#combine_result
t5 = PythonOperator(
    task_id='combine_data',
    python_callable=combine_top3,
    dag=dag,
)

t6 = BashOperator(
    task_id='bq_load',
    bash_command='bq load --source_format=CSV --autodetect \
            cmc_dataset.crypto_daily_return\
            gs://[GCS_BUCKET]/data/result_cmc.csv',
    dag=dag,
)

t1 >> [t2, t3, t4] >> t5 >> t6




