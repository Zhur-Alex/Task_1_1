from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


#------------------------Подключение_к_БД------------------------
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from datetime import datetime
from time import sleep

engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
connection = engine.connect()

#----------------------Инициализация_таблиц----------------------
from sqlalchemy import Table, Column, Integer, Date, VARCHAR, CHAR, Numeric, Float, MetaData


workflow_name = "Task_1_1"


metadata_obj1 = MetaData(schema = 'ds') # DS - детальный слой
metadata_obj2 = MetaData(schema = 'ds')
metadata_obj3 = MetaData(schema = 'ds')
metadata_obj4 = MetaData(schema = 'ds')
metadata_obj5 = MetaData(schema = 'ds')
metadata_obj6 = MetaData(schema = 'ds')
metadata_obj7 = MetaData(schema = 'logs')

table1 = Table(
    "ft_balance_f",
    metadata_obj1,
    Column("row_nb", Integer),
    Column("on_date", Date, nullable=False, primary_key=True),
    Column("account_rk", Numeric, nullable=False, primary_key=True),
    Column("currency_rk", Numeric),
    Column("balance_out", Float)
)

table2 = Table(
    "ft_posting_f",
    metadata_obj2,
    Column("row_nb", Integer, primary_key=True),   #<----------------- тут первичный ключ, т.к. все остальное может быть неуникальным
    Column("oper_date", Date, nullable=False, primary_key=True),
    Column("credit_account_rk", Numeric, nullable=False, primary_key=True),
    Column("debet_account_rk", Numeric, nullable=False, primary_key=True),
    Column("credit_amount", Float),
    Column("debet_amount", Float)
)

table3 = Table(
    "md_account_d",
    metadata_obj3,
    Column("row_nb", Integer),
    Column("data_actual_date", Date, nullable=False, primary_key=True),
    Column("data_actual_end_date", Date, nullable=False),
    Column("account_rk", Numeric, nullable=False, primary_key=True),
    Column("account_number", VARCHAR(20), nullable=False),
    Column("char_type", CHAR, nullable=False),
    Column("currency_rk", Numeric, nullable=False),
    Column("currency_code", VARCHAR(3), nullable=False)
)

table4 = Table(
    "md_currency_d",
    metadata_obj4,
    Column("row_nb", Integer),
    Column("currency_rk", Numeric, nullable=False, primary_key=True),
    Column("data_actual_date", Date, nullable=False, primary_key=True),
    Column("data_actual_end_date", Date), 
    Column("currency_code", VARCHAR(3)),
    Column("code_iso_char", VARCHAR(3))
)

table5 = Table(
    "md_exchange_rate_d",
    metadata_obj5,
    Column("row_nb", Integer, nullable=False, primary_key=True),  # <----------------- тут первичный ключ, т.к. выдало ошибку
    Column("data_actual_date", Date, nullable=False, primary_key=True),
    Column("data_actual_end_date", Date), 
    Column("currency_rk", Numeric, nullable=False, primary_key=True),
    Column("reduced_cource", Float),
    Column("code_iso_num", VARCHAR(3))
)

table6 = Table(
    "md_ledger_account_s",
    metadata_obj6,
    Column("row_nb", Integer),
    Column("chapter", CHAR), 
    Column("chapter_name", VARCHAR(16)), 
    Column("section_number", Integer,),
    Column("section_name", VARCHAR(22)),
    Column("subsection_name", VARCHAR(21)),
    Column("ledger1_account", Integer), 
    Column("ledger1_account_name", VARCHAR(47)),
    Column("ledger_account", Integer, nullable=False, primary_key=True),
    Column("ledger_account_name", VARCHAR(153)),
    Column("characteristic", CHAR),
    Column("is_resident", Integer), 
    Column("is_reserve", Integer), 
    Column("is_reserved", Integer), 
    Column("is_loan", Integer), 
    Column("is_reserved_assets", Integer), 
    Column("is_overdue", Integer), 
    Column("is_interest", Integer), 
    Column("pair_account", Integer),
    Column("start_date", Date, nullable=False, primary_key=True),
    Column("end_date", Date),
    Column("is_rub_only", Integer), 
    Column("min_term", CHAR),
    Column("min_term_measure", CHAR),
    Column("max_term", CHAR),
    Column("max_term_measure", CHAR),
    Column("ledger_acc_full_name_translit", CHAR),
    Column("is_revaluation", CHAR),
    Column("is_correct", CHAR)
)

#---------------------Функции для логирования#---------------------
from functools import wraps

def start_wf_log():
    if Variable.get("Task_1_1_run_id") == None:
        Variable.set("Task_1_1_run_id", 1)
    else:
        value = int(Variable.get("Task_1_1_run_id")) + 1
        Variable.set("Task_1_1_run_id", value)
        
    row_timestamp = datetime.now()
    wf_name = workflow_name,
    
    run_id = Variable.get("Task_1_1_run_id")
    
    event = 'start'
    event_date = datetime.now()
    event_datetime = datetime.now()
    
    columns = ['row_timestamp', 'wf_name', 'run_id', 'event', 'event_date', 'event_datetime']
    
    cort = (row_timestamp, wf_name, run_id, event, event_date, event_datetime)

    df_start = pd.DataFrame([cort], columns=columns)
    sleep(5)
    df_start.to_sql(name='wf_log', con=engine, schema='logs', if_exists='append', index=False)
    

def end_wf_log():

    row_timestamp = datetime.now()
    wf_name = workflow_name,
    
    
    #run_id = (pd.read_sql_query('SELECT COALESCE(MAX(run_id),0) as run_id FROM logs.wf_log', connection).iloc[0][0]) 
    run_id = Variable.get("Task_1_1_run_id")
    
    
    event = 'end'
    event_date = datetime.now()
    event_datetime = datetime.now()

    columns = ['row_timestamp', 'wf_name', 'run_id', 'event', 'event_date', 'event_datetime']
    
    cort = (row_timestamp, wf_name, run_id, event, event_date, event_datetime)
    
    df_end = pd.DataFrame([cort], columns=columns)
    df_end.to_sql(name='wf_log', con=engine, schema='logs', if_exists='append', index=False)

def task_log_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_date_launche = datetime.today()
        task_name = func.__name__
        run_id = Variable.get("Task_1_1_run_id")
        time_start = datetime.now()
        more_info = 'all_good'
        result = 'SUCCESS!'
        
        try:
            func(*args, **kwargs)
        except Exception as ex:
            result = 'FAIL...'
            more_info = ex
            print(f"ERROR: {ex}")
        sleep(1)
        time_end = datetime.now()
        time_duration = (time_end - time_start).total_seconds()
        
        columns = ['row_timestamp', 'task_name', 'run_id', 'time_start', 'time_end', 'duration_sec', 'result', 'info']
    
        cort = (task_date_launche, task_name, run_id, time_start, time_end, time_duration, result, more_info)

        df_start = pd.DataFrame([cort], columns=columns)
        df_start.to_sql(name='tasks_log', con=engine, schema='logs', if_exists='append', index=False)
    return wrapper       
 

#------------------------Создание_таблиц-------------------------
@task_log_function
def create_table1():                                                      #   <----------------------------------
    metadata_obj1.create_all(engine)

@task_log_function
def create_table2():                                                      #   <----------------------------------
    metadata_obj2.create_all(engine)

@task_log_function    
def create_table3():                                                      #   <----------------------------------
    metadata_obj3.create_all(engine)

@task_log_function    
def create_table4():                                                      #   <----------------------------------
    metadata_obj4.create_all(engine)

@task_log_function   
def create_table5():                                                      #   <----------------------------------
    metadata_obj5.create_all(engine)

@task_log_function   
def create_table6():                                                      #   <----------------------------------
    metadata_obj6.create_all(engine)


#----------------------Загрузка_данных_в_БД#----------------------
import pandas as pd

@task_log_function
def download_file1():   
        dtype_date_col = {'on_date': Date}                                                                                           #   <----------------------------------
        df_1 = pd.read_csv('/root/airflow-data/ft_balance_f.csv', sep=";")
        df_1.columns = df_1.columns.str.lower()
        df_1.to_sql(name='ft_balance_f', con=engine, schema='ds', if_exists='replace', index=False, dtype=dtype_date_col)
        
        
@task_log_function
def download_file2():                                                                                                                #   <----------------------------------
        dtype_date_col = {'oper_date': Date}  
        df_2 = pd.read_csv('/root/airflow-data/ft_posting_f.csv', sep=";")
        df_2.columns = df_2.columns.str.lower()
        df_2.to_sql(name='ft_posting_f', con=engine, schema ='ds', if_exists='replace', index=False, dtype=dtype_date_col)
        
        
@task_log_function
def download_file3():
        dtype_date_col = {'data_actual_date': Date, 'data_actual_end_date': Date}                                                    #   <----------------------------------
        df_3 = pd.read_csv('/root/airflow-data/md_account_d.csv', sep=";")
        df_3.columns = df_3.columns.str.lower()
        df_3.to_sql(name='md_account_d', con=engine, schema ='ds', if_exists='replace', index=False, dtype=dtype_date_col)
        
        
@task_log_function
def download_file4():                                                                                                                #   <----------------------------------
        dtype_date_col = {'data_actual_date': Date, 'data_actual_end_date': Date} 
        df_4 = pd.read_csv('/root/airflow-data/md_currency_d.csv', sep=";")
        df_4.columns = df_4.columns.str.lower()
        df_4.to_sql(name='md_currency_d', con=engine, schema ='ds', if_exists='replace', index=False, dtype=dtype_date_col)
        

@task_log_function
def download_file5():                                                                                                                #   <----------------------------------
        dtype_date_col = {'data_actual_date': Date, 'data_actual_end_date': Date} 
        df_5 = pd.read_csv('/root/airflow-data/md_exchange_rate_d.csv', sep=";")
        df_5.columns = df_5.columns.str.lower()
        df_5.to_sql(name='md_exchange_rate_d', con=engine, schema ='ds', if_exists='replace', index=False, dtype=dtype_date_col)
        

@task_log_function
def download_file6():                                                                                           
        dtype_date_col = {'start_date': Date, 'end_date': Date} 
        df_6 = pd.read_csv('/root/airflow-data/md_ledger_account_s.csv', sep=";", encoding="cp866")             # encoding="cp866" добавил для корректности чтения CSV
        df_6.columns = df_6.columns.str.lower()
        df_6.to_sql(name='md_ledger_account_s', con=engine, schema ='ds', if_exists='replace', index=False, dtype=dtype_date_col)


#----------------------Инициализация DAG'a#------------------------
args = {
    'owner': 'Alexandr',
    'start_date':datetime(2024, 7, 3),
    'provide_context':True
}

with DAG(
    dag_id = workflow_name, 
    description='work_with_DS', 
    schedule_interval=None,  
    catchup=False, 
    default_args=args) as dag:

    start_task=PythonOperator(
        task_id='start_task',
        python_callable=start_wf_log
    )

    end_task=PythonOperator(
        task_id='end_task',
        python_callable=end_wf_log
    )
	
    create_task_1=PythonOperator(
        task_id='create_task_1',
        python_callable=create_table1
    )
    
    create_task_2=PythonOperator(
        task_id='create_task_2',
        python_callable=create_table2
    )
    
    create_task_3=PythonOperator(
        task_id='create_task_3',
        python_callable=create_table3
    )
    
    create_task_4=PythonOperator(
        task_id='create_task_4',
        python_callable=create_table4
    )
    
    create_task_5=PythonOperator(
        task_id='create_task_5',
        python_callable=create_table5
    )
    
    create_task_6=PythonOperator(
        task_id='create_task_6',
        python_callable=create_table6
    )
    
    download_date_task_1=PythonOperator(
        task_id='download_task_1',
        python_callable=download_file1
    )
    
    download_date_task_2=PythonOperator(
        task_id='download_task_2',
        python_callable=download_file2
    )
    
    download_date_task_3=PythonOperator(
        task_id='download_task_3',
        python_callable=download_file3
    )
    
    download_date_task_4=PythonOperator(
        task_id='download_task_4',
        python_callable=download_file4
    )
    
    download_date_task_5=PythonOperator(
        task_id='download_task_5',
        python_callable=download_file5
    )
    
    download_date_task_6=PythonOperator(
        task_id='download_task_6',
        python_callable=download_file6
    )

    start_task >> create_task_1
    start_task >> create_task_2
    start_task >> create_task_3
    start_task >> create_task_4
    start_task >> create_task_5
    start_task >> create_task_6
    create_task_1 >> download_date_task_1
    create_task_2 >> download_date_task_2
    create_task_3 >> download_date_task_3
    create_task_4 >> download_date_task_4
    create_task_5 >> download_date_task_5
    create_task_6 >> download_date_task_6
    download_date_task_1 >> end_task
    download_date_task_2 >> end_task
    download_date_task_3 >> end_task
    download_date_task_4 >> end_task
    download_date_task_5 >> end_task
    download_date_task_6 >> end_task 