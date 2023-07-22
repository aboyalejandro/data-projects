import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

class ExportDestination:
    def __init__(self, task_id_xcom, folder_path, postgres_table):
        self.task_id_xcom = task_id_xcom
        self.folder_path = folder_path
        self.postgres_table = postgres_table

    def report_to_csv(self, **kwargs):
        df = kwargs['ti'].xcom_pull(task_ids=self.task_id_xcom) 
        print('Successfully downloaded csv to tmp folder.')
        df.to_csv(f'/tmp/{self.folder_path}_{pd.Timestamp.now().strftime("%y%m%d%H%M%S")}.csv', index=False)
        return f'{self.folder_path} updated succesfully.'

    def send_to_posgres(self, **kwargs):
        data = kwargs['ti'].xcom_pull(task_ids=self.task_id_xcom)
        postgres_hook = PostgresHook(postgres_conn_id="POSTGRESQL")
        data.to_sql(self.postgres_table, postgres_hook.get_sqlalchemy_engine(), if_exists='append', chunksize=1000)
        return f'{self.postgres_table} updated succesfully.'