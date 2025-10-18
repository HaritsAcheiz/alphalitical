# dags/news_scraper_via_ssh.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'harits',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="news_scraper_via_ssh",
    start_date=datetime(2025, 10, 17),
    schedule='30 7 * * *',
    catchup=False,
    tags=["news_scraper", "gateway"]
) as dag:
    run_scraper = SSHOperator(
        task_id="run_scraper",
        ssh_conn_id="ssh_gateway",
        command=(
            "source /opt/venvs/news_scraper/bin/activate && "
            "python3 /opt/news_scraper/scraper.py"
        ),
        get_pty=True
    )
    run_transformer = SSHOperator(
        task_id="run_transformer",
        ssh_conn_id="ssh_gateway",
        command=(
            "source /opt/venvs/news_scraper/bin/activate && "
            "python3 /opt/news_scraper/transformer.py"
        ),
        get_pty=True
    )
    run_loader = SSHOperator(
        task_id="run_loader",
        ssh_conn_id="ssh_gateway",
        command=(
            "source /opt/venvs/news_scraper/bin/activate && "
            "python3 /opt/news_scraper/loader.py"
        ),
        get_pty=True
    )

run_scraper >> run_transformer >> run_loader