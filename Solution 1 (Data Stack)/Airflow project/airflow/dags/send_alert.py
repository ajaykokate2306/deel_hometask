from airflow import DAG
from airflow.decorators import task # For taskflow API
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
import logging
from datetime import datetime
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# from airflow.operators.bash import BashOperator


def run_alerts():
    """Check only the latest day's balances for >50% changes."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = hook.get_conn()
    cur = conn.cursor()

    query = """
        WITH balances AS (
                SELECT
                    organization_id,
                    date,
                    BALANCE_AMOUNT,
                    LAG(BALANCE_AMOUNT) OVER (
                        PARTITION BY organization_id ORDER BY date
                    ) AS prev_balance
                FROM DEEL_DB.FACTS.FCT_INVOICES_AGGREGATED
            ),
            latest_day AS (
                SELECT MAX(date) AS max_date FROM balances
            )

            SELECT
                b.organization_id,
                b.date,
                b.BALANCE_AMOUNT,
                b.prev_balance,
                ABS(b.BALANCE_AMOUNT - b.prev_balance) / NULLIF(b.prev_balance, 0) difference
            FROM balances b
            JOIN latest_day ld ON b.date = ld.max_date
            WHERE b.prev_balance IS NOT NULL
            AND ABS(b.BALANCE_AMOUNT - b.prev_balance) / NULLIF(b.prev_balance, 0) > 0.5
    """

    cur.execute(query)
    rows = cur.fetchall()

    print(len(rows))

    if not rows:
        logging.info("No balance changes >50 on the latest day.")
    else:
        for org, date_str, balance, prev, diff in rows:
            logging.warning(
                f"ALERT: Org {org} balance changed >50% on {date_str}: {prev} → {balance}"
            )
        
    cur.close()
    conn.close()



with DAG(
    dag_id="send_alert_via_slack_updated",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["snowflake"],
) as dag:
    
    send_alert = PythonOperator(
        task_id = 'send_alert_to_slack',
        python_callable = run_alerts
    )
    
    send_alert



# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from datetime import datetime

# SNOWFLAKE_CONN_ID = "snowflake"

# with DAG(
#     dag_id="snowflake_example_dag",
#     start_date=datetime(2025, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     tags=["snowflake"],
# ) as dag:

#     # Create a table if not exists
#     create_table = SnowflakeOperator(
#         task_id="create_table",
#         snowflake_conn_id=SNOWFLAKE_CONN_ID,
#         sql="""
#         CREATE TABLE IF NOT EXISTS deel_db.facts.my_table (
#             id INT,
#             name STRING,
#             created_at TIMESTAMP
#         );
#         """,
#     )

#     # Insert sample data
#     insert_data = SnowflakeOperator(
#         task_id="insert_data",
#         snowflake_conn_id=SNOWFLAKE_CONN_ID,
#         sql="""
#         INSERT INTO deel_db.facts.my_table (id, name, created_at)
#         VALUES (1, 'Rajesh', CURRENT_TIMESTAMP);
#         """,
#     )

#     # Query the table
#     query_data = SnowflakeOperator(
#         task_id="query_data",
#         snowflake_conn_id=SNOWFLAKE_CONN_ID,
#         sql="SELECT * FROM deel_db.facts.my_table;",
#     )

#     create_table >> insert_data >> query_data
