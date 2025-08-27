from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.slack_operator import SlackAPIPostOperator

import logging
from datetime import datetime


def run_alerts(**context):
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
                FROM DEEL_DB.FACTS.fct_org_daily_balances
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

    cur.close()
    conn.close()

    if not rows:
        logging.info("No balance changes >50% on the latest day.")
        return "No significant balance changes today."

    messages = []
    for org, date_str, balance, prev, diff in rows:
        msg = (
            f"ALERT: Org {org} balance changed >50% on {date_str}: "
            f"{prev} → {balance} ({diff:.2%})"
        )
        logging.warning(msg)
        messages.append(msg)

    # Join all alerts into one Slack message
    return "\n".join(messages)


with DAG(
    dag_id="balance_alert",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["snowflake", "slack"],
) as dag:

    check_data = PythonOperator(
        task_id="check_data",
        python_callable=run_alerts,
        provide_context=True,
    )

    send_alert = SlackAPIPostOperator(
            task_id='send_alert',
            slack_conn_id='slack_api',  # Ensure this connection is configured in Airflow
            channel='#balance-change-alert',  # Replace with your Slack channel name or ID
            text="Hello! This is an automated trigger to notify about >50 balance change in the organizations. Please find the details below: \n\n"  + "{{ ti.xcom_pull(task_ids='check_data') }}",
            
        )
 
    check_data >> send_alert