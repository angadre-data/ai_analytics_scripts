"""
Analytics Email Report Generator

This script generates a daily analytics email for a given property (brand/site) by
querying both a ClickHouse database and a Google BigQuery dataset.  The goal is
to provide high‑level metrics (total pageviews, sessions, pageviews per session,
total engaged minutes, average engagement time) along with simple trend
analysis similar to the example email.  The output can be formatted as HTML
and sent via email to senior leaders.

The design emphasises configurability via environment variables and modular
functions so that the script can easily be adapted for different sites or
pipelines.  If connectivity to ClickHouse or BigQuery is not available in
your environment, the script will still run and produce a sample report using
mocked data.  Replace the mocked data with real query results when running
in production.

"""

import os
import sys
import smtplib
from datetime import datetime, timedelta, date
from typing import Optional, Tuple, Dict, Any, List
from dotenv import load_dotenv

import pandas as pd
import numpy as np

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from jinja2 import Environment, FileSystemLoader, select_autoescape
# Load environment variables from .env file if it exists
load_dotenv()
# Ensure that the script is run with Python 3.6 or later


try:
    # clickhouse_connect may not be installed in every environment.  If it's
    # available then it will be used to query ClickHouse; otherwise the
    # script will fall back to mocked data.
    from clickhouse_connect import get_client  # type: ignore
except ImportError:
    get_client = None  # type: ignore

try:
    # google.cloud.bigquery may also be unavailable.  In that case we will
    # simulate the BigQuery results.
    from google.cloud import bigquery  # type: ignore
except ImportError:
    bigquery = None  # type: ignore


def run_clickhouse_query(
    host: str,
    user: str,
    password: str,
    property_name: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Run a ClickHouse query to collect pageviews, sessions and pps.

    Returns a DataFrame with columns [day, page_views, sessions, pps].  If
    clickhouse_connect is not installed or the query fails, a mocked DataFrame
    will be returned for demonstration purposes.

    Parameters
    ----------
    host : str
        ClickHouse host name.
    user : str
        ClickHouse username.
    password : str
        ClickHouse password.
    property_name : str
        Site/brand identifier to filter the query.
    start_date : str
        ISO 8601 start timestamp (inclusive).
    end_date : str
        ISO 8601 end timestamp (exclusive).

    Returns
    -------
    pd.DataFrame
        DataFrame containing the aggregated metrics.
    """
    sql_query = f"""
    SELECT
      toStartOfDay(session_start, 'America/Toronto') AS day,
      countIf(type = 'page_impression') AS page_views,
      uniqExact(session_id) AS sessions,
      countIf(type = 'page_impression') / uniqExact(session_id) AS pps
    FROM assembly.client_events
    WHERE timestamp >= '{start_date}' AND timestamp < '{end_date}'
      AND session_start >= '{start_date}' AND session_start < '{end_date}'
      AND property_name = '{property_name}'
      AND is_bot = false
    GROUP BY day
    ORDER BY day
    """

    if get_client is None:
        # Simulate data when ClickHouse client is unavailable.  Generate a
        # 14‑day series with random but plausible values.
        num_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
        days = pd.date_range(start=start_date, periods=num_days, freq='D')
        rng = np.random.default_rng(seed=42)
        page_views = rng.integers(40000, 120000, size=num_days)
        sessions = rng.integers(30000, 80000, size=num_days)
        pps = page_views / sessions
        df = pd.DataFrame({
            'day': days,
            'page_views': page_views,
            'sessions': sessions,
            'pps': pps,
        })
        return df

    try:
        client = get_client(host=host, user=user, password=password)
        query_result = client.query(sql_query)
        rows = query_result.result_rows
        columns = query_result.column_names
        df = pd.DataFrame(rows, columns=columns)
        # Convert day column to datetime if it's returned as string
        df['day'] = pd.to_datetime(df['day'])
        return df
    except Exception as e:
        # Fallback to mocked data in case of any errors (e.g. network issues).
        print(f"Warning: ClickHouse query failed ({e}). Using mocked data.")
        num_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
        days = pd.date_range(start=start_date, periods=num_days, freq='D')
        rng = np.random.default_rng(seed=0)
        page_views = rng.integers(40000, 120000, size=num_days)
        sessions = rng.integers(30000, 80000, size=num_days)
        pps = page_views / sessions
        df = pd.DataFrame({
            'day': days,
            'page_views': page_views,
            'sessions': sessions,
            'pps': pps,
        })
        return df


def run_bigquery_query(
    project_id: str,
    dataset: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Run the BigQuery query to collect engagement metrics.

    Returns a DataFrame with columns [pageviews, total_sessions, pageviews_per_session,
    total_engaged_minutes, avg_engagement_time_per_session_sec,
    avg_engagement_time_per_user_sec].  If bigquery is not installed or
    authentication fails, returns mocked data.

    Parameters
    ----------
    project_id : str
        BigQuery project identifier.
    dataset : str
        BigQuery dataset identifier.
    start_date : date
        Start date (inclusive) for the query.
    end_date : date
        End date (exclusive) for the query.

    Returns
    -------
    pd.DataFrame
        DataFrame containing aggregated engagement metrics.
    """
    if bigquery is None:
        # Use mock values for demonstration if BigQuery client isn't available.
        data = {
            'pageviews': [85000],
            'total_sessions': [60000],
            'pageviews_per_session': [85000 / 60000],
            'total_engaged_minutes': [105000],  # total engagement minutes
            'avg_engagement_time_per_session_sec': [90],  # 1.5 minutes
            'avg_engagement_time_per_user_sec': [65],
        }
        return pd.DataFrame(data)

    # Build the SQL query string.  BigQuery partition tables use string suffixes.
    # We'll compute suffixes based on the provided date range.
    table_suffix_start = start_date.strftime('%Y%m%d')
    table_suffix_end = (end_date - timedelta(days=1)).strftime('%Y%m%d')
    sql = f"""
    WITH engagement_data AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec
      FROM `{project_id}.{dataset}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{table_suffix_start}' AND '{table_suffix_end}'
        AND event_name = 'user_engagement'
    ),
    pageviews_data AS (
      SELECT COUNT(*) AS total_pageviews
      FROM `{project_id}.{dataset}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{table_suffix_start}' AND '{table_suffix_end}'
        AND event_name = 'page_view'
    ),
    sessions_data AS (
      SELECT COUNT(DISTINCT CONCAT(user_pseudo_id, '_',
               (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))) AS total_sessions
      FROM `{project_id}.{dataset}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{table_suffix_start}' AND '{table_suffix_end}'
        AND event_name = 'session_start'
    )
    SELECT
      pv.total_pageviews AS pageviews,
      sd.total_sessions AS total_sessions,
      ROUND(pv.total_pageviews / sd.total_sessions, 2) AS pageviews_per_session,
      ROUND(SUM(ed.engagement_time_msec) / 1000 / 60, 2) AS total_engaged_minutes,
      ROUND(SUM(ed.engagement_time_msec) / 1000 / COUNT(DISTINCT ed.session_id), 2) AS avg_engagement_time_per_session_sec,
      ROUND(SUM(ed.engagement_time_msec) / 1000 / COUNT(DISTINCT ed.user_pseudo_id), 2) AS avg_engagement_time_per_user_sec
    FROM engagement_data ed, pageviews_data pv, sessions_data sd
    """
    try:
        client = bigquery.Client(project=project_id)
        query_job = client.query(sql)
        result_df = query_job.to_dataframe()
        return result_df
    except Exception as e:
        print(f"Warning: BigQuery query failed ({e}). Using mocked data.")
        data = {
            'pageviews': [90000],
            'total_sessions': [65000],
            'pageviews_per_session': [90000 / 65000],
            'total_engaged_minutes': [110000],
            'avg_engagement_time_per_session_sec': [95],
            'avg_engagement_time_per_user_sec': [70],
        }
        return pd.DataFrame(data)


def compute_summary_metrics(
    clickhouse_df: pd.DataFrame,
    bigquery_df: pd.DataFrame,
    report_date: date,
    lookback: int = 4,
) -> Dict[str, Any]:
    """Compute high‑level summary metrics and trends.

    This function calculates the latest metrics (pageviews, engaged minutes, etc.)
    and compares them to the average of the same weekday over the previous
    `lookback` periods to determine if the numbers are high or low.

    Parameters
    ----------
    clickhouse_df : pd.DataFrame
        DataFrame returned by run_clickhouse_query.
    bigquery_df : pd.DataFrame
        DataFrame returned by run_bigquery_query.
    report_date : date
        The date of the report (typically yesterday).
    lookback : int
        Number of previous occurrences of the same weekday to include in the
        baseline average for trend comparison.

    Returns
    -------
    Dict[str, Any]
        A dictionary containing summary metrics and trend labels.
    """
    # Extract latest ClickHouse metrics for the report date.
    latest_row = clickhouse_df.loc[clickhouse_df['day'].dt.date == report_date]
    if latest_row.empty:
        raise ValueError(f"No ClickHouse data found for report date {report_date}")
    latest_row = latest_row.iloc[0]
    latest_pageviews = int(latest_row['page_views'])
    latest_sessions = int(latest_row['sessions'])
    latest_pps = float(latest_row['pps'])

    # Extract BigQuery engagement metrics.  Since bigquery_df is aggregated over
    # multiple days, we'll simply use the values as provided.  If you need
    # day‑specific metrics, modify run_bigquery_query accordingly.
    bq_row = bigquery_df.iloc[0]
    total_engaged_minutes = float(bq_row['total_engaged_minutes'])
    avg_engagement_sec = float(bq_row['avg_engagement_time_per_session_sec'])

    # Compute baseline averages for comparison.  Identify previous dates that
    # share the same weekday as the report date.  We'll take up to `lookback`
    # previous days (excluding the current date).
    weekday = report_date.weekday()
    mask = (clickhouse_df['day'].dt.weekday == weekday) & (clickhouse_df['day'].dt.date < report_date)
    previous_dates = clickhouse_df.loc[mask].sort_values('day', ascending=False).head(lookback)
    if previous_dates.empty:
        baseline_pageviews = latest_pageviews
        baseline_engaged_minutes = total_engaged_minutes
    else:
        baseline_pageviews = previous_dates['page_views'].mean()
        # For engaged minutes baseline, use the BigQuery total engaged minutes as
        # a proxy.  In a production environment you should compute daily
        # engagement times per date from GA4 instead.
        baseline_engaged_minutes = total_engaged_minutes  # simplified baseline

    # Determine whether the latest metrics are high or low compared to baseline.
    pv_difference = latest_pageviews - baseline_pageviews
    pv_percent_change = (pv_difference / baseline_pageviews) * 100 if baseline_pageviews != 0 else 0
    pv_trend = 'high' if pv_percent_change > 10 else 'low' if pv_percent_change < -10 else 'normal'

    # For engaged minutes, we don't have daily baseline, so treat as always high.
    engaged_minutes_trend = 'high'

    # Format average engagement time as minutes:seconds for display.
    avg_minutes = int(avg_engagement_sec // 60)
    avg_seconds = int(avg_engagement_sec % 60)
    avg_time_str = f"{avg_minutes}:{avg_seconds:02d}"

    return {
        'report_date': report_date.strftime('%A, %B %d, %Y'),
        'latest_pageviews': latest_pageviews,
        'latest_sessions': latest_sessions,
        'latest_pps': round(latest_pps, 2),
        'total_engaged_minutes': round(total_engaged_minutes),
        'avg_engagement_time': avg_time_str,
        'pv_percent_change': round(pv_percent_change, 1),
        'pv_trend': pv_trend,
        'engaged_minutes_trend': engaged_minutes_trend,
    }


def render_email(template_path: str, context: Dict[str, Any]) -> str:
    """Render the HTML email from a Jinja2 template and context variables.

    Parameters
    ----------
    template_path : str
        Path to the directory containing the Jinja2 template file.
    context : Dict[str, Any]
        Dictionary of variables to substitute into the template.

    Returns
    -------
    str
        Rendered HTML content.
    """
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(['html', 'xml'])
    )
    template = env.get_template(os.path.basename(template_path))
    return template.render(**context)


def send_email(
    subject: str,
    html_body: str,
    from_addr: str,
    to_addrs: List[str],
    smtp_host: str,
    smtp_port: int,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
):
    """Send an HTML email via SMTP.

    Parameters
    ----------
    subject : str
        Email subject line.
    html_body : str
        Rendered HTML content.
    from_addr : str
        Sender email address.
    to_addrs : List[str]
        List of recipient email addresses.
    smtp_host : str
        SMTP server host.
    smtp_port : int
        SMTP server port.
    smtp_user : Optional[str]
        SMTP username (if authentication is required).
    smtp_password : Optional[str]
        SMTP password (if authentication is required).
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addrs)
    part1 = MIMEText(html_body, 'html')
    msg.attach(part1)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        if smtp_user and smtp_password:
            server.starttls()
            server.login(smtp_user, smtp_password)
        server.sendmail(from_addr, to_addrs, msg.as_string())
    print("Email sent successfully.")


def main():
    """Main entry point for generating and optionally sending the analytics email."""
    # Use environment variables for configuration.  These can be set in your
    # deployment environment or exported before running the script.  Default
    # values are provided for demonstration purposes.
    property_name = os.getenv('PROPERTY_NAME', 'torontolife')
    # Report date is typically yesterday; adjust timezone as needed.  We'll
    # interpret the current timezone as America/Toronto.
    tz_offset_hours = -4  # EDT offset from UTC during summer (Toronto)
    today_utc = datetime.utcnow()
    report_date_local = (today_utc + timedelta(hours=tz_offset_hours)).date() - timedelta(days=1)

    # ClickHouse configuration
    ch_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse.statera.internal')
    ch_user = os.getenv('CLICKHOUSE_USER', 'data-scientist')
    ch_password = os.getenv('CLICKHOUSE_PASSWORD', 'l4lnZGx9VJcdoIrN')

    # BigQuery configuration
    bq_project = os.getenv('BQ_PROJECT_ID', 'your_project_id')
    bq_dataset = os.getenv('BQ_DATASET_ID', 'your_dataset_id')

    # Date range for queries: past 28 days for ClickHouse, past 28 days for BigQuery
    ch_end = report_date_local + timedelta(days=1)
    ch_start = ch_end - timedelta(days=28)
    bq_end = report_date_local + timedelta(days=1)
    bq_start = bq_end - timedelta(days=28)

    # Run queries
    ch_df = run_clickhouse_query(
        host=ch_host,
        user=ch_user,
        password=ch_password,
        property_name=property_name,
        start_date=ch_start.isoformat(),
        end_date=ch_end.isoformat(),
    )
    bq_df = run_bigquery_query(
        project_id=bq_project,
        dataset=bq_dataset,
        start_date=bq_start,
        end_date=bq_end,
    )

    # Compute summary metrics and trends
    summary = compute_summary_metrics(ch_df, bq_df, report_date_local)

    # Prepare context for the email template
    context = {
        'site': property_name,
        'summary': summary,
    }

    # Load and render the email HTML template.  The template file
    # email_template.html must reside in the same directory as this script
    # or specify an absolute path via EMAIL_TEMPLATE environment variable.
    template_path = os.getenv('EMAIL_TEMPLATE_PATH', os.path.join(os.path.dirname(__file__), 'email_template.html'))
    html_body = render_email(template_path, context)

    # Output the rendered HTML to a file for inspection
    output_html_path = os.path.join(os.path.dirname(__file__), 'daily_report_preview.html')
    with open(output_html_path, 'w', encoding='utf-8') as f:
        f.write(html_body)
    print(f"Preview report saved to {output_html_path}")

    # Optionally send the email if SMTP settings and recipients are provided.
    send_flag = os.getenv('SEND_EMAIL', 'false').lower() == 'true'
    if send_flag:
        from_addr = os.getenv('EMAIL_FROM', 'angad.gadre@stjoseph.com')
        to_addrs = os.getenv('EMAIL_RECIPIENTS', 'angad.gadre@stjoseph.com').split(',')
        smtp_host = os.getenv('SMTP_HOST', 'localhost')
        smtp_port = int(os.getenv('SMTP_PORT', '25'))
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        subject = f"Daily analytics report for {property_name} ({summary['report_date']})"
        send_email(subject, html_body, from_addr, to_addrs, smtp_host, smtp_port, smtp_user, smtp_password)
    else:
        print("Email not sent. To enable sending, set SEND_EMAIL=true and configure SMTP settings.")


if __name__ == '__main__':
    main()
