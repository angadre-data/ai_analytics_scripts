"""
Analytics Email Report Generator - Multi-Property Version

This script generates a weekly analytics email for multiple properties (brands/sites)
by querying a ClickHouse database and optionally a Google BigQuery dataset. It
provides per-property 7-day metrics including revenue, ad impressions, IPP, RPS,
and a top-10 articles breakdown by pageviews.

The report is generated in plain-text format and sent via SMTP if enabled.
"""

import os
import sys
import smtplib
import logging
import warnings
import socket
from datetime import datetime, timedelta, date, timezone
from typing import Optional, Dict, Any, List, Tuple
from dotenv import load_dotenv

# Suppress urllib3 OpenSSL warning
warnings.filterwarnings('ignore', message='.*urllib3 v2 only supports OpenSSL.*')

import pandas as pd
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Optional CSS inlining library
try:
    from premailer import transform  # type: ignore
except ImportError:
    transform = None  # type: ignore

# Load environment variables from .env without clearing existing ones
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

try:
    from clickhouse_connect import get_client  # type: ignore
except ImportError:
    get_client = None  # type: ignore

try:
    from google.cloud import bigquery  # type: ignore
except ImportError:
    bigquery = None  # type: ignore


def run_clickhouse_query_multi_property(
    host: str,
    user: str,
    password: str,
    properties: List[str],
    start_ts: datetime,
    end_ts: datetime
) -> pd.DataFrame:
    """
    Run the multi-property 7-day summary query.
    
    Returns DataFrame with columns:
    property_name, total_pageviews, total_sessions, total_revenue,
    total_ad_impressions, ad_impressions_gt_0, ipp, rps, total_new_articles_published
    """
    if get_client is None:
        logger.warning("ClickHouse client not available, returning empty DataFrame")
        return pd.DataFrame()
    
    # Format properties list for SQL IN clause
    properties_str = "', '".join(properties)
    
    # Format timestamps for ClickHouse
    start_ts_str = start_ts.strftime('%Y-%m-%d %H:%M:%S')
    end_ts_str = end_ts.strftime('%Y-%m-%d %H:%M:%S')
    
    sql_query = f"""
    /* Per-property 7-day summary */
    WITH
      toDateTime('{start_ts_str}') AS start_ts_local,
      toDateTime('{end_ts_str}') AS end_ts_local,

    /* Minimal metadata: PostType + PublishedAt for "new articles" count */
    published_meta AS (
      SELECT
        property_name,
        wp_post_id,
        dictGetOrNull('assembly.wordpress_metadata','PostType',   (property_name, wp_post_id)) AS post_type,
        toDateTime(dictGetOrNull('assembly.wordpress_metadata','PublishedAt',(property_name, wp_post_id))) AS wp_publish_date
      FROM (
        SELECT DISTINCT property_name, wp_post_id
        FROM assembly.client_events
        WHERE wp_post_id != 0
      )
    )

    /* Base, filtered events within the window */
    SELECT
      ev.property_name,

      /* core volumes */
      countIf(ev.type = 'page_impression')                      AS total_pageviews,
      uniqExact(ev.session_id)                                  AS total_sessions,
      sum(ev.revenue)              AS total_revenue,
      countIf(ev.type = 'ad_impression')                        AS total_ad_impressions,
      countIf(ev.type = 'ad_impression' AND ev.revenue > 0)     AS ad_impressions_gt_0,

      /* KPIs */
      total_ad_impressions / NULLIF(total_pageviews, 0)         AS ipp,
      total_revenue / NULLIF(total_sessions, 0)                 AS rps,

      /* new articles published in the window (by PublishedAt) */
      uniqExactIf(ev.wp_post_id, pm.wp_publish_date >= start_ts_local AND pm.wp_publish_date < end_ts_local) AS total_new_articles_published

    FROM assembly.client_events ev
    left JOIN published_meta pm
      ON ev.property_name = pm.property_name
     AND ev.wp_post_id    = pm.wp_post_id
    WHERE
      /* apply window to both event time and session_start */
      toTimeZone(ev.timestamp, 'America/Toronto')      >= start_ts_local
      AND toTimeZone(ev.timestamp, 'America/Toronto')  <  end_ts_local
      AND toTimeZone(ev.session_start, 'America/Toronto') >= start_ts_local
      AND toTimeZone(ev.session_start, 'America/Toronto') <  end_ts_local
    AND ev.property_name IN ('{properties_str}')
      /* article-only */
      AND lower(pm.post_type) NOT IN ('page','hub-page','sjh_grid','contest','content_module')

      /* quality */
      AND ev.is_bot = 0
      AND ev.wp_post_id != 0
      AND ev.url != ''

    GROUP BY
      ev.property_name
    ORDER BY
      ev.property_name
    """
    
    try:
        logger.info(f"Querying ClickHouse for multi-property summary: {properties}")
        client = get_client(host=host, user=user, password=password)
        query_result = client.query(sql_query)
        df = pd.DataFrame(query_result.result_rows, columns=query_result.column_names)
        logger.info(f"Retrieved {len(df)} property summaries from ClickHouse")
        return df
    except Exception as e:
        logger.error(f"ClickHouse multi-property query failed: {e}")
        return pd.DataFrame()


def run_top_articles_query_multi(
    host: str,
    user: str,
    password: str,
    properties: List[str],
    start_ts: datetime,
    end_ts: datetime
) -> pd.DataFrame:
    """
    Run the top-10 articles per property query.
    
    Returns DataFrame with columns:
    property_name, wp_post_id, article_title, permalink, pageviews_7d,
    ad_impressions_7d, revenue_7d, ipp_7d, pageviews_lifetime,
    revenue_publish_date, rpm_lifetime, rank_within_property
    """
    if get_client is None:
        logger.warning("ClickHouse client not available, returning empty DataFrame")
        return pd.DataFrame()
    
    # Format properties list for SQL IN clause
    # properties_str = "', '".join(properties)
    properties_str = "'" + "', '".join(properties) + "'"
    
    # Format timestamps for ClickHouse
    start_ts_str = start_ts.strftime('%Y-%m-%d %H:%M:%S')
    end_ts_str = end_ts.strftime('%Y-%m-%d %H:%M:%S')
    
    sql_query = f"""
    WITH
    -- 7-day window (Toronto local time interpreted on server)
    toDateTime('{start_ts_str}') AS start_ts_local,
    toDateTime('{end_ts_str}') AS end_ts_local,

    -- Lifetime window lower bound: tune this for performance vs. "true" lifetime
    -- e.g. last 365 days, or earlier if you need deeper history
    toDateTime('2024-01-01 00:00:00') AS lifetime_start_ts

/* 1) 7-day metrics per article, filtered at the source */
, weekly AS (
    SELECT
        property_name,
        wp_post_id,
        countIf(type = 'page_impression')              AS pageviews_7d,
        countIf(type = 'ad_impression')                AS ad_impressions_7d,
        sum(revenue)         AS revenue_7d
    FROM assembly.client_events
    WHERE
        -- 7-day window on both timestamp and session_start
        timestamp      >= start_ts_local
        AND timestamp  <  end_ts_local
        AND session_start >= start_ts_local
        AND session_start <  end_ts_local

        -- property filter (templated)
        AND property_name IN ({properties_str})

        -- quality filters
        AND is_bot = 0
        AND wp_post_id != 0
        AND url != ''

        -- article-only via dictGet (no pre-join)
        AND lower(
              dictGet(
                'assembly.wordpress_metadata',
                'PostType',
                tuple(property_name, wp_post_id)
              )
            ) NOT IN ('page','hub-page','sjh_grid','contest','content_module')
    GROUP BY
        property_name,
        wp_post_id
)

/* 2) Rank and keep only top 10 per property by 7-day pageviews */
, top10 AS (
    SELECT
        property_name,
        wp_post_id,
        pageviews_7d,
        ad_impressions_7d,
        revenue_7d,
        ad_impressions_7d / NULLIF(pageviews_7d, 0) AS ipp_7d,
        row_number() OVER (
          PARTITION BY property_name
          ORDER BY pageviews_7d DESC
        ) AS rn
    FROM weekly
    WHERE pageviews_7d > 0
)

/* 3) Lifetime metrics restricted to top10 articles only, with a timestamp bound */
, lifetime AS (
    SELECT
        ev.property_name,
        ev.wp_post_id,
        uniqExact(ev.session_id)                      AS sessions_lifetime,
        countIf(ev.type = 'page_impression')          AS pageviews_lifetime,
        sum(ev.revenue)  AS revenue_lifetime
    FROM assembly.client_events ev
    INNER JOIN top10 t
      ON ev.property_name = t.property_name
     AND ev.wp_post_id    = t.wp_post_id
    WHERE
        ev.is_bot = 0
        AND ev.wp_post_id != 0
        AND ev.url != ''

        -- IMPORTANT: restrict lifetime scan to a reasonable horizon
        AND ev.timestamp >= lifetime_start_ts

        -- property filter again (cheap, but keeps things tidy)
        AND ev.property_name IN ({properties_str})

        -- no need to re-check PostType here: top10 already article-only
    GROUP BY
        ev.property_name,
        ev.wp_post_id
)

SELECT
    t.property_name,
    t.wp_post_id,

    -- Article title + permalink
    dictGetOrNull(
      'assembly.wordpress_metadata',
      'Title',
      tuple(t.property_name, t.wp_post_id)
    ) AS article_title,
    dictGetOrNull(
      'assembly.wordpress_metadata',
      'Permalink',
      tuple(t.property_name, t.wp_post_id)
    ) AS permalink,

    -- Publish date
    toDateTime(
      dictGetOrNull(
        'assembly.wordpress_metadata',
        'PublishedAt',
        tuple(t.property_name, t.wp_post_id)
      )
    ) AS wp_publish_date,

    -- Days since publish
    dateDiff(
      'day',
      wp_publish_date,
      now()
    ) AS days_since_publish,

    -- 7-day metrics
    t.pageviews_7d,
    t.ad_impressions_7d,
    t.revenue_7d,
    t.ipp_7d,

    -- Lifetime metrics
    l.pageviews_lifetime,
    l.sessions_lifetime,
    l.pageviews_lifetime / NULLIF(l.sessions_lifetime, 0) AS pps_lifetime,
    l.revenue_lifetime AS revenue_publish_date,
    (l.revenue_lifetime / NULLIF(l.pageviews_lifetime, 0)) * 1000 AS rpm_lifetime,

    -- Rank
    t.rn AS rank_within_property

FROM top10 t
LEFT JOIN lifetime l
  ON t.property_name = l.property_name
 AND t.wp_post_id    = l.wp_post_id
WHERE t.rn <= 10
ORDER BY
    t.property_name,
    t.rn
    """
    
    try:
        logger.info(f"Querying ClickHouse for top articles across properties: {properties}")
        client = get_client(host=host, user=user, password=password)
        query_result = client.query(sql_query)
        df = pd.DataFrame(query_result.result_rows, columns=query_result.column_names)
        logger.info(f"Retrieved {len(df)} top articles from ClickHouse")
        return df
    except Exception as e:
        logger.error(f"ClickHouse top articles query failed: {e}")
        return pd.DataFrame()


def canonicalize_brand_name(property_name: str) -> str:
    """Convert property name to display-friendly brand name."""
    brand_map = {
        'torontolife': 'Toronto Life',
        'todaysparent': 'Today\'s Parent',
        'chatelaine': 'Chatelaine',
        'macleans': 'Maclean\'s',
        'fashion': 'Fashion',
        'chatelaine_fr': 'Châtelaine'
    }
    return brand_map.get(property_name.lower(), property_name.title())


def build_report_context(
    properties: List[str],
    summary_df: pd.DataFrame,
    top_articles_df: pd.DataFrame,
    start_date: date,
    end_date: date
) -> Dict[str, Any]:
    """
    Build the context for email rendering from query results.
    """
    report_date_str = f"{start_date.strftime('%B %d')} - {end_date.strftime('%B %d, %Y')}"
    
    properties_data = []
    
    for prop in properties:
        # Get summary for this property
        prop_summary = summary_df[summary_df['property_name'] == prop]
        
        if prop_summary.empty:
            logger.warning(f"No summary data found for property: {prop}")
            continue
        
        row = prop_summary.iloc[0]
        
        # Calculate PPS (pageviews per session)
        pps = float(row['total_pageviews']) / float(row['total_sessions']) if row['total_sessions'] > 0 else 0.0
        
        # Build property summary
        property_data = {
            'name': prop,
            'display_name': canonicalize_brand_name(prop),
            'summary_7d': {
                'total_pageviews': int(row['total_pageviews']),
                'total_sessions': int(row['total_sessions']),
                'total_revenue': float(row['total_revenue']),
                'total_ad_impressions': int(row['total_ad_impressions']),
                'ad_impressions_gt_0': int(row['ad_impressions_gt_0']),
                'pps': pps,
                'ipp': float(row['ipp']),
                'rps': float(row['rps']),
            },
            'top_articles': []
        }
        
        # Get top articles for this property
        try:
            prop_articles = top_articles_df[top_articles_df['property_name'] == prop]
        except KeyError:
            logger.warning(f"Top articles data missing 'property_name' column for property {prop}; skipping top articles for this property")
            prop_articles = pd.DataFrame()
        
        for _, article in prop_articles.iterrows():
            # Get lifetime metrics
            pageviews_lifetime = int(article['pageviews_lifetime']) if pd.notna(article['pageviews_lifetime']) else 0
            
            # Get pps_lifetime from query (per-article calculation)
            pps_lifetime = None
            if 'pps_lifetime' in article.index and pd.notna(article['pps_lifetime']):
                pps_lifetime = float(article['pps_lifetime'])
            
            article_data = {
                'rank': int(article['rank_within_property']),
                'title': str(article['article_title']) if pd.notna(article['article_title']) else 'Untitled',
                'permalink': str(article['permalink']) if pd.notna(article['permalink']) else '',
                'published_date': str(article['wp_publish_date'])[:10] if pd.notna(article['wp_publish_date']) else 'N/A',
                'days_since_publish': int(article['days_since_publish']) if pd.notna(article['days_since_publish']) else 0,
                'pageviews_7d': int(article['pageviews_7d']),
                'ad_impressions_7d': int(article['ad_impressions_7d']),
                'revenue_7d': float(article['revenue_7d']),
                'ipp_7d': float(article['ipp_7d']),
                'pageviews_lifetime': pageviews_lifetime,
                'pps_lifetime': pps_lifetime,
                'revenue_lifetime': float(article['revenue_publish_date']) if pd.notna(article['revenue_publish_date']) else 0.0,
                'rpm_lifetime': float(article['rpm_lifetime']) if pd.notna(article['rpm_lifetime']) else 0.0,
            }
            property_data['top_articles'].append(article_data)
        
        properties_data.append(property_data)
    
    return {
        'report_date': report_date_str,
        'properties': properties_data
    }


def render_plain_text_report(context: Dict[str, Any]) -> str:
    """
    Render the multi-property report as plain text.
    """
    lines = []
    lines.append("=" * 80)
    lines.append(f"WEEKLY CONTENT ANALYTICS REPORT")
    lines.append(f"Report Period: {context['report_date']}")
    lines.append("=" * 80)
    lines.append("")
    
    for prop in context['properties']:
        lines.append("-" * 80)
        lines.append(f"Brand: {prop['display_name']}")
        lines.append("-" * 80)
        lines.append("")
        
        summary = prop['summary_7d']
        lines.append("7-Day Summary:")
        lines.append(f"  Total Pageviews:        {summary['total_pageviews']:,} (statera)")
        lines.append(f"  Total Sessions:         {summary['total_sessions']:,}")
        lines.append(f"  Total Revenue:          ${summary['total_revenue']:,.0f}")
        lines.append(f"  Total Ad Impressions:   {summary['total_ad_impressions']:,}")
        lines.append(f"  Ad Impressions > $0:    {summary['ad_impressions_gt_0']:,}")
        lines.append(f"  PPS (PVs/session):      {summary['pps']:.2f}")
        lines.append(f"  IPP (Impr/Pageview):    {summary['ipp']:.1f}")
        lines.append(f"  RPS (Revenue/Session):  ${summary['rps']:.4f}")
        lines.append("")
        
        if prop['top_articles']:
            lines.append("Top 10 Articles by Pageviews:")
            lines.append("")
            for article in prop['top_articles']:
                lines.append(f"  #{article['rank']}. {article['title']}")
                if article['permalink']:
                    lines.append(f"      URL: {article['permalink']}")
                lines.append(f"      Days Since Publish: {article['days_since_publish']} (WP_publish_date: {article['published_date']})")
                lines.append(f"      7-Day Pageviews:     {article['pageviews_7d']:,} (statera)")
                lines.append(f"      7-Day Revenue:       ${article['revenue_7d']:,.0f}")
                lines.append(f"      7-Day Ad Impr:       {article['ad_impressions_7d']:,}")
                lines.append(f"      7-Day IPP:           {article['ipp_7d']:.1f}")
                # Display Lifetime PPS from per-article query calculation
                if article['pps_lifetime'] is not None:
                    lines.append(f"      Lifetime PPS (PVs/session): {article['pps_lifetime']:.2f}")
                else:
                    lines.append(f"      Lifetime PPS (PVs/session): NA")
                lines.append(f"      Lifetime Revenue:    ${article['revenue_lifetime']:,.0f}")
                lines.append(f"      Lifetime RPM (revenue/1k-PVs): ${article['rpm_lifetime']:.1f}")
                lines.append("")
        else:
            lines.append("  No top articles data available")
            lines.append("")
        
        lines.append("")
    
    lines.append("=" * 80)
    return "\n".join(lines)


def send_plain_email(
    subject: str,
    body: str,
    from_addr: str,
    to_addrs: List[str],
    smtp_host: str,
    smtp_port: int,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
    max_retries: int = 3,
) -> bool:
    """Send a plain‑text email via SMTP with retry logic."""
    msg = MIMEText(body, 'plain')
    msg['Subject'] = subject
    msg['From'] = from_addr
    
    # Filter and clean recipient addresses
    clean_to_addrs = [addr.strip() for addr in to_addrs if addr and addr.strip()]
    if not clean_to_addrs:
        logger.error("No valid recipient addresses provided")
        return False
    
    # Use BCC for all recipients - set To header to generic value
    msg['To'] = "Content Analytics Report"
    logger.info(f"Email recipients count: {len(clean_to_addrs)}")
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries}: Connecting to SMTP server at {smtp_host}:{smtp_port}")
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.ehlo()
                if smtp_user and smtp_password:
                    logger.info("Starting TLS and authenticating")
                    server.starttls()
                    server.login(smtp_user, smtp_password)
                logger.info("Sending email")
                server.sendmail(from_addr, clean_to_addrs, msg.as_string())
                logger.info("Email sent successfully")
                return True
        except (ConnectionRefusedError, socket.gaierror) as e:
            logger.error(f"Connection error on attempt {attempt}: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error on attempt {attempt}: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
    
    logger.error(f"Failed to send email after {max_retries} attempts")
    return False


def main() -> None:
    """Entry point for generating and sending the analytics email."""
    logger.info("Starting multi-property analytics email report generation")
    
    # Parse properties from environment
    properties_str = os.getenv('PROPERTIES', 'torontolife')
    properties = [p.strip() for p in properties_str.split(',') if p.strip()]
    logger.info(f"Properties to report on: {properties}")
    
    # Calculate report period (previous Monday-Sunday)
    tz_offset_hours = -5  # America/Toronto (EST)
    today_utc = datetime.now(timezone.utc)
    today_local = (today_utc + timedelta(hours=tz_offset_hours)).date()
    
    # Determine the reporting week (previous Monday–Sunday)
    days_since_monday = today_local.weekday()
    last_monday = today_local - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    # Convert to datetime for queries
    start_ts = datetime.combine(last_monday, datetime.min.time())
    end_ts = datetime.combine(last_sunday + timedelta(days=1), datetime.min.time())
    
    logger.info(f"Report period: {last_monday} to {last_sunday}")
    
    # Configuration
    ch_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse.statera.internal')
    ch_user = os.getenv('CLICKHOUSE_USER', 'data-scientist')
    ch_password = os.getenv('CLICKHOUSE_PASSWORD', '')
    
    # Run multi-property queries
    summary_df = run_clickhouse_query_multi_property(
        ch_host, ch_user, ch_password, properties, start_ts, end_ts
    )
    
    top_articles_df = run_top_articles_query_multi(
        ch_host, ch_user, ch_password, properties, start_ts, end_ts
    )
    
    if summary_df.empty:
        logger.error("No summary data retrieved from ClickHouse. Cannot generate report.")
        return
    
    # Debug: Log column names to troubleshoot
    logger.info(f"Summary DataFrame columns: {summary_df.columns.tolist()}")
    if not top_articles_df.empty:
        logger.info(f"Top Articles DataFrame columns: {top_articles_df.columns.tolist()}")
    
    # Build report context
    context = build_report_context(properties, summary_df, top_articles_df, last_monday, last_sunday)
    
    logger.info(f"Report context built for {len(context['properties'])} properties")
    
    # Render plain-text report
    plain_text_body = render_plain_text_report(context)
    
    # Check if email sending is enabled
    send_email_enabled = os.getenv('SEND_EMAIL', 'false').lower() == 'true'
    
    if not send_email_enabled:
        logger.info("Email sending is disabled (SEND_EMAIL=false)")
        logger.info("Report preview:\n" + plain_text_body)
        logger.info("To enable email sending, set SEND_EMAIL=true in your environment")
        return
    
    # Validate SMTP configuration
    from_addr = os.getenv('EMAIL_FROM', 'angad.gadre@stjoseph.com')
    to_addrs_str = os.getenv('EMAIL_TO', 'angad.gadre@stjoseph.com')
    smtp_host = os.getenv('SMTP_HOST', 'localhost')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('EMAIL_USER')
    smtp_password = os.getenv('EMAIL_PASSWORD')
    
    # Parse comma-separated recipient list
    to_addrs = [addr.strip() for addr in to_addrs_str.split(',') if addr.strip()]
    
    if not to_addrs:
        logger.error("No valid recipient email addresses configured (EMAIL_TO)")
        logger.info("Report preview:\n" + plain_text_body)
        return
    
    logger.info(f"SMTP configuration: host={smtp_host}, port={smtp_port}, user={smtp_user or 'None'}")
    
    # Build subject line - format as "Weekly Content Analytics Report (December 01-07, 2025)"
    # Extract start and end dates from context
    start_str = last_monday.strftime('%B %d')
    end_str = last_sunday.strftime('%d, %Y')
    subject = f"Weekly Content Analytics Report ({start_str}-{end_str})"
    
    # Send plain-text email with retry logic
    success = send_plain_email(
        subject, plain_text_body, from_addr, to_addrs,
        smtp_host, smtp_port, smtp_user, smtp_password
    )
    
    if not success:
        logger.error("Email sending failed after all retry attempts")
        logger.info("Report preview:\n" + plain_text_body)
    else:
        logger.info("Weekly analytics report sent successfully")


if __name__ == '__main__':
    main()
