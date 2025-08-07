# Using the clickhouse_connection.py code, write a function to fetch daily aggregated data from ClickHouse and save it to a CSV file. The function should take the date as an argument and use it to filter the data in the SQL query. The function should also handle any exceptions that may occur during the database connection or data retrieval process. Finally, the function should return a success message if the data is saved successfully, or an error message if an exception occurs.
import pandas as pd
import numpy as np
from clickhouse_connect import get_client
import os
from datetime import datetime
import logging
import gzip
import shutil

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ClickHouse connection credentials
host = 'clickhouse.statera.internal'
port = 8123
username = 'data-scientist'
password = 'l4lnZGx9VJcdoIrN'


sql_query = '''-- 1. Extract post metadata in one place
WITH published_meta AS (
  SELECT
    property_name,
    wp_post_id,
    dictGetOrNull(
      'assembly.wordpress_metadata',
      'PostType',
      (property_name, wp_post_id)
    ) AS post_type,
    toDateTime(
      dictGetOrNull(
        'assembly.wordpress_metadata',
        'PublishedAt',
        (property_name, wp_post_id)
      )
    ) AS wp_publish_date,
    dictGetOrNull(
      'assembly.wordpress_metadata',
      'AuthorDisplayName',
      (property_name, wp_post_id)
    ) AS author_display_name,
    dictGetOrNull(
      'assembly.wordpress_metadata',
      'Title',
      (property_name, wp_post_id)
    ) AS article_title,
    toUInt32(
      dictGetOrNull(
        'assembly.wordpress_metadata',
        'LexemeCount',
        (property_name, wp_post_id)
      )
    ) AS lexeme_count,
    toFloat32(
      dictGetOrNull(
        'assembly.wordpress_metadata',
        'Sentiment',
        (property_name, wp_post_id)
      )
    ) AS sentiment,
    dictGetOrNull(
      'assembly.wordpress_metadata',
      'Permalink',
      (property_name, wp_post_id)
    ) AS permalink
  FROM
    (SELECT DISTINCT property_name, wp_post_id
     FROM assembly.client_events
     WHERE wp_post_id != 0
       AND property_name IN (
         'todaysparent','torontolife','chatelaine',
         'macleans','fashion','chatelaine_fr'
       )
    )
),

-- 2. Join events to metadata, and clean the permalink
events AS (
  SELECT
    ev.timestamp,
    formatDateTime(ev.timestamp, '%Y-%m-%d') AS dt_event,
    ev.property_name,
    ev.wp_post_id,
    ev.type,
    ev.revenue,
    ev.user_id,
    ev.page_view_id,
    ev.session_id,
    pm.wp_publish_date,
    pm.author_display_name,
    pm.article_title,
    pm.lexeme_count,
    pm.sentiment,
    pm.permalink,
    -- Strip any duplicated "https://https://" prefix
    replace(pm.permalink, 'https://https://', 'https://') AS cleaned_url,
    if(
  empty( splitByString('/', coalesce(cleaned_url, ''))[4] ),
  'homepage',
  splitByString('/', coalesce(cleaned_url, ''))[4]
) AS website_category
  FROM assembly.client_events AS ev
  LEFT JOIN published_meta AS pm
    ON ev.property_name = pm.property_name
   AND ev.wp_post_id      = pm.wp_post_id
  WHERE ev.timestamp >= now() - INTERVAL 14 DAY
    AND ev.property_name IN (
      'todaysparent','torontolife','chatelaine',
      'macleans','fashion','chatelaine_fr'
    )
    and post_type NOT IN ('page', 'hub-page', 'sjh_grid', 'contest', 'content_module')
    AND ev.is_bot = 0
    AND ev.wp_post_id != 0
    AND ev.url != ''
)

-- 3. Final aggregation by property, post, date
SELECT
  property_name,
  wp_post_id,
  dt_event,
  -- Expose the cleaned URL
  MAX(cleaned_url)                  AS cleaned_url,
  max(website_category)            AS website_category,
  -- Metadata fields
  MAX(wp_publish_date)              AS wp_publish_date,
  MAX(author_display_name)          AS author_display_name,
  MAX(article_title)                AS article_title,
  MAX(lexeme_count)                 AS lexeme_count,
  MAX(sentiment)                    AS sentiment,
  MAX(permalink)                    AS raw_permalink,
  -- Blended eCPM lookup
  CASE
    WHEN property_name = 'canadianbusiness' THEN 9.98
    WHEN property_name = 'chatelaine'       THEN 5.18
    WHEN property_name = 'chatelaine_fr'    THEN 3.57
    WHEN property_name = 'fashion'          THEN 3.86
    WHEN property_name = 'macleans'         THEN 4.48
    WHEN property_name = 'torontolife'      THEN 4.96
    WHEN property_name = 'todaysparent'     THEN 3.74
    ELSE 0
  END AS blended_eCPM,
  -- Revenue at blended rate
  (COUNTIf(type = 'ad_impression' AND revenue > 0) * blended_eCPM) / 1000
    AS total_revenue_blended_ecpm,
  -- Core metrics
  COUNTIf(type = 'ad_impression')                           AS total_ad_impressions,
  COUNTIf(type = 'ad_impression' AND revenue > 0)           AS ad_impressions_with_revenue,
  SUMIf(revenue, type IN ('ad_impression','ad_click'))      AS total_revenue,
  COUNT(DISTINCT user_id)                                   AS total_users,
  COUNT(DISTINCT page_view_id)                              AS total_pageviews,
  COUNT(DISTINCT session_id)                                AS total_sessions,
  COUNTIf(type = 'ad_impression' AND revenue > 0) /
    NULLIF(COUNT(DISTINCT page_view_id), 0)                  AS monetizable_imp_per_pv,
  COUNT(DISTINCT page_view_id) /
    NULLIF(COUNT(DISTINCT session_id), 0)                    AS pageviews_per_visit

FROM events
GROUP BY
  property_name,
  wp_post_id,
  dt_event
ORDER BY
  dt_event DESC,
  property_name,
  wp_post_id
  '''

# Connect to ClickHouse
client = get_client(host=host, port=port, username=username, password=password)

# Execute query and fetch data into a pandas DataFrame
query_result = client.query(sql_query)
# Get the rows of the query result
rows = query_result.result_rows

# Get the column names of the query result
columns = query_result.column_names

# Convert to a pandas DataFrame
df = pd.DataFrame(rows, columns=columns)

# assert that the DataFrame is not empty
assert not df.empty, "The DataFrame is empty. No data was fetched from ClickHouse."

# assert that the DataFrame has the expected columns
expected_columns = [
    'property_name', 'wp_post_id', 'dt_event', 'cleaned_url',
    'website_category', 'wp_publish_date', 'author_display_name',
    'article_title', 'lexeme_count', 'sentiment', 'raw_permalink',
    'blended_eCPM', 'total_revenue_blended_ecpm', 'total_ad_impressions',
    'ad_impressions_with_revenue', 'total_revenue', 'total_users',
    'total_pageviews', 'total_sessions', 'monetizable_imp_per_pv',
    'pageviews_per_visit'
]
assert all(col in df.columns for col in expected_columns), "The DataFrame does not have the expected columns."

# assert that the value counts of 'property_name' are in the expected range
assert df['property_name'].value_counts().min() >= 1000, "Some property names have < 1k rows. Check the data."


# Save the DataFrame to a CSV file named content_roi_p7d_{today_date}.csv without a function
# Get today's date in YYYY-MM-DD format
today_date = datetime.now().strftime('%Y-%m-%d')
# Define the file path
file_path = f'content_roi_p14d_{today_date}.csv'
# Save the DataFrame to a CSV file
df.to_csv(file_path, index=False)

# find the max publish date in the DataFrame and assert if it is the same as today
assert df['wp_publish_date'].max().date() == datetime.now().date(), "Max publish date is not today"

# convert the file saved above to a gzip file, and save the original CSV and the new gzip file in the same directory: /Users/angad.gadre/Documents/Code repo/Data_Apps/Data/
output_directory = '/Users/angad.gadre/Documents/Code repo/Data_Apps/Data/'
# Save the DataFrame to a CSV file in the specified directory
output_file_path = os.path.join(output_directory, file_path)
df.to_csv(output_file_path, index=False)

# Compress the CSV file to gzip format
with open('/Users/angad.gadre/Documents/Code repo/Data_Apps/Data/'+file_path, 'rb') as f_in:
    with gzip.open('/Users/angad.gadre/Documents/Code repo/Data_Apps/Data/adPerformanceData.csv.gzip', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

