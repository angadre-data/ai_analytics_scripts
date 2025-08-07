import os
import sys
from datetime import datetime
import pandas as pd
from clickhouse_connect import get_client
from git import Repo

# ---------- CONFIG ----------
# Use environment variables for secrets and flexibility
# CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_HOST = 'clickhouse.statera.internal'
# CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_USER = 'data-scientist'
# CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_PASSWORD = 'l4lnZGx9VJcdoIrN'

# OUTPUT_DIR = os.getenv('OUTPUT_DIR', './output')
OUTPUT_DIR = '/Users/angad.gadre/Documents/Code repo/Data_Apps/Data/'
# GZIP_FILENAME = os.getenv('GZIP_FILENAME', 'data_export.csv.gz')
GZIP_FILENAME = 'adPerformanceData.csv.gzip'
RUN_DATE = datetime.now().strftime('%Y-%m-%d')
CSV_FILENAME = f'content_roi_p14d_{RUN_DATE}.csv'
# REPO_DIR = os.getenv('REPO_DIR', '/path/to/your/local/git/repo')   # <-- Set this!
REPO_DIR = '/Users/angad.gadre/Documents/Code repo/content-roi-insight-hub/'
# GIT_BRANCH = os.getenv('GIT_BRANCH', 'main')
GIT_BRANCH = 'main'

# SQL_QUERY = os.getenv('SQL_QUERY', 'SELECT * FROM your_table')      # <-- Set this!
SQL_QUERY = '''
-- 1. Extract post metadata in one place
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


# ---------- FUNCTIONS ----------
def run_query(query):
    client = get_client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    # Execute query and fetch data into a pandas DataFrame
    query_result = client.query(query)
    # Get the rows of the query result
    rows = query_result.result_rows
    # Get the column names of the query result
    columns = query_result.column_names
    # Convert to a pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)
    return df

# test = run_query(SQL_QUERY)

def get_table_name(query):
    # Simple parser: expects 'SELECT ... FROM table_name'
    try:
        return query.lower().split('from')[1].split()[0]
    except Exception:
        raise ValueError("Could not parse table name from query.")

def basic_quality_checks(df):
    if df.empty:
        raise ValueError("DataFrame is empty!")
    if df.isnull().sum().sum() > 0:
        raise ValueError("Null values found in DataFrame!")
    # Add more custom checks as needed

# basic_quality_checks(test)

def save_to_csv_gzip(df, output_dir, repo_dir, csv_filename, gzip_filename):
    # os.makedirs(output_dir, exist_ok=True)
    file_path_csv = os.path.join(output_dir, csv_filename)
    df.to_csv(file_path_csv, index=False)
    file_path_gzip = os.path.join(output_dir, gzip_filename)
    df.to_csv(file_path_gzip, index=False, compression='gzip')
    # Move the gzip file to the output directory
    file_path_repo = os.path.join(repo_dir, 'public/sampleData/', gzip_filename)
    df.to_csv(file_path_repo, index=False, compression='gzip')

    return file_path_gzip

# save_to_csv_gzip(test, OUTPUT_DIR, REPO_DIR, CSV_FILENAME, GZIP_FILENAME)

# def commit_and_push(file_path, repo_dir, branch="main", commit_prefix="Automated data update"):
#     file_path_repo = os.path.join(repo_dir, 'public/sampleData/',file_path)
#     assert os.path.exists(file_path_repo), f"File {file_path_repo} does not exist"
#     assert os.path.exists(os.path.join(repo_dir, ".git")), f"{repo_dir} is not a git repo"
#     repo = Repo(repo_dir)
#     origin = repo.remote(name='origin')
#     repo.git.fetch()
#     repo.git.reset('--hard', f'origin/{branch}')
#     # Now proceed with add/commit/push
#     rel_path = os.path.relpath(file_path_repo, repo_dir)
#     repo.git.add(rel_path)
#     commit_message = f"{commit_prefix}: {os.path.basename(file_path_repo)} ({datetime.now():%Y-%m-%d %H:%M})"
#     repo.index.commit(commit_message)
#     # --- PULL before PUSH to avoid non-fast-forward errors ---
#     origin.pull(branch)
#     origin.push(branch, force=True)
#     print(f"Pushed {file_path} to {branch} on remote repo.")

def sync_and_reset_repo(repo_dir, branch="main"):
    repo = Repo(repo_dir)
    origin = repo.remote(name='origin')
    repo.git.fetch()
    repo.git.reset('--hard', f'origin/{branch}')
    print("Repo synced to remote.")

def commit_and_push(file_path, repo_dir, branch="main", commit_prefix="Automated data update"):
    file_path_repo = os.path.join(repo_dir, 'public/sampleData/', file_path)
    assert os.path.exists(file_path_repo), f"File {file_path_repo} does not exist"
    repo = Repo(repo_dir)
    rel_path = os.path.relpath(file_path_repo, repo_dir)
    repo.git.add(rel_path)
    commit_message = f"{commit_prefix}: {os.path.basename(file_path_repo)} ({datetime.now():%Y-%m-%d %H:%M})"
    repo.index.commit(commit_message)
    repo.remote(name='origin').push(branch, force=True)
    print(f"Pushed {file_path} to {branch} on remote repo.")

def main():
    try:
        print("Syncing repo with remote...")
        sync_and_reset_repo(REPO_DIR, GIT_BRANCH)
        
        print("Running ClickHouse query...")
        df = run_query(SQL_QUERY)
        print(f"Query successful: {len(df)} rows.")

        print("Saving dataframe as gzip CSV...")
        file_path = save_to_csv_gzip(df, OUTPUT_DIR, REPO_DIR, CSV_FILENAME, GZIP_FILENAME)
        print(f"Data saved to: {file_path}")

        print("Committing and pushing to Git...")
        commit_and_push(GZIP_FILENAME, REPO_DIR, branch=GIT_BRANCH)
        print("All done!")

    except Exception as e:
        print(f"Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
    # 0 6 * * * /usr/bin/python3 "/Users/angad.gadre/Documents/Code repo/Data_Apps/Scripts/roi_script.py" >> /Users/angad.gadre/Documents/Code\ repo/Data_Apps/Data/logs/pipeline_cron.log 2>&1