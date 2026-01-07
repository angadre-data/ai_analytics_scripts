# Analytics Email Report - Multi-Property Version

## Overview

This script generates weekly analytics reports for multiple properties (brands/sites) by querying ClickHouse and optionally BigQuery. It provides comprehensive 7-day metrics and top-10 article breakdowns per property.

## Features

- **Multi-Property Support**: Report on multiple properties in a single run
- **7-Day Metrics Per Property**:
  - Total Pageviews
  - Total Sessions
  - Total Revenue
  - Total Ad Impressions
  - Ad Impressions > $0
  - IPP (Impressions Per Pageview)
  - RPS (Revenue Per Session)
- **Top-10 Articles Per Property**:
  - 7-Day Pageviews
  - 7-Day Revenue
  - 7-Day Ad Impressions
  - 7-Day IPP
  - Lifetime Revenue
  - Lifetime RPM (Revenue Per 1000 Pageviews)
- **Plain-Text Email Format**: Clean, readable reports
- **Robust Error Handling**: Retry logic for SMTP, graceful handling of empty data
- **Dry-Run Mode**: Preview reports without sending emails

## Configuration

### Environment Variables

Required variables (set in `.env` file or environment):

```bash
# ClickHouse Configuration
CLICKHOUSE_HOST=clickhouse.statera.internal
CLICKHOUSE_USER=data-scientist
CLICKHOUSE_PASSWORD=your_password_here

# Properties to Report On (comma-separated)
PROPERTIES=torontolife,todaysparent,chatelaine,macleans,fashion,chatelaine_fr

# SMTP Configuration
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
EMAIL_USER=your_email@domain.com
EMAIL_PASSWORD=your_app_password_here
EMAIL_FROM=your_email@domain.com
EMAIL_TO=recipient1@domain.com,recipient2@domain.com

# Email Control
SEND_EMAIL=false  # Set to 'true' to enable email sending
```

### Gmail SMTP Setup

If using Gmail for SMTP:

1. Enable 2-Factor Authentication on your Google Account
2. Generate an App Password:
   - Go to Google Account > Security > App passwords
   - Select "Mail" and your device
   - Copy the generated 16-character password
3. Use the app password in `EMAIL_PASSWORD` (not your regular Gmail password)

## Usage

### Dry-Run Mode (Recommended for Testing)

```bash
# Preview report without sending email
SEND_EMAIL=false python3 Scripts/analytics_email_report.py
```

### Single Property

```bash
# Report for one property
PROPERTIES=torontolife SEND_EMAIL=false python3 Scripts/analytics_email_report.py
```

### Multiple Properties

```bash
# Report for multiple properties
PROPERTIES=torontolife,todaysparent,chatelaine SEND_EMAIL=true python3 Scripts/analytics_email_report.py
```

### Production Run

```bash
# Full report with email sending enabled
SEND_EMAIL=true python3 Scripts/analytics_email_report.py
```

## Report Schedule

The script is designed to run every Monday morning and reports on the previous Monday-Sunday week.

- **Report Period**: Previous Monday to Sunday (7 days)
- **Recommended Schedule**: Monday mornings (e.g., via cron)

### Cron Example

```cron
# Run every Monday at 8:00 AM
0 8 * * 1 cd /path/to/Data_Apps && /usr/bin/python3 Scripts/analytics_email_report.py
```

## Report Format

### Email Structure

```
================================================================================
WEEKLY ANALYTICS REPORT
Report Period: November 17 - November 23, 2025
================================================================================

--------------------------------------------------------------------------------
PROPERTY: TORONTOLIFE
--------------------------------------------------------------------------------

7-Day Summary:
  Total Pageviews:        1,234,567
  Total Sessions:         567,890
  Total Revenue:          $12,345.67
  Total Ad Impressions:   2,345,678
  Ad Impressions > $0:    1,987,654
  IPP (Impr/Pageview):    1.9012
  RPS (Revenue/Session):  $0.0217

Top 10 Articles by Pageviews:

  #1. Article Title Here
      URL: https://www.torontolife.com/article-url
      7-Day Pageviews:     45,678
      7-Day Revenue:       $987.65
      7-Day Ad Impr:       98,765
      7-Day IPP:           2.1623
      Lifetime Revenue:    $2,345.67
      Lifetime RPM:        $51.34

  [... continues for top 10 articles ...]

--------------------------------------------------------------------------------
PROPERTY: TODAYSPARENT
--------------------------------------------------------------------------------
[... similar format for each property ...]
```

## Queries

The script uses two main ClickHouse queries:

### 1. Per-Property 7-Day Summary

Groups metrics by property_name for the reporting period:
- Pageviews, sessions, revenue
- Ad impressions (total and > $0)
- IPP and RPS calculations
- New articles published count

### 2. Top-10 Articles Per Property

Ranks articles by 7-day pageviews within each property:
- 7-day metrics (pageviews, revenue, ad impressions, IPP)
- Lifetime metrics (revenue, RPM)
- Article metadata (title, permalink)

## Troubleshooting

### Query Performance

If queries timeout:
- The ClickHouse queries are complex and may take 30-60 seconds on large datasets
- Consider adding indexes on commonly queried fields
- Run during off-peak hours

### SMTP Connection Errors

If email sending fails:
- Verify `SMTP_HOST` and `SMTP_PORT` are correct
- For Gmail, ensure you're using an App Password (not your regular password)
- Check firewall/network restrictions
- Review logs for specific error messages

### Empty Results

If no data is returned:
- Verify property names match exactly (case-sensitive)
- Check date range alignment (script reports on previous Monday-Sunday)
- Ensure data exists in ClickHouse for the specified period
- Review ClickHouse logs for query errors

### Testing

Always test with dry-run mode first:

```bash
SEND_EMAIL=false PROPERTIES=torontolife python3 Scripts/analytics_email_report.py
```

## Future Enhancements

Planned improvements:
- HTML email templates with charts/graphs
- Move SQL queries to external .sql files
- Add trend analysis (week-over-week comparisons)
- Support for custom date ranges
- Dashboard integration
- Export to CSV/PDF

## Support

For issues or questions:
- Check logs for detailed error messages
- Verify environment variable configuration
- Test with a single property first before running multi-property reports
