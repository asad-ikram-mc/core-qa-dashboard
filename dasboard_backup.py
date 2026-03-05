"""
CORE Pipeline — QA Dashboard v3
=================================
Two data sources:
  1. Lambda 7 S3 backups (finalsql_*.csv) → true WoW snapshot analysis
  2. Live DB (sprinklr_table) → data quality & coverage checks

Usage:
    pip install boto3 pandas psycopg2-binary
    python wow_dashboard_v3.py

Output:
    index.html
"""

import boto3
import psycopg2
import pandas as pd
import json
import os
from io import BytesIO
from datetime import datetime

# ── Config ──
AWS_PROFILE = os.environ.get('AWS_PROFILE', 'asad')
S3_BUCKET = 'wikitablescrapexample'
BACKUP_PREFIX = 'check/'

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'prod-fluency-sprinklr-db-eu2.cpmfagf94hdi.eu-west-2.rds.amazonaws.com'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'dbname': os.environ.get('DB_NAME', 'sprinklrproddb'),
    'user': os.environ.get('DB_USER', 'tableausprinklr'),
    'password': os.environ.get('DB_PASSWORD', 'HiXy074Hi')
}

# ═══════════════════════════════════════════════════════════════════════════
# DATA SOURCE 1: Lambda 7 S3 Backups → WoW
# ═══════════════════════════════════════════════════════════════════════════

def list_backup_files(s3):
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=BACKUP_PREFIX):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            key = obj['Key']
            if key.startswith('check/finalsql_') and key.endswith('.csv'):
                try:
                    date_str = key.replace('check/finalsql_', '').replace('.csv', '')
                    file_date = datetime.strptime(date_str, '%Y-%m-%d')
                    if file_date >= datetime(2025, 1, 1):
                        files.append((file_date, key))
                except:
                    continue
    files.sort()
    return files


def load_and_aggregate(s3, file_date, file_key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=file_key)
    df = pd.read_csv(BytesIO(obj['Body'].read()), low_memory=False)

    if 'Benchmark Type' in df.columns:
        df = df[df['Benchmark Type'] == 'Median']
    if 'Delivery' in df.columns:
        df = df[df['Delivery'].isin(['Organic', 'Boosted'])]
    if 'Total Impressions' not in df.columns:
        return pd.DataFrame()

    df['Total Impressions'] = pd.to_numeric(df['Total Impressions'], errors='coerce')
    df = df[df['Total Impressions'].notna()]

    if 'Country' not in df.columns or 'Platform Granular' not in df.columns:
        return pd.DataFrame()

    agg = df.groupby(['Country', 'Platform Granular']).agg(
        post_count=('Total Impressions', 'count'),
        total_impressions=('Total Impressions', 'sum'),
        avg_impressions_per_post=('Total Impressions', 'mean')
    ).reset_index()
    agg['avg_impressions_per_post'] = agg['avg_impressions_per_post'].round(2)
    agg['week_ending'] = file_date.strftime('%Y-%m-%d')
    return agg


def build_wow_dataset():
    print("── Lambda 7 Backups (S3) ──")
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client('s3')
    backup_files = list_backup_files(s3)

    if not backup_files:
        print("  ❌ No backup files found")
        return pd.DataFrame()

    print(f"  Found {len(backup_files)} backups: {backup_files[0][0].strftime('%Y-%m-%d')} → {backup_files[-1][0].strftime('%Y-%m-%d')}")

    all_snapshots = []
    for i, (file_date, file_key) in enumerate(backup_files, 1):
        date_str = file_date.strftime('%Y-%m-%d')
        print(f"  [{i}/{len(backup_files)}] {date_str}...", end=" ")
        try:
            agg = load_and_aggregate(s3, file_date, file_key)
            if len(agg) > 0:
                all_snapshots.append(agg)
                print(f"✅ {len(agg)} rows")
            else:
                print("⚠️  Empty")
        except Exception as e:
            print(f"❌ {e}")

    if not all_snapshots:
        return pd.DataFrame()

    df = pd.concat(all_snapshots, ignore_index=True)
    df = df.sort_values(['Country', 'Platform Granular', 'week_ending']).reset_index(drop=True)

    g = ['Country', 'Platform Granular']
    df['prev_post_count'] = df.groupby(g)['post_count'].shift(1)
    df['prev_impressions'] = df.groupby(g)['total_impressions'].shift(1)
    df['post_count_change'] = df['post_count'] - df['prev_post_count']
    df['impressions_change'] = df['total_impressions'] - df['prev_impressions']
    df['impressions_pct_change'] = (
        (df['impressions_change'] / df['prev_impressions'].replace(0, float('nan'))) * 100
    ).round(2)

    result = df[[
        'week_ending', 'Country', 'Platform Granular',
        'post_count', 'total_impressions', 'avg_impressions_per_post',
        'prev_post_count', 'prev_impressions',
        'post_count_change', 'impressions_change', 'impressions_pct_change'
    ]].copy()

    result = result.sort_values(
        ['Country', 'Platform Granular', 'week_ending'],
        ascending=[True, True, False]
    ).reset_index(drop=True)

    print(f"  ✅ WoW dataset: {len(result):,} rows, {result['week_ending'].nunique()} snapshots")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# DATA SOURCE 2: Live DB → Data Quality
# ═══════════════════════════════════════════════════════════════════════════

QUERY_QUALITY = """
SELECT
    DATE_TRUNC('week', "Date"::date) + INTERVAL '6 days' AS week_ending,
    "Country",
    "Platform Granular",
    "Delivery",
    COUNT(*) AS total_rows,
    SUM(CASE WHEN "Total Impressions" IS NULL THEN 1 ELSE 0 END) AS null_impressions,
    SUM(CASE WHEN "Total Impressions" IS NOT NULL THEN 1 ELSE 0 END) AS has_impressions,
    SUM("Total Impressions") AS sum_impressions,
    SUM(CASE WHEN "Country" IS NULL OR "Country" = '' THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN "Platform Granular" IS NULL OR "Platform Granular" = '' THEN 1 ELSE 0 END) AS null_platform,
    SUM(CASE WHEN "Delivery" IS NULL OR "Delivery" = '' THEN 1 ELSE 0 END) AS null_delivery,
    SUM(CASE WHEN "Account" IS NULL OR "Account" = '' THEN 1 ELSE 0 END) AS null_account,
    SUM(CASE WHEN "Permalink" IS NULL OR "Permalink" = '' THEN 1 ELSE 0 END) AS null_permalink,
    SUM(CASE WHEN "Benchmark Type" IS NULL OR "Benchmark Type" = '' THEN 1 ELSE 0 END) AS null_benchmark,
    SUM(CASE WHEN "Benchmark Matcher" IS NULL OR "Benchmark Matcher" = '' THEN 1 ELSE 0 END) AS null_matcher,
    SUM(CASE WHEN "Reputational Topic" IS NULL OR "Reputational Topic" = '' THEN 1 ELSE 0 END) AS null_rep_topic,
    SUM(CASE WHEN "Reputational Sub Topic" IS NULL OR "Reputational Sub Topic" = '' THEN 1 ELSE 0 END) AS null_rep_sub,
    SUM(CASE WHEN "Post Message" IS NULL OR "Post Message" = '' THEN 1 ELSE 0 END) AS null_post_msg,
    SUM(CASE WHEN "Campaign" IS NULL OR "Campaign" = '' THEN 1 ELSE 0 END) AS null_campaign,
    SUM(CASE WHEN "Region" IS NULL OR "Region" = '' THEN 1 ELSE 0 END) AS null_region,
    SUM(CASE WHEN "MESSAGE_TYPE" IS NULL OR "MESSAGE_TYPE" = '' THEN 1 ELSE 0 END) AS null_msg_type,
    SUM(CASE WHEN "Post Format" IS NULL OR "Post Format" = '' THEN 1 ELSE 0 END) AS null_post_format,
    SUM(CASE WHEN "Matcher" IS NULL OR "Matcher" = '' THEN 1 ELSE 0 END) AS null_matcher_col,
    SUM(CASE WHEN "Content Category Type" IS NULL OR "Content Category Type" = '' THEN 1 ELSE 0 END) AS null_content_cat,
    SUM(CASE WHEN author_name IS NULL OR author_name = '' THEN 1 ELSE 0 END) AS null_author
FROM sprinklr_table
WHERE "Date" IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 2, 3
"""

QUERY_LINE_COLLAB = """
SELECT
    DATE_TRUNC('week', "Date"::date) + INTERVAL '6 days' AS week_ending,
    "Country",
    "Platform Granular",
    "Account",
    is_manual_tracker,
    CASE WHEN is_manual_tracker = 1 THEN 'LINE' WHEN is_manual_tracker = 2 THEN 'Collab' ELSE 'Other' END AS tracker_type,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN "Total Impressions" IS NULL THEN 1 ELSE 0 END) AS null_impressions,
    SUM(CASE WHEN "Total Impressions" IS NOT NULL THEN 1 ELSE 0 END) AS has_impressions,
    SUM("Total Impressions") AS sum_impressions,
    SUM(CASE WHEN "Permalink" IS NULL OR "Permalink" = '' THEN 1 ELSE 0 END) AS null_permalink,
    SUM(CASE WHEN "Benchmark Type" IS NULL OR "Benchmark Type" = '' THEN 1 ELSE 0 END) AS null_benchmark,
    SUM(CASE WHEN "Benchmark Matcher" IS NULL OR "Benchmark Matcher" = '' THEN 1 ELSE 0 END) AS null_matcher,
    SUM(CASE WHEN "Delivery" IS NULL OR "Delivery" = '' THEN 1 ELSE 0 END) AS null_delivery,
    SUM(CASE WHEN "Post Message" IS NULL OR "Post Message" = '' THEN 1 ELSE 0 END) AS null_post_msg,
    SUM(CASE WHEN "Reputational Topic" IS NULL OR "Reputational Topic" = '' THEN 1 ELSE 0 END) AS null_rep_topic,
    SUM(CASE WHEN "Content Category Type" IS NULL OR "Content Category Type" = '' THEN 1 ELSE 0 END) AS null_content_cat,
    SUM(CASE WHEN author_name IS NULL OR author_name = '' THEN 1 ELSE 0 END) AS null_author,
    SUM(CASE WHEN "Country" IS NULL OR "Country" = '' THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN "Matcher" IS NULL OR "Matcher" = '' THEN 1 ELSE 0 END) AS null_matcher_col,
    SUM(CASE WHEN what_kind_of_collab_post IS NULL OR what_kind_of_collab_post = '' THEN 1 ELSE 0 END) AS null_collab_kind,
    SUM(CASE WHEN "is_this_a_collab_post?" IS NULL OR "is_this_a_collab_post?" = '' THEN 1 ELSE 0 END) AS null_is_collab,
    SUM(CASE WHEN "Name of the collaborator" IS NULL OR "Name of the collaborator" = '' THEN 1 ELSE 0 END) AS null_collab_name
FROM sprinklr_table
WHERE "Date" IS NOT NULL
  AND is_manual_tracker IN (1, 2)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC, 6, 2, 3
"""


def build_quality_dataset():
    print("\n── Live Database ──")
    print("  Connecting...")
    conn = psycopg2.connect(**DB_CONFIG)

    print("  Running quality query...")
    df = pd.read_sql(QUERY_QUALITY, conn)
    print(f"  ✅ Quality: {len(df):,} rows")

    print("  Running LINE & Collab query...")
    lc_df = pd.read_sql(QUERY_LINE_COLLAB, conn)
    print(f"  ✅ LINE & Collab: {len(lc_df):,} rows")

    conn.close()
    return df, lc_df


# ═══════════════════════════════════════════════════════════════════════════
# HTML Generation
# ═══════════════════════════════════════════════════════════════════════════

def df_to_json(df):
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        elif df[col].dtype == 'object':
            df[col] = df[col].fillna('')
    records = df.to_dict(orient='records')
    for r in records:
        for k, v in r.items():
            try:
                if pd.isna(v):
                    r[k] = None
            except (TypeError, ValueError):
                pass
    return records


def generate_html(wow_df, quality_df, lc_df):
    wow_blob = json.dumps(df_to_json(wow_df), default=str)
    quality_blob = json.dumps(df_to_json(quality_df), default=str)
    lc_blob = json.dumps(df_to_json(lc_df), default=str)
    generated_at = datetime.now().strftime('%Y-%m-%d %H:%M')

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CORE — Pipeline QA Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
:root {{
  --bg:#0b0f19;--surface:#111827;--surface-2:#1a2332;--surface-3:#212d3f;
  --border:#1e293b;--border-light:#2d3d52;
  --text:#e2e8f0;--text-dim:#8492a6;--text-muted:#4a5568;
  --accent:#3b82f6;--accent-glow:rgba(59,130,246,0.12);
  --green:#10b981;--green-bg:rgba(16,185,129,0.08);
  --red:#ef4444;--red-bg:rgba(239,68,68,0.08);
  --amber:#f59e0b;--amber-bg:rgba(245,158,11,0.08);
  --purple:#8b5cf6;--cyan:#06b6d4;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}}
::-webkit-scrollbar{{width:5px;height:5px}}
::-webkit-scrollbar-track{{background:var(--surface)}}
::-webkit-scrollbar-thumb{{background:var(--border-light);border-radius:3px}}

.header{{background:var(--surface);border-bottom:1px solid var(--border);padding:14px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}}
.header-left{{display:flex;align-items:center;gap:14px}}
.logo{{width:34px;height:34px;background:linear-gradient(135deg,var(--accent),var(--purple));border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;color:#fff}}
.header h1{{font-size:16px;font-weight:600;letter-spacing:-0.3px}}
.header h1 span{{color:var(--text-dim);font-weight:400}}
.badge{{font-family:'JetBrains Mono',monospace;font-size:10px;padding:3px 8px;border-radius:4px;background:var(--accent-glow);color:var(--accent);border:1px solid rgba(59,130,246,0.2)}}
.badge-green{{background:var(--green-bg);color:var(--green);border-color:rgba(16,185,129,0.2)}}
.badge-purple{{background:rgba(139,92,246,0.08);color:var(--purple);border-color:rgba(139,92,246,0.2)}}

.main{{padding:20px 28px;max-width:1600px;margin:0 auto}}

/* Tabs */
.tab-bar{{display:flex;gap:0;margin-bottom:20px;border-bottom:2px solid var(--border)}}
.tab{{padding:10px 24px;font-size:13px;font-weight:600;color:var(--text-dim);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:all 0.15s}}
.tab:hover{{color:var(--text)}}
.tab.active{{color:var(--accent);border-bottom-color:var(--accent)}}
.tab-content{{display:none}}.tab-content.active{{display:block}}

/* Filters */
.filter-bar{{display:flex;align-items:center;gap:10px;margin-bottom:20px;flex-wrap:wrap;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px 16px}}
.filter-bar label{{font-size:11px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px}}
select{{font-family:'DM Sans',sans-serif;background:var(--surface-2);color:var(--text);border:1px solid var(--border);padding:6px 10px;border-radius:5px;font-size:12px;outline:none;cursor:pointer}}
select:focus{{border-color:var(--accent)}}
.btn{{font-family:'DM Sans',sans-serif;padding:6px 14px;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer;border:none;transition:all 0.15s}}
.btn-primary{{background:var(--accent);color:#fff}}.btn-primary:hover{{background:#2563eb}}
.btn-ghost{{background:transparent;color:var(--text-dim);border:1px solid var(--border)}}.btn-ghost:hover{{color:var(--text)}}
.filter-sep{{width:1px;height:24px;background:var(--border);margin:0 4px}}

/* KPIs */
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px}}
.kpi{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px}}
.kpi.blue::before{{background:var(--accent)}}.kpi.green::before{{background:var(--green)}}
.kpi.amber::before{{background:var(--amber)}}.kpi.red::before{{background:var(--red)}}
.kpi.purple::before{{background:var(--purple)}}.kpi.cyan::before{{background:var(--cyan)}}
.kpi-label{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin-bottom:6px}}
.kpi-val{{font-size:22px;font-weight:700;letter-spacing:-0.5px;font-feature-settings:'tnum' 1}}
.kpi-sub{{font-size:11px;color:var(--text-dim);margin-top:3px;font-family:'JetBrains Mono',monospace}}
.up{{color:var(--green)}}.down{{color:var(--red)}}.flat{{color:var(--text-muted)}}

/* Charts */
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:20px}}
.grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:20px}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}}
.card.full{{grid-column:1/-1}}
.card h3{{font-size:12px;font-weight:600;margin-bottom:2px}}
.card .sub{{font-size:10px;color:var(--text-dim);margin-bottom:12px}}
.cw{{position:relative;height:260px}}.cw.tall{{height:340px}}.cw.short{{height:200px}}

/* Section headers */
.sh{{font-size:14px;font-weight:600;margin:24px 0 14px;display:flex;align-items:center;gap:8px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.sh .dot{{width:7px;height:7px;border-radius:50%}}

/* Tables */
.tc{{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:20px}}
.ts{{overflow-x:auto;max-height:480px;overflow-y:auto}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead{{position:sticky;top:0;z-index:10}}
th{{background:var(--surface-2);padding:8px 12px;text-align:left;font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-dim);border-bottom:1px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none}}
th:hover{{color:var(--text)}}
td{{padding:8px 12px;border-bottom:1px solid var(--border);font-family:'JetBrains Mono',monospace;font-size:11px;white-space:nowrap}}
tr:hover td{{background:rgba(59,130,246,0.03)}}
td.pos{{color:var(--green)}}td.neg{{color:var(--red)}}
.pill{{display:inline-block;padding:2px 7px;border-radius:3px;font-size:10px;font-weight:600}}
.pill-g{{background:var(--green-bg);color:var(--green)}}
.pill-r{{background:var(--red-bg);color:var(--red)}}
.pill-a{{background:var(--amber-bg);color:var(--amber)}}
.anom-row td{{background:rgba(239,68,68,0.03)}}

/* Coverage heatmap */
.cov-miss{{background:rgba(239,68,68,0.15);color:var(--red);text-align:center;font-weight:600}}
.cov-low{{background:rgba(245,158,11,0.1);color:var(--amber);text-align:center}}
.cov-mid{{background:rgba(59,130,246,0.08);color:var(--accent);text-align:center}}
.cov-high{{background:rgba(16,185,129,0.1);color:var(--green);text-align:center}}
.cov-head{{font-size:9px;min-width:70px;text-align:center}}
.cov-dim{{position:sticky;left:0;background:var(--surface-2);z-index:5;font-family:'DM Sans',sans-serif;font-weight:600;min-width:140px}}
.tick{{color:var(--green);font-weight:700;text-align:center}}.cross{{color:var(--red);font-weight:700;text-align:center}}
.null-ok{{background:rgba(16,185,129,0.06);text-align:center;color:var(--green)}}.null-bad{{background:rgba(239,68,68,0.1);text-align:center;color:var(--red);font-weight:600}}

@media(max-width:900px){{.grid2,.grid3{{grid-template-columns:1fr}}.kpi-row{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <div class="logo">QA</div>
    <h1>CORE Pipeline <span>— QA Dashboard</span></h1>
  </div>
  <div style="display:flex;align-items:center;gap:10px">
    <span class="badge">{generated_at}</span>
    <span class="badge badge-purple">Lambda 7 + DB</span>
  </div>
</div>

<div class="main">
  <!-- Tab Navigation -->
  <div class="tab-bar">
    <div class="tab active" onclick="switchTab('wow')">Week on Week</div>
    <div class="tab" onclick="switchTab('analysis')">WoW Analysis</div>
    <div class="tab" onclick="switchTab('quality')">Data Quality</div>
    <div class="tab" onclick="switchTab('linecollab')">LINE & Collab</div>
  </div>

  <!-- ═══════════════════════════════════════════════════════ -->
  <!-- TAB 1: WEEK ON WEEK (Lambda 7 Snapshots)              -->
  <!-- ═══════════════════════════════════════════════════════ -->
  <div id="tab-wow" class="tab-content active">
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:16px;font-size:12px;line-height:1.7">
      <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:var(--accent)">What is this tab?</div>
      <div style="color:var(--text-dim)">
        <strong style="color:var(--text)">Source:</strong> Lambda 7 S3 backup files (<code style="font-family:JetBrains Mono;font-size:11px;color:var(--amber)">finalsql_YYYY-MM-DD.csv</code>) — each file is a true point-in-time snapshot of what the pipeline produced on that date.<br>
        <strong style="color:var(--text)">What it shows:</strong> How total impressions, post counts, and averages change from one pipeline run to the next. Each "snapshot" is one Lambda 7 run. The WoW change is calculated by comparing the <em>same</em> Country + Platform combo across consecutive snapshots — so if YouTube ES had 19M impressions on Feb 23 and 58M on Mar 1, that's a real +207% change.<br>
        <strong style="color:var(--text)">Columns used:</strong> <code style="font-family:JetBrains Mono;font-size:11px">Country</code>, <code style="font-family:JetBrains Mono;font-size:11px">Platform Granular</code>, <code style="font-family:JetBrains Mono;font-size:11px">Total Impressions</code>, <code style="font-family:JetBrains Mono;font-size:11px">Delivery</code> (filtered to Organic + Boosted), <code style="font-family:JetBrains Mono;font-size:11px">Benchmark Type</code> (filtered to Median only).
      </div>
    </div>

    <div class="filter-bar">
      <label>Country</label><select id="w-co"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Platform</label><select id="w-pl"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Weeks</label>
      <select id="w-wk">
        <option value="1">Last 1 week</option>
        <option value="2">Last 2 weeks</option>
        <option value="3">Last 3 weeks</option>
        <option value="4">Last 4 weeks</option>
        <option value="8" selected>Last 8 weeks</option>
        <option value="12">Last 12 weeks</option>
        <option value="26">Last 6 months</option>
        <option value="52">Last 1 year</option>
        <option value="0">All</option>
      </select>
      <div class="filter-sep"></div>
      <button class="btn btn-primary" onclick="applyWow()">Apply</button>
      <button class="btn btn-ghost" onclick="exportWow()">↓ CSV</button>
    </div>

    <div class="kpi-row" id="w-kpis"></div>

    <div class="grid2">
      <div class="card full"><h3>Total Impressions — Snapshot over Snapshot</h3><div class="sub">Line = total · Bars = change vs previous snapshot</div><div class="cw tall"><canvas id="wc1"></canvas></div></div>
    </div>
    <div class="grid2">
      <div class="card"><h3>Impressions by Platform</h3><div class="sub">Stacked area per snapshot</div><div class="cw tall"><canvas id="wc2"></canvas></div></div>
      <div class="card"><h3>Impressions by Country</h3><div class="sub">Stacked area per snapshot</div><div class="cw tall"><canvas id="wc3"></canvas></div></div>
    </div>
  </div>

  <!-- ═══════════════════════════════════════════════════════ -->
  <!-- TAB 2: WOW ANALYSIS                                    -->
  <!-- ═══════════════════════════════════════════════════════ -->
  <div id="tab-analysis" class="tab-content">
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:16px;font-size:12px;line-height:1.7">
      <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:var(--purple)">What is this tab?</div>
      <div style="color:var(--text-dim)">
        <strong style="color:var(--text)">Source:</strong> Same Lambda 7 S3 snapshots as the WoW tab.<br>
        <strong style="color:var(--text)">What it shows:</strong> Flags <em>anomalies</em> — any Country + Platform combo whose impressions changed by more than the threshold between consecutive snapshots. This tab focuses on <strong>what went wrong (or surprisingly right)</strong> rather than overall trends. It also shows coverage heatmaps to spot gaps — which platforms/countries are missing data in which snapshots.<br>
        <strong style="color:var(--text)">How it differs from WoW tab:</strong> The WoW tab shows trends (total impressions, stacked charts). This tab filters to only the outliers — drops and spikes that exceed your threshold — so you can quickly find what needs attention without scrolling through hundreds of stable rows.
      </div>
    </div>

    <div class="filter-bar">
      <label>Country</label><select id="a-co"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Platform</label><select id="a-pl"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Weeks</label>
      <select id="a-wk">
        <option value="1">Last 1 week</option>
        <option value="2">Last 2 weeks</option>
        <option value="3">Last 3 weeks</option>
        <option value="4">Last 4 weeks</option>
        <option value="8" selected>Last 8 weeks</option>
        <option value="12">Last 12 weeks</option>
        <option value="26">Last 6 months</option>
        <option value="52">Last 1 year</option>
        <option value="0">All</option>
      </select>
      <div class="filter-sep"></div>
      <label>Threshold</label>
      <select id="a-th">
        <option value="20">20%</option>
        <option value="30" selected>30%</option>
        <option value="50">50%</option>
      </select>
      <div class="filter-sep"></div>
      <button class="btn btn-primary" onclick="applyAnalysis()">Apply</button>
    </div>

    <div class="kpi-row" id="a-kpis" style="grid-template-columns:repeat(4,1fr)"></div>

    <div class="grid2">
      <div class="card full"><h3>Anomaly Timeline</h3><div class="sub">Red = drops · Green = spikes · Size = magnitude</div><div class="cw tall"><canvas id="ac1"></canvas></div></div>
    </div>

    <div class="sh"><span class="dot" style="background:var(--red)"></span>Anomaly Table</div>
    <div class="tc"><div class="ts" id="a-tbl"></div></div>

    <div class="sh"><span class="dot" style="background:var(--amber)"></span>Platform Coverage (Lambda 7 Snapshots)</div>
    <div class="sub-note" style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px 15px">Posts per snapshot · colored by impressions · ✗ = missing</div>
    <div class="tc"><div class="ts" id="a-cov-plat" style="max-height:600px"></div></div>

    <div class="sh"><span class="dot" style="background:var(--amber)"></span>Country Coverage (Lambda 7 Snapshots)</div>
    <div class="tc"><div class="ts" id="a-cov-co" style="max-height:600px"></div></div>
  </div>

  <!-- ═══════════════════════════════════════════════════════ -->
  <!-- TAB 3: DATA QUALITY (Live DB)                           -->
  <!-- ═══════════════════════════════════════════════════════ -->
  <div id="tab-quality" class="tab-content">
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:16px;font-size:12px;line-height:1.7">
      <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:var(--green)">What is this tab?</div>
      <div style="color:var(--text-dim)">
        <strong style="color:var(--text)">Source:</strong> Live database (<code style="font-family:JetBrains Mono;font-size:11px;color:var(--amber)">sprinklr_table</code>) — the current state of the data after all Lambda processing.<br>
        <strong style="color:var(--text)">What it shows:</strong> Data quality and completeness checks grouped by the <code style="font-family:JetBrains Mono;font-size:11px">Date</code> column (published date). Checks for NULL values across key columns, whether each platform/country is receiving impression data each week, and delivery type breakdown.<br>
        <strong style="color:var(--text)">Columns checked for NULLs:</strong> <code style="font-family:JetBrains Mono;font-size:11px">Total Impressions</code>, <code style="font-family:JetBrains Mono;font-size:11px">Country</code>, <code style="font-family:JetBrains Mono;font-size:11px">Platform Granular</code>, <code style="font-family:JetBrains Mono;font-size:11px">Delivery</code>, <code style="font-family:JetBrains Mono;font-size:11px">Account</code>, <code style="font-family:JetBrains Mono;font-size:11px">Permalink</code>, <code style="font-family:JetBrains Mono;font-size:11px">Benchmark Type</code>, <code style="font-family:JetBrains Mono;font-size:11px">Benchmark Matcher</code>, <code style="font-family:JetBrains Mono;font-size:11px">Reputational Topic</code>, <code style="font-family:JetBrains Mono;font-size:11px">Reputational Sub Topic</code>, <code style="font-family:JetBrains Mono;font-size:11px">Post Message</code>, <code style="font-family:JetBrains Mono;font-size:11px">Campaign</code>, <code style="font-family:JetBrains Mono;font-size:11px">Region</code>, <code style="font-family:JetBrains Mono;font-size:11px">MESSAGE_TYPE</code>, <code style="font-family:JetBrains Mono;font-size:11px">Post Format</code>, <code style="font-family:JetBrains Mono;font-size:11px">Matcher</code>, <code style="font-family:JetBrains Mono;font-size:11px">Content Category Type</code>, <code style="font-family:JetBrains Mono;font-size:11px">author_name</code>. Null heatmap shows % of rows with missing data — hover for raw counts.<br>
        <strong style="color:var(--text)">How it differs from WoW tabs:</strong> WoW tabs use Lambda 7 backup snapshots from S3 to track <em>changes over time</em>. This tab queries the live DB to check the <em>current state</em> of the data — completeness, gaps, and quality issues.
      </div>
    </div>

    <div class="filter-bar">
      <label>Country</label><select id="q-co"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Platform</label><select id="q-pl"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Delivery</label><select id="q-dl"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Duration</label>
      <select id="q-dur">
        <option value="1">Last 1 week</option>
        <option value="2">Last 2 weeks</option>
        <option value="3">Last 3 weeks</option>
        <option value="4">Last 4 weeks</option>
        <option value="8" selected>Last 8 weeks</option>
        <option value="12">Last 12 weeks</option>
        <option value="16">Last 16 weeks</option>
        <option value="26">Last 6 months</option>
        <option value="52">Last 1 year</option>
        <option value="0">All time</option>
      </select>
      <div class="filter-sep"></div>
      <button class="btn btn-primary" onclick="applyQuality()">Apply</button>
      <button class="btn btn-ghost" onclick="exportQuality()">↓ CSV</button>
    </div>

    <div class="kpi-row" id="q-kpis"></div>

    <div class="sh"><span class="dot" style="background:var(--green)"></span>Impressions Flow by Platform (✓ / ✗)</div>
    <div class="sub-note" style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px 15px">Does each platform have impression data each week? Green ✓ = has data · Red ✗ = missing</div>
    <div class="tc"><div class="ts" id="q-flow" style="max-height:600px"></div></div>

    <div class="sh"><span class="dot" style="background:var(--red)"></span>Null Check Heatmap (% of rows with missing data)</div>
    <div class="sub-note" style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px 15px">Shows % of rows with NULL/empty values per column per week · Red = has nulls · Green = 0% · Hover for raw counts</div>
    <div class="tc"><div class="ts" id="q-nulls" style="max-height:600px"></div></div>

    <div class="grid2">
      <div class="card"><h3>Total Rows by Week</h3><div class="sub">Post count trend from database</div><div class="cw"><canvas id="qc1"></canvas></div></div>
      <div class="card"><h3>Null Impressions by Week</h3><div class="sub">How many rows are missing impression data</div><div class="cw"><canvas id="qc2"></canvas></div></div>
    </div>
    <div class="grid2">
      <div class="card"><h3>Total Impressions by Week</h3><div class="sub">Sum of impressions trend — are totals growing or shrinking?</div><div class="cw"><canvas id="qc3"></canvas></div></div>
      <div class="card"><h3>Null Rate Trend — Key Columns</h3><div class="sub">% of rows with NULL values per week for critical columns</div><div class="cw"><canvas id="qc4"></canvas></div></div>
    </div>
    <div class="grid2">
      <div class="card"><h3>Rows by Platform per Week</h3><div class="sub">Stacked — is any platform dropping off?</div><div class="cw tall"><canvas id="qc5"></canvas></div></div>
      <div class="card"><h3>Rows by Country per Week</h3><div class="sub">Stacked — is any country missing?</div><div class="cw tall"><canvas id="qc6"></canvas></div></div>
    </div>

    <div class="sh"><span class="dot" style="background:var(--cyan)"></span>Impressions Flow by Country (✓ / ✗)</div>
    <div class="tc"><div class="ts" id="q-flow-co" style="max-height:600px"></div></div>

    <div class="sh"><span class="dot" style="background:var(--purple)"></span>Delivery Breakdown by Week</div>
    <div class="tc"><div class="ts" id="q-delivery" style="max-height:480px"></div></div>
  </div>

  <!-- ═══════════════════════════════════════════════════════ -->
  <!-- TAB 4: LINE & COLLAB POSTS                              -->
  <!-- ═══════════════════════════════════════════════════════ -->
  <div id="tab-linecollab" class="tab-content">
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:16px;font-size:12px;line-height:1.7">
      <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:var(--amber)">What is this tab?</div>
      <div style="color:var(--text-dim)">
        <strong style="color:var(--text)">Source:</strong> Live database (<code style="font-family:JetBrains Mono;font-size:11px;color:var(--amber)">sprinklr_table</code>) filtered to <code style="font-family:JetBrains Mono;font-size:11px">is_manual_tracker = 1</code> (LINE posts) and <code style="font-family:JetBrains Mono;font-size:11px">is_manual_tracker = 2</code> (Collab posts).<br>
        <strong style="color:var(--text)">What it shows:</strong> Data quality checks specifically for LINE and Collaboration posts — these go through separate pipeline paths (Lambda 8 for Collab deduplication, Lambda 9 for LINE). Checks for NULL values in key columns, impression coverage, and collab-specific fields like collaborator name and collab post type.<br>
        <strong style="color:var(--text)">Columns checked:</strong> <code style="font-family:JetBrains Mono;font-size:11px">Total Impressions</code>, <code style="font-family:JetBrains Mono;font-size:11px">Permalink</code>, <code style="font-family:JetBrains Mono;font-size:11px">Benchmark Type/Matcher</code>, <code style="font-family:JetBrains Mono;font-size:11px">Delivery</code>, <code style="font-family:JetBrains Mono;font-size:11px">Post Message</code>, <code style="font-family:JetBrains Mono;font-size:11px">Reputational Topic</code>, <code style="font-family:JetBrains Mono;font-size:11px">Content Category Type</code>, <code style="font-family:JetBrains Mono;font-size:11px">author_name</code>, <code style="font-family:JetBrains Mono;font-size:11px">Matcher</code>, <code style="font-family:JetBrains Mono;font-size:11px">what_kind_of_collab_post</code>, <code style="font-family:JetBrains Mono;font-size:11px">is_this_a_collab_post?</code>, <code style="font-family:JetBrains Mono;font-size:11px">Name of the collaborator</code>.
      </div>
    </div>

    <div class="filter-bar">
      <label>Type</label>
      <select id="lc-type">
        <option value="ALL">Both (LINE + Collab)</option>
        <option value="LINE">LINE only (tracker=1)</option>
        <option value="Collab">Collab only (tracker=2)</option>
      </select>
      <div class="filter-sep"></div>
      <label>Country</label><select id="lc-co"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Platform</label><select id="lc-pl"><option value="ALL">All</option></select>
      <div class="filter-sep"></div>
      <label>Duration</label>
      <select id="lc-dur">
        <option value="1">Last 1 week</option>
        <option value="2">Last 2 weeks</option>
        <option value="3">Last 3 weeks</option>
        <option value="4">Last 4 weeks</option>
        <option value="8" selected>Last 8 weeks</option>
        <option value="12">Last 12 weeks</option>
        <option value="16">Last 16 weeks</option>
        <option value="26">Last 6 months</option>
        <option value="52">Last 1 year</option>
        <option value="0">All time</option>
      </select>
      <div class="filter-sep"></div>
      <button class="btn btn-primary" onclick="applyLC()">Apply</button>
      <button class="btn btn-ghost" onclick="exportLC()">↓ CSV</button>
    </div>

    <div class="kpi-row" id="lc-kpis"></div>

    <div class="grid2">
      <div class="card"><h3>LINE vs Collab Posts by Week</h3><div class="sub">Stacked bar — volume trend</div><div class="cw"><canvas id="lcc1"></canvas></div></div>
      <div class="card"><h3>Impressions by Type</h3><div class="sub">LINE vs Collab impression totals per week</div><div class="cw"><canvas id="lcc2"></canvas></div></div>
    </div>

    <div class="sh"><span class="dot" style="background:var(--green)"></span>Impression Flow by Account (✓ / ✗)</div>
    <div class="sub-note" style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px 15px">Does each account have impression data each week?</div>
    <div class="tc"><div class="ts" id="lc-flow" style="max-height:600px"></div></div>

    <div class="sh"><span class="dot" style="background:var(--red)"></span>Null Check Heatmap</div>
    <div class="sub-note" style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px 15px">Count of NULL/empty values per column per week · Red = nulls present</div>
    <div class="tc"><div class="ts" id="lc-nulls" style="max-height:600px"></div></div>

    <div class="sh"><span class="dot" style="background:var(--cyan)"></span>Non-Null Fill Rate by Column (%)</div>
    <div class="sub-note" style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px 15px">% of rows that have data (non-null/non-empty) per week · Higher = better</div>
    <div class="tc"><div class="ts" id="lc-fill" style="max-height:600px"></div></div>
  </div>
</div>

<script>
// ═══ DATA ═══
const WOW={wow_blob};
const QAL={quality_blob};
const LCR={lc_blob};
const C=['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#14b8a6','#f97316','#6366f1','#84cc16','#e879f9','#22d3ee','#fb923c','#a78bfa','#34d399'];
const charts={{}};
function dc(id){{if(charts[id])charts[id].destroy()}}
const fmt=v=>v==null?'—':typeof v==='number'?(Math.abs(v)>=1e9?(v/1e9).toFixed(1)+'B':Math.abs(v)>=1e6?(v/1e6).toFixed(1)+'M':Math.abs(v)>=1e3?(v/1e3).toFixed(1)+'K':v.toLocaleString()):v;
const pct=v=>v==null?'—':(v>0?'+':'')+v.toFixed(1)+'%';
const cc=v=>v==null?'flat':v>0?'up':'down';
const ic=v=>v==null?'':v>0?'▲':'▼';

// ═══ TAB SWITCHING ═══
function switchTab(id){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('tab-'+id).classList.add('active');
}}

// ═══ CHART HELPERS ═══
function bOpts(yLabel,stacked){{
  return {{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},
    plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'#1a2332',borderColor:'#2d3d52',borderWidth:1,bodyFont:{{family:'JetBrains Mono',size:11}},callbacks:{{label:ctx=>ctx.dataset.label+': '+fmt(ctx.parsed.y)}}}}}},
    scales:{{x:{{type:'time',time:{{unit:'week',displayFormats:{{week:'MMM d'}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568'}},stacked:!!stacked}},y:{{title:{{display:true,text:yLabel,color:'#8492a6',font:{{family:'DM Sans',size:10}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>fmt(v)}},stacked:!!stacked}}}}
  }};
}}
function pieOpts(){{return {{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'right',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}},padding:6,boxWidth:10}}}},tooltip:{{backgroundColor:'#1a2332',borderColor:'#2d3d52',borderWidth:1,bodyFont:{{family:'JetBrains Mono',size:11}},callbacks:{{label:ctx=>ctx.label+': '+fmt(ctx.parsed)}}}}}}}}}}

// ═══════════════════════════════════════════════════════
// TAB 1: WEEK ON WEEK
// ═══════════════════════════════════════════════════════
let wFiltered=WOW;

function applyWow(){{
  const co=document.getElementById('w-co').value;
  const pl=document.getElementById('w-pl').value;
  const wk=parseInt(document.getElementById('w-wk').value);
  wFiltered=WOW.filter(r=>{{
    if(co!=='ALL'&&r.Country!==co)return false;
    if(pl!=='ALL'&&r['Platform Granular']!==pl)return false;
    return true;
  }});
  if(wk>0){{
    const allW=[...new Set(wFiltered.map(r=>r.week_ending))].sort().reverse().slice(0,wk);
    const ws=new Set(allW);wFiltered=wFiltered.filter(r=>ws.has(r.week_ending));
  }}
  renderWow();
}}

function renderWow(){{
  // KPIs
  const byW={{}};
  wFiltered.forEach(r=>{{const w=r.week_ending;if(!byW[w])byW[w]={{w,imp:0,posts:0}};byW[w].imp+=(r.total_impressions||0);byW[w].posts+=(r.post_count||0)}});
  const agg=Object.values(byW).sort((a,b)=>a.w.localeCompare(b.w));
  const totalImp=agg.reduce((s,r)=>s+r.imp,0);
  const totalPosts=agg.reduce((s,r)=>s+r.posts,0);
  const L=agg[agg.length-1],P=agg.length>=2?agg[agg.length-2]:null;
  let iChg=null;
  if(L&&P&&P.imp>0)iChg=((L.imp-P.imp)/P.imp*100);

  document.getElementById('w-kpis').innerHTML=`
    <div class="kpi blue"><div class="kpi-label">Total Impressions</div><div class="kpi-val">${{fmt(totalImp)}}</div><div class="kpi-sub">${{agg.length}} snapshots</div></div>
    <div class="kpi green"><div class="kpi-label">Latest Snapshot</div><div class="kpi-val">${{L?fmt(L.imp):'—'}}</div><div class="kpi-sub ${{cc(iChg)}}">${{ic(iChg)}} ${{pct(iChg)}} vs prev</div></div>
    <div class="kpi amber"><div class="kpi-label">Latest Posts</div><div class="kpi-val">${{L?fmt(L.posts):'—'}}</div><div class="kpi-sub">${{L?L.w:''}}</div></div>
    <div class="kpi purple"><div class="kpi-label">Avg/Post</div><div class="kpi-val">${{totalPosts>0?fmt(totalImp/totalPosts):'—'}}</div></div>
    <div class="kpi cyan"><div class="kpi-label">Scope</div><div class="kpi-val">${{new Set(wFiltered.map(r=>r.Country)).size}}C · ${{new Set(wFiltered.map(r=>r['Platform Granular'])).size}}P</div></div>
  `;

  // Chart 1: Aggregate impressions + change bars
  dc('wc1');
  const chgs=agg.map((w,i)=>i===0?0:w.imp-agg[i-1].imp);
  charts.wc1=new Chart(document.getElementById('wc1'),{{
    type:'bar',
    data:{{labels:agg.map(w=>new Date(w.w)),datasets:[
      {{type:'line',label:'Impressions',data:agg.map(w=>w.imp),borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.08)',fill:true,tension:0.3,pointRadius:3,pointBackgroundColor:'#3b82f6',yAxisID:'y'}},
      {{type:'bar',label:'Change',data:chgs,backgroundColor:chgs.map(v=>v>=0?'rgba(16,185,129,0.35)':'rgba(239,68,68,0.35)'),borderColor:chgs.map(v=>v>=0?'#10b981':'#ef4444'),borderWidth:1,yAxisID:'y1'}}
    ]}},
    options:{{
      ...bOpts('Impressions'),
      plugins:{{legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}}}},
      scales:{{...bOpts('').scales,y1:{{position:'right',title:{{display:true,text:'Change',color:'#8492a6',font:{{family:'DM Sans',size:10}}}},grid:{{display:false}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>fmt(v)}}}}}}
    }}
  }});

  // Chart 2: Platform stacked
  const byPlat={{}};
  wFiltered.forEach(r=>{{const k=r['Platform Granular']||'?',w=r.week_ending;if(!byPlat[k])byPlat[k]={{}};byPlat[k][w]=(byPlat[k][w]||0)+(r.total_impressions||0)}});
  const allWks=[...new Set(wFiltered.map(r=>r.week_ending))].sort();
  const pNames=Object.keys(byPlat).sort();
  dc('wc2');
  charts.wc2=new Chart(document.getElementById('wc2'),{{type:'line',data:{{labels:allWks.map(w=>new Date(w)),datasets:pNames.map((n,i)=>({{label:n,data:allWks.map(w=>byPlat[n][w]||0),borderColor:C[i%C.length],backgroundColor:C[i%C.length]+'22',fill:true,tension:0.3,pointRadius:0}}))}},options:{{...bOpts('Impressions',true),plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:6}}}}}}}}}});

  // Chart 3: Country stacked
  const byCo={{}};
  wFiltered.forEach(r=>{{const k=r.Country||'?',w=r.week_ending;if(!byCo[k])byCo[k]={{}};byCo[k][w]=(byCo[k][w]||0)+(r.total_impressions||0)}});
  const cNames=Object.keys(byCo).sort();
  dc('wc3');
  charts.wc3=new Chart(document.getElementById('wc3'),{{type:'line',data:{{labels:allWks.map(w=>new Date(w)),datasets:cNames.map((n,i)=>({{label:n,data:allWks.map(w=>byCo[n][w]||0),borderColor:C[(i+7)%C.length],backgroundColor:C[(i+7)%C.length]+'22',fill:true,tension:0.3,pointRadius:0}}))}},options:{{...bOpts('Impressions',true),plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:6}}}}}}}}}});
}}

// ═══════════════════════════════════════════════════════
// TAB 2: WOW ANALYSIS
// ═══════════════════════════════════════════════════════
let aFiltered=WOW;

function applyAnalysis(){{
  const co=document.getElementById('a-co').value;
  const pl=document.getElementById('a-pl').value;
  const wk=parseInt(document.getElementById('a-wk').value);
  aFiltered=WOW.filter(r=>{{
    if(co!=='ALL'&&r.Country!==co)return false;
    if(pl!=='ALL'&&r['Platform Granular']!==pl)return false;
    return true;
  }});
  if(wk>0){{
    const allW=[...new Set(aFiltered.map(r=>r.week_ending))].sort().reverse().slice(0,wk);
    const ws=new Set(allW);aFiltered=aFiltered.filter(r=>ws.has(r.week_ending));
  }}
  renderAnalysis();
}}

function renderAnalysis(){{
  const th=parseInt(document.getElementById('a-th').value);
  const anoms=aFiltered.filter(r=>r.impressions_pct_change!=null&&Math.abs(r.impressions_pct_change)>th).sort((a,b)=>Math.abs(b.impressions_pct_change)-Math.abs(a.impressions_pct_change));
  const drops=anoms.filter(r=>r.impressions_pct_change<0);
  const spikes=anoms.filter(r=>r.impressions_pct_change>0);

  document.getElementById('a-kpis').innerHTML=`
    <div class="kpi red"><div class="kpi-label">Total Drops</div><div class="kpi-val">${{drops.length}}</div></div>
    <div class="kpi green"><div class="kpi-label">Total Spikes</div><div class="kpi-val">${{spikes.length}}</div></div>
    <div class="kpi amber"><div class="kpi-label">Worst Drop</div><div class="kpi-val">${{drops.length?pct(drops[0].impressions_pct_change):'—'}}</div><div class="kpi-sub">${{drops.length?drops[0].Country+' · '+drops[0]['Platform Granular']:''}}</div></div>
    <div class="kpi purple"><div class="kpi-label">Biggest Spike</div><div class="kpi-val">${{spikes.length?pct(spikes[0].impressions_pct_change):'—'}}</div><div class="kpi-sub">${{spikes.length?spikes[0].Country+' · '+spikes[0]['Platform Granular']:''}}</div></div>
  `;

  // Bubble chart
  dc('ac1');
  const dropPts=drops.map(r=>({{x:r.week_ending,y:r.impressions_pct_change,r:Math.min(Math.abs(r.impressions_pct_change)/5,20),label:r.Country+' · '+r['Platform Granular']}}));
  const spikePts=spikes.map(r=>({{x:r.week_ending,y:r.impressions_pct_change,r:Math.min(Math.abs(r.impressions_pct_change)/5,20),label:r.Country+' · '+r['Platform Granular']}}));
  charts.ac1=new Chart(document.getElementById('ac1'),{{
    type:'bubble',
    data:{{datasets:[
      {{label:'Drops',data:dropPts,backgroundColor:'rgba(239,68,68,0.35)',borderColor:'#ef4444',borderWidth:1}},
      {{label:'Spikes',data:spikePts,backgroundColor:'rgba(16,185,129,0.35)',borderColor:'#10b981',borderWidth:1}}
    ]}},
    options:{{responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}},tooltip:{{backgroundColor:'#1a2332',borderColor:'#2d3d52',borderWidth:1,bodyFont:{{family:'JetBrains Mono',size:11}},callbacks:{{label:ctx=>{{const p=ctx.raw;return`${{p.label}}: ${{pct(p.y)}}`}}}}}}}},
      scales:{{x:{{type:'time',time:{{unit:'week',displayFormats:{{week:'MMM d'}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568'}}}},y:{{title:{{display:true,text:'Change %',color:'#8492a6',font:{{family:'DM Sans',size:10}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>pct(v)}}}}}}
    }}
  }});

  // Anomaly table
  let h='<table><thead><tr><th>Snapshot</th><th>Country</th><th>Platform</th><th>Posts</th><th>Impressions</th><th>Prev</th><th>Change</th><th>%</th><th>Status</th></tr></thead><tbody>';
  anoms.forEach(r=>{{
    const c=r.impressions_pct_change<0?'neg':'pos';
    const pill=r.impressions_pct_change<-50?'pill-r':r.impressions_pct_change<0?'pill-a':'pill-g';
    const lbl=r.impressions_pct_change<-50?'CRITICAL':r.impressions_pct_change<0?'DROP':'SPIKE';
    h+=`<tr class="${{r.impressions_pct_change<-50?'anom-row':''}}"><td>${{r.week_ending}}</td><td>${{r.Country}}</td><td>${{r['Platform Granular']}}</td><td>${{r.post_count}}</td><td>${{fmt(r.total_impressions)}}</td><td>${{fmt(r.prev_impressions)}}</td><td class="${{c}}">${{fmt(r.impressions_change)}}</td><td class="${{c}}">${{pct(r.impressions_pct_change)}}</td><td><span class="pill ${{pill}}">${{lbl}}</span></td></tr>`;
  }});
  h+='</tbody></table>';
  document.getElementById('a-tbl').innerHTML=h;

  // Coverage heatmaps
  const allWks=[...new Set(aFiltered.map(r=>r.week_ending))].sort();
  function covTable(dimKey,targetId){{
    const m={{}};
    aFiltered.forEach(r=>{{const d=r[dimKey]||'?',w=r.week_ending;if(!m[d])m[d]={{}};if(!m[d][w])m[d][w]={{p:0,i:0}};m[d][w].p+=(r.post_count||0);m[d][w].i+=(r.total_impressions||0)}});
    const dims=Object.keys(m).sort();
    let t='<table><thead><tr><th class="cov-dim">Name</th>';
    allWks.forEach(w=>{{t+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
    t+='</tr></thead><tbody>';
    dims.forEach(d=>{{
      t+=`<tr><td class="cov-dim">${{d}}</td>`;
      allWks.forEach(w=>{{
        const c=m[d]?.[w];
        if(!c||c.p===0)t+=`<td class="cov-miss">✗</td>`;
        else{{const cls=c.i>=1e6?'cov-high':c.i>=1e5?'cov-mid':'cov-low';t+=`<td class="${{cls}}" title="${{fmt(c.i)}}">${{c.p}}</td>`}}
      }});
      t+='</tr>';
    }});
    t+='</tbody></table>';
    document.getElementById(targetId).innerHTML=t;
  }}
  covTable('Platform Granular','a-cov-plat');
  covTable('Country','a-cov-co');
}}

// ═══════════════════════════════════════════════════════
// TAB 3: DATA QUALITY (Live DB)
// ═══════════════════════════════════════════════════════
let qFiltered=QAL;

function applyQuality(){{
  const co=document.getElementById('q-co').value;
  const pl=document.getElementById('q-pl').value;
  const dl=document.getElementById('q-dl').value;
  const wk=parseInt(document.getElementById('q-dur').value);
  qFiltered=QAL.filter(r=>{{
    if(co!=='ALL'&&r.Country!==co)return false;
    if(pl!=='ALL'&&r['Platform Granular']!==pl)return false;
    if(dl!=='ALL'&&r.Delivery!==dl)return false;
    return true;
  }});
  if(wk>0){{
    const allW=[...new Set(qFiltered.map(r=>r.week_ending))].sort();
    const keepWeeks=new Set(allW.slice(-wk));
    qFiltered=qFiltered.filter(r=>keepWeeks.has(r.week_ending));
  }}
  renderQuality();
}}

function renderQuality(){{
  const allWks=[...new Set(qFiltered.map(r=>r.week_ending))].sort();

  // Aggregate by week
  const byW={{}};
  qFiltered.forEach(r=>{{
    const w=r.week_ending;if(!byW[w])byW[w]={{w,rows:0,nullImp:0,hasImp:0,sumImp:0,nullCo:0,nullPl:0,nullDl:0,nullAcc:0,nullPerm:0,nullBench:0,nullMatch:0,nullRepTopic:0,nullRepSub:0,nullPostMsg:0,nullCampaign:0,nullRegion:0,nullMsgType:0,nullPostFmt:0,nullMatcherCol:0,nullContentCat:0,nullAuthor:0}};
    byW[w].rows+=(r.total_rows||0);
    byW[w].nullImp+=(r.null_impressions||0);
    byW[w].hasImp+=(r.has_impressions||0);
    byW[w].sumImp+=(r.sum_impressions||0);
    byW[w].nullCo+=(r.null_country||0);
    byW[w].nullPl+=(r.null_platform||0);
    byW[w].nullDl+=(r.null_delivery||0);
    byW[w].nullAcc+=(r.null_account||0);
    byW[w].nullPerm+=(r.null_permalink||0);
    byW[w].nullBench+=(r.null_benchmark||0);
    byW[w].nullMatch+=(r.null_matcher||0);
    byW[w].nullRepTopic+=(r.null_rep_topic||0);
    byW[w].nullRepSub+=(r.null_rep_sub||0);
    byW[w].nullPostMsg+=(r.null_post_msg||0);
    byW[w].nullCampaign+=(r.null_campaign||0);
    byW[w].nullRegion+=(r.null_region||0);
    byW[w].nullMsgType+=(r.null_msg_type||0);
    byW[w].nullPostFmt+=(r.null_post_format||0);
    byW[w].nullMatcherCol+=(r.null_matcher_col||0);
    byW[w].nullContentCat+=(r.null_content_cat||0);
    byW[w].nullAuthor+=(r.null_author||0);
  }});
  const wagg=allWks.map(w=>byW[w]).filter(Boolean);

  // KPIs
  const totalRows=wagg.reduce((s,r)=>s+r.rows,0);
  const totalNullImp=wagg.reduce((s,r)=>s+r.nullImp,0);
  const totalNullBench=wagg.reduce((s,r)=>s+r.nullBench,0);
  const totalNullMatch=wagg.reduce((s,r)=>s+r.nullMatch,0);
  const nullPct=totalRows>0?(totalNullImp/totalRows*100):0;
  const latestW=wagg[wagg.length-1];

  document.getElementById('q-kpis').innerHTML=`
    <div class="kpi blue"><div class="kpi-label">Total Rows</div><div class="kpi-val">${{fmt(totalRows)}}</div><div class="kpi-sub">${{allWks.length}} weeks</div></div>
    <div class="kpi ${{nullPct>5?'red':'green'}}"><div class="kpi-label">Null Impressions</div><div class="kpi-val">${{fmt(totalNullImp)}}</div><div class="kpi-sub">${{nullPct.toFixed(1)}}% of rows</div></div>
    <div class="kpi ${{totalNullBench>0?'amber':'green'}}"><div class="kpi-label">Null Benchmark Type</div><div class="kpi-val">${{fmt(totalNullBench)}}</div></div>
    <div class="kpi ${{totalNullMatch>0?'amber':'green'}}"><div class="kpi-label">Null Bench Matcher</div><div class="kpi-val">${{fmt(totalNullMatch)}}</div></div>
    <div class="kpi green"><div class="kpi-label">Latest Week</div><div class="kpi-val">${{latestW?fmt(latestW.rows):'—'}}</div><div class="kpi-sub">${{latestW?latestW.w:''}}</div></div>
    <div class="kpi cyan"><div class="kpi-label">Platforms</div><div class="kpi-val">${{new Set(qFiltered.map(r=>r['Platform Granular'])).size}}</div></div>
  `;

  // ── Impressions Flow: Platform x Week (tick/cross) ──
  const platFlow={{}};
  qFiltered.forEach(r=>{{
    const p=r['Platform Granular']||'?',w=r.week_ending;
    if(!platFlow[p])platFlow[p]={{}};
    if(!platFlow[p][w])platFlow[p][w]={{rows:0,imp:0}};
    platFlow[p][w].rows+=(r.has_impressions||0);
    platFlow[p][w].imp+=(r.sum_impressions||0);
  }});
  const plats=Object.keys(platFlow).sort();
  let ft='<table><thead><tr><th class="cov-dim">Platform</th>';
  allWks.forEach(w=>{{ft+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
  ft+='</tr></thead><tbody>';
  plats.forEach(p=>{{
    ft+=`<tr><td class="cov-dim">${{p}}</td>`;
    allWks.forEach(w=>{{
      const c=platFlow[p]?.[w];
      if(!c||c.rows===0)ft+=`<td class="cross">✗</td>`;
      else ft+=`<td class="tick" title="${{fmt(c.imp)}} imp · ${{c.rows}} rows">✓</td>`;
    }});
    ft+='</tr>';
  }});
  ft+='</tbody></table>';
  document.getElementById('q-flow').innerHTML=ft;

  // ── Country Flow ──
  const coFlow={{}};
  qFiltered.forEach(r=>{{
    const co=r.Country||'?',w=r.week_ending;
    if(!coFlow[co])coFlow[co]={{}};
    if(!coFlow[co][w])coFlow[co][w]={{rows:0,imp:0}};
    coFlow[co][w].rows+=(r.has_impressions||0);
    coFlow[co][w].imp+=(r.sum_impressions||0);
  }});
  const cos=Object.keys(coFlow).sort();
  let ct='<table><thead><tr><th class="cov-dim">Country</th>';
  allWks.forEach(w=>{{ct+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
  ct+='</tr></thead><tbody>';
  cos.forEach(co=>{{
    ct+=`<tr><td class="cov-dim">${{co}}</td>`;
    allWks.forEach(w=>{{
      const c=coFlow[co]?.[w];
      if(!c||c.rows===0)ct+=`<td class="cross">✗</td>`;
      else ct+=`<td class="tick" title="${{fmt(c.imp)}}">✓</td>`;
    }});
    ct+='</tr>';
  }});
  ct+='</tbody></table>';
  document.getElementById('q-flow-co').innerHTML=ct;

  // ── Null Check Heatmap ──
  const nullCols=[
    {{k:'nullImp',l:'Impressions'}},{{k:'nullCo',l:'Country'}},{{k:'nullPl',l:'Platform'}},
    {{k:'nullDl',l:'Delivery'}},{{k:'nullAcc',l:'Account'}},{{k:'nullPerm',l:'Permalink'}},
    {{k:'nullBench',l:'Benchmark Type'}},{{k:'nullMatch',l:'Benchmark Matcher'}},
    {{k:'nullRepTopic',l:'Reputational Topic'}},{{k:'nullRepSub',l:'Reputational Sub Topic'}},
    {{k:'nullPostMsg',l:'Post Message'}},{{k:'nullCampaign',l:'Campaign'}},
    {{k:'nullRegion',l:'Region'}},{{k:'nullMsgType',l:'MESSAGE_TYPE'}},
    {{k:'nullPostFmt',l:'Post Format'}},{{k:'nullMatcherCol',l:'Matcher'}},
    {{k:'nullContentCat',l:'Content Category Type'}},{{k:'nullAuthor',l:'author_name'}}
  ];
  let nt='<table><thead><tr><th class="cov-dim">Column</th>';
  allWks.forEach(w=>{{nt+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
  nt+='</tr></thead><tbody>';
  nullCols.forEach(col=>{{
    nt+=`<tr><td class="cov-dim">${{col.l}}</td>`;
    allWks.forEach(w=>{{
      const d=byW[w];
      if(!d){{nt+=`<td class="cov-miss">—</td>`;return}}
      const nulls=d[col.k]||0;
      const pcnt=d.rows>0?(nulls/d.rows*100):0;
      if(nulls===0)nt+=`<td class="null-ok">0%</td>`;
      else nt+=`<td class="null-bad" title="${{nulls}} nulls of ${{d.rows}} rows">${{pcnt.toFixed(1)}}%</td>`;
    }});
    nt+='</tr>';
  }});
  nt+='</tbody></table>';
  document.getElementById('q-nulls').innerHTML=nt;

  // ── Charts ──
  dc('qc1');
  charts.qc1=new Chart(document.getElementById('qc1'),{{type:'bar',data:{{labels:wagg.map(w=>new Date(w.w)),datasets:[{{label:'Rows',data:wagg.map(w=>w.rows),backgroundColor:'rgba(59,130,246,0.4)',borderColor:'#3b82f6',borderWidth:1,borderRadius:3}}]}},options:bOpts('Total Rows')}});

  dc('qc2');
  charts.qc2=new Chart(document.getElementById('qc2'),{{type:'bar',data:{{labels:wagg.map(w=>new Date(w.w)),datasets:[
    {{label:'Has Impressions',data:wagg.map(w=>w.hasImp),backgroundColor:'rgba(16,185,129,0.4)',borderColor:'#10b981',borderWidth:1,borderRadius:3}},
    {{label:'Null Impressions',data:wagg.map(w=>w.nullImp),backgroundColor:'rgba(239,68,68,0.4)',borderColor:'#ef4444',borderWidth:1,borderRadius:3}}
  ]}},options:{{...bOpts('Rows'),plugins:{{legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}}}}}}}});

  // Chart 3: Impressions trend (line)
  dc('qc3');
  charts.qc3=new Chart(document.getElementById('qc3'),{{type:'line',data:{{labels:wagg.map(w=>new Date(w.w)),datasets:[
    {{label:'Total Impressions',data:wagg.map(w=>w.sumImp),borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.08)',fill:true,tension:0.3,pointRadius:3,pointBackgroundColor:'#3b82f6'}}
  ]}},options:bOpts('Impressions')}});

  // Chart 4: Null rate trend for key columns (line)
  dc('qc4');
  const nullRateData=[
    {{k:'nullImp',l:'Impressions',c:'#ef4444'}},
    {{k:'nullBench',l:'Benchmark Type',c:'#f59e0b'}},
    {{k:'nullMatch',l:'Bench Matcher',c:'#8b5cf6'}},
    {{k:'nullRepTopic',l:'Reputational Topic',c:'#06b6d4'}},
    {{k:'nullPostMsg',l:'Post Message',c:'#ec4899'}},
    {{k:'nullAuthor',l:'author_name',c:'#10b981'}}
  ];
  charts.qc4=new Chart(document.getElementById('qc4'),{{type:'line',data:{{
    labels:wagg.map(w=>new Date(w.w)),
    datasets:nullRateData.map(col=>({{
      label:col.l,
      data:wagg.map(w=>w.rows>0?(w[col.k]/w.rows*100):0),
      borderColor:col.c,backgroundColor:'transparent',tension:0.3,pointRadius:2,borderWidth:2
    }}))
  }},options:{{
    ...bOpts('Null %'),
    plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:6}}}}}},
    scales:{{...bOpts('Null %').scales,y:{{...bOpts('Null %').scales.y,ticks:{{...bOpts('Null %').scales.y.ticks,callback:v=>v.toFixed(0)+'%'}}}}}}
  }}}});

  // Chart 5: Rows by Platform stacked bar
  dc('qc5');
  const platByW={{}};
  qFiltered.forEach(r=>{{
    const p=r['Platform Granular']||'?',w=r.week_ending;
    if(!platByW[w])platByW[w]={{}};
    platByW[w][p]=(platByW[w][p]||0)+(r.total_rows||0);
  }});
  const qPlats=[...new Set(qFiltered.map(r=>r['Platform Granular']))].sort();
  charts.qc5=new Chart(document.getElementById('qc5'),{{type:'bar',data:{{
    labels:allWks.map(w=>new Date(w)),
    datasets:qPlats.map((p,i)=>({{
      label:p,data:allWks.map(w=>platByW[w]?.[p]||0),
      backgroundColor:C[i%C.length]+'88',borderColor:C[i%C.length],borderWidth:1,borderRadius:2
    }}))
  }},options:{{...bOpts('Rows',true),plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:4}}}}}}}}}});

  // Chart 6: Rows by Country stacked bar
  dc('qc6');
  const coByW={{}};
  qFiltered.forEach(r=>{{
    const co=r.Country||'?',w=r.week_ending;
    if(!coByW[w])coByW[w]={{}};
    coByW[w][co]=(coByW[w][co]||0)+(r.total_rows||0);
  }});
  const qCos2=[...new Set(qFiltered.map(r=>r.Country))].sort();
  charts.qc6=new Chart(document.getElementById('qc6'),{{type:'bar',data:{{
    labels:allWks.map(w=>new Date(w)),
    datasets:qCos2.map((co,i)=>({{
      label:co,data:allWks.map(w=>coByW[w]?.[co]||0),
      backgroundColor:C[(i+7)%C.length]+'88',borderColor:C[(i+7)%C.length],borderWidth:1,borderRadius:2
    }}))
  }},options:{{...bOpts('Rows',true),plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:4}}}}}}}}}});

  // ── Delivery Breakdown ──
  const dlByW={{}};
  qFiltered.forEach(r=>{{
    const w=r.week_ending,dl=r.Delivery||'Unknown';
    if(!dlByW[w])dlByW[w]={{}};
    dlByW[w][dl]=(dlByW[w][dl]||0)+(r.total_rows||0);
  }});
  const deliveries=[...new Set(qFiltered.map(r=>r.Delivery||'Unknown'))].sort();
  let dt='<table><thead><tr><th>Week</th>';
  deliveries.forEach(d=>{{dt+=`<th>${{d}}</th>`}});
  dt+='<th>Total</th></tr></thead><tbody>';
  allWks.forEach(w=>{{
    dt+=`<tr><td>${{w}}</td>`;
    let tot=0;
    deliveries.forEach(d=>{{const v=dlByW[w]?.[d]||0;tot+=v;dt+=`<td>${{fmt(v)}}</td>`}});
    dt+=`<td style="font-weight:600">${{fmt(tot)}}</td></tr>`;
  }});
  dt+='</tbody></table>';
  document.getElementById('q-delivery').innerHTML=dt;
}}

// ═══════════════════════════════════════════════════════
// TAB 4: LINE & COLLAB
// ═══════════════════════════════════════════════════════
let lcFiltered=LCR;

function applyLC(){{
  const tp=document.getElementById('lc-type').value;
  const co=document.getElementById('lc-co').value;
  const pl=document.getElementById('lc-pl').value;
  const wk=parseInt(document.getElementById('lc-dur').value);
  lcFiltered=LCR.filter(r=>{{
    if(tp!=='ALL'&&r.tracker_type!==tp)return false;
    if(co!=='ALL'&&r.Country!==co)return false;
    if(pl!=='ALL'&&r['Platform Granular']!==pl)return false;
    return true;
  }});
  if(wk>0){{
    const allW=[...new Set(lcFiltered.map(r=>r.week_ending))].sort();
    const keepWeeks=new Set(allW.slice(-wk));
    lcFiltered=lcFiltered.filter(r=>keepWeeks.has(r.week_ending));
  }}
  renderLC();
}}

function renderLC(){{
  const allWks=[...new Set(lcFiltered.map(r=>r.week_ending))].sort();

  // Aggregate by week + type
  const byWT={{}};
  lcFiltered.forEach(r=>{{
    const w=r.week_ending,t=r.tracker_type||'?';
    if(!byWT[w])byWT[w]={{}};
    if(!byWT[w][t])byWT[w][t]={{rows:0,imp:0}};
    byWT[w][t].rows+=(r.total_rows||0);
    byWT[w][t].imp+=(r.sum_impressions||0);
  }});

  // Aggregate by week (flat)
  const byW={{}};
  lcFiltered.forEach(r=>{{
    const w=r.week_ending;
    if(!byW[w])byW[w]={{w,rows:0,nullImp:0,hasImp:0,sumImp:0,nullPerm:0,nullBench:0,nullMatch:0,nullDl:0,nullPostMsg:0,nullRepTopic:0,nullContentCat:0,nullAuthor:0,nullCo:0,nullMatcherCol:0,nullCollabKind:0,nullIsCollab:0,nullCollabName:0}};
    byW[w].rows+=(r.total_rows||0);
    byW[w].nullImp+=(r.null_impressions||0);
    byW[w].hasImp+=(r.has_impressions||0);
    byW[w].sumImp+=(r.sum_impressions||0);
    byW[w].nullPerm+=(r.null_permalink||0);
    byW[w].nullBench+=(r.null_benchmark||0);
    byW[w].nullMatch+=(r.null_matcher||0);
    byW[w].nullDl+=(r.null_delivery||0);
    byW[w].nullPostMsg+=(r.null_post_msg||0);
    byW[w].nullRepTopic+=(r.null_rep_topic||0);
    byW[w].nullContentCat+=(r.null_content_cat||0);
    byW[w].nullAuthor+=(r.null_author||0);
    byW[w].nullCo+=(r.null_country||0);
    byW[w].nullMatcherCol+=(r.null_matcher_col||0);
    byW[w].nullCollabKind+=(r.null_collab_kind||0);
    byW[w].nullIsCollab+=(r.null_is_collab||0);
    byW[w].nullCollabName+=(r.null_collab_name||0);
  }});

  // KPIs
  const totalRows=Object.values(byW).reduce((s,r)=>s+r.rows,0);
  const lineRows=allWks.reduce((s,w)=>(byWT[w]?.LINE?.rows||0)+s,0);
  const collabRows=allWks.reduce((s,w)=>(byWT[w]?.Collab?.rows||0)+s,0);
  const totalImp=Object.values(byW).reduce((s,r)=>s+r.sumImp,0);
  const totalNullImp=Object.values(byW).reduce((s,r)=>s+r.nullImp,0);
  const nullPct=totalRows>0?(totalNullImp/totalRows*100):0;

  document.getElementById('lc-kpis').innerHTML=`
    <div class="kpi blue"><div class="kpi-label">Total Rows</div><div class="kpi-val">${{fmt(totalRows)}}</div><div class="kpi-sub">${{allWks.length}} weeks</div></div>
    <div class="kpi amber"><div class="kpi-label">LINE Posts</div><div class="kpi-val">${{fmt(lineRows)}}</div><div class="kpi-sub">is_manual_tracker = 1</div></div>
    <div class="kpi purple"><div class="kpi-label">Collab Posts</div><div class="kpi-val">${{fmt(collabRows)}}</div><div class="kpi-sub">is_manual_tracker = 2</div></div>
    <div class="kpi green"><div class="kpi-label">Total Impressions</div><div class="kpi-val">${{fmt(totalImp)}}</div></div>
    <div class="kpi ${{nullPct>5?'red':'green'}}"><div class="kpi-label">Null Impressions</div><div class="kpi-val">${{fmt(totalNullImp)}}</div><div class="kpi-sub">${{nullPct.toFixed(1)}}%</div></div>
  `;

  // Charts: LINE vs Collab by week
  dc('lcc1');
  charts.lcc1=new Chart(document.getElementById('lcc1'),{{type:'bar',data:{{labels:allWks.map(w=>new Date(w)),datasets:[
    {{label:'LINE',data:allWks.map(w=>byWT[w]?.LINE?.rows||0),backgroundColor:'rgba(245,158,11,0.5)',borderColor:'#f59e0b',borderWidth:1,borderRadius:3}},
    {{label:'Collab',data:allWks.map(w=>byWT[w]?.Collab?.rows||0),backgroundColor:'rgba(139,92,246,0.5)',borderColor:'#8b5cf6',borderWidth:1,borderRadius:3}}
  ]}},options:{{...bOpts('Posts',true),plugins:{{legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}}}}}}}});

  dc('lcc2');
  charts.lcc2=new Chart(document.getElementById('lcc2'),{{type:'bar',data:{{labels:allWks.map(w=>new Date(w)),datasets:[
    {{label:'LINE Imp',data:allWks.map(w=>byWT[w]?.LINE?.imp||0),backgroundColor:'rgba(245,158,11,0.5)',borderColor:'#f59e0b',borderWidth:1,borderRadius:3}},
    {{label:'Collab Imp',data:allWks.map(w=>byWT[w]?.Collab?.imp||0),backgroundColor:'rgba(139,92,246,0.5)',borderColor:'#8b5cf6',borderWidth:1,borderRadius:3}}
  ]}},options:{{...bOpts('Impressions',true),plugins:{{legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}}}}}}}});

  // Account flow (tick/cross)
  const acctFlow={{}};
  lcFiltered.forEach(r=>{{
    const a=(r.Account||'?')+' ('+r.tracker_type+')',w=r.week_ending;
    if(!acctFlow[a])acctFlow[a]={{}};
    if(!acctFlow[a][w])acctFlow[a][w]={{rows:0,imp:0}};
    acctFlow[a][w].rows+=(r.has_impressions||0);
    acctFlow[a][w].imp+=(r.sum_impressions||0);
  }});
  const accts=Object.keys(acctFlow).sort();
  let af='<table><thead><tr><th class="cov-dim">Account (Type)</th>';
  allWks.forEach(w=>{{af+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
  af+='</tr></thead><tbody>';
  accts.forEach(a=>{{
    af+=`<tr><td class="cov-dim">${{a}}</td>`;
    allWks.forEach(w=>{{
      const c=acctFlow[a]?.[w];
      if(!c||c.rows===0)af+=`<td class="cross">✗</td>`;
      else af+=`<td class="tick" title="${{fmt(c.imp)}} imp">✓</td>`;
    }});
    af+='</tr>';
  }});
  af+='</tbody></table>';
  document.getElementById('lc-flow').innerHTML=af;

  // Null check heatmap
  const lcNullCols=[
    {{k:'nullImp',l:'Impressions'}},{{k:'nullPerm',l:'Permalink'}},{{k:'nullBench',l:'Benchmark Type'}},
    {{k:'nullMatch',l:'Benchmark Matcher'}},{{k:'nullDl',l:'Delivery'}},{{k:'nullPostMsg',l:'Post Message'}},
    {{k:'nullRepTopic',l:'Reputational Topic'}},{{k:'nullContentCat',l:'Content Category Type'}},
    {{k:'nullAuthor',l:'author_name'}},{{k:'nullCo',l:'Country'}},{{k:'nullMatcherCol',l:'Matcher'}},
    {{k:'nullCollabKind',l:'what_kind_of_collab_post'}},{{k:'nullIsCollab',l:'is_this_a_collab_post?'}},
    {{k:'nullCollabName',l:'Name of collaborator'}}
  ];
  let ln='<table><thead><tr><th class="cov-dim">Column</th>';
  allWks.forEach(w=>{{ln+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
  ln+='</tr></thead><tbody>';
  lcNullCols.forEach(col=>{{
    ln+=`<tr><td class="cov-dim">${{col.l}}</td>`;
    allWks.forEach(w=>{{
      const d=byW[w];
      if(!d){{ln+=`<td class="cov-miss">—</td>`;return}}
      const nulls=d[col.k]||0;
      const pcnt=d.rows>0?(nulls/d.rows*100):0;
      if(nulls===0)ln+=`<td class="null-ok">0%</td>`;
      else ln+=`<td class="null-bad" title="${{nulls}} nulls of ${{d.rows}}">${{pcnt.toFixed(1)}}%</td>`;
    }});
    ln+='</tr>';
  }});
  ln+='</tbody></table>';
  document.getElementById('lc-nulls').innerHTML=ln;

  // Fill rate (% non-null) — inverted view
  let fr='<table><thead><tr><th class="cov-dim">Column</th>';
  allWks.forEach(w=>{{fr+=`<th class="cov-head">${{w.slice(5)}}</th>`}});
  fr+='</tr></thead><tbody>';
  lcNullCols.forEach(col=>{{
    fr+=`<tr><td class="cov-dim">${{col.l}}</td>`;
    allWks.forEach(w=>{{
      const d=byW[w];
      if(!d){{fr+=`<td class="cov-miss">—</td>`;return}}
      const nulls=d[col.k]||0;
      const fillPct=d.rows>0?((d.rows-nulls)/d.rows*100):0;
      if(fillPct>=99.9)fr+=`<td class="null-ok">100%</td>`;
      else if(fillPct>=80)fr+=`<td class="cov-mid">${{fillPct.toFixed(1)}}%</td>`;
      else if(fillPct>=50)fr+=`<td class="cov-low">${{fillPct.toFixed(1)}}%</td>`;
      else fr+=`<td class="null-bad">${{fillPct.toFixed(1)}}%</td>`;
    }});
    fr+='</tr>';
  }});
  fr+='</tbody></table>';
  document.getElementById('lc-fill').innerHTML=fr;
}}

// ═══ EXPORTS ═══
function exportWow(){{
  const hs=['week_ending','Country','Platform Granular','post_count','total_impressions','avg_impressions_per_post','prev_impressions','impressions_change','impressions_pct_change'];
  let csv=hs.join(',')+`\\n`;
  wFiltered.forEach(r=>{{csv+=hs.map(h=>r[h]??'').join(',')+`\\n`}});
  const b=new Blob([csv],{{type:'text/csv'}});const a=document.createElement('a');
  a.href=URL.createObjectURL(b);a.download=`wow_${{new Date().toISOString().slice(0,10)}}.csv`;a.click();
}}
function exportQuality(){{
  const hs=['week_ending','Country','Platform Granular','Delivery','total_rows','null_impressions','has_impressions','sum_impressions','null_country','null_platform','null_delivery','null_account','null_permalink','null_benchmark','null_matcher','null_rep_topic','null_rep_sub','null_post_msg','null_campaign','null_region','null_msg_type','null_post_format','null_matcher_col','null_content_cat','null_author'];
  let csv=hs.join(',')+`\\n`;
  qFiltered.forEach(r=>{{csv+=hs.map(h=>r[h]??'').join(',')+`\\n`}});
  const b=new Blob([csv],{{type:'text/csv'}});const a=document.createElement('a');
  a.href=URL.createObjectURL(b);a.download=`quality_${{new Date().toISOString().slice(0,10)}}.csv`;a.click();
}}
function exportLC(){{
  const hs=['week_ending','Country','Platform Granular','Account','tracker_type','total_rows','null_impressions','has_impressions','sum_impressions','null_permalink','null_benchmark','null_matcher','null_delivery','null_post_msg','null_rep_topic','null_content_cat','null_author','null_collab_kind','null_is_collab','null_collab_name'];
  let csv=hs.join(',')+`\\n`;
  lcFiltered.forEach(r=>{{csv+=hs.map(h=>r[h]??'').join(',')+`\\n`}});
  const b=new Blob([csv],{{type:'text/csv'}});const a=document.createElement('a');
  a.href=URL.createObjectURL(b);a.download=`line_collab_${{new Date().toISOString().slice(0,10)}}.csv`;a.click();
}}

// ═══ INIT ═══
function init(){{
  // Populate WoW filters
  const wCos=[...new Set(WOW.map(r=>r.Country))].sort();
  const wPls=[...new Set(WOW.map(r=>r['Platform Granular']))].sort();
  wCos.forEach(c=>{{document.getElementById('w-co').innerHTML+=`<option>${{c}}</option>`;document.getElementById('a-co').innerHTML+=`<option>${{c}}</option>`}});
  wPls.forEach(p=>{{document.getElementById('w-pl').innerHTML+=`<option>${{p}}</option>`;document.getElementById('a-pl').innerHTML+=`<option>${{p}}</option>`}});

  // Populate Quality filters
  const qCos=[...new Set(QAL.map(r=>r.Country))].sort();
  const qPls=[...new Set(QAL.map(r=>r['Platform Granular']))].sort();
  const qDls=[...new Set(QAL.map(r=>r.Delivery))].sort();
  qCos.forEach(c=>{{document.getElementById('q-co').innerHTML+=`<option>${{c}}</option>`}});
  qPls.forEach(p=>{{document.getElementById('q-pl').innerHTML+=`<option>${{p}}</option>`}});
  qDls.forEach(d=>{{document.getElementById('q-dl').innerHTML+=`<option>${{d}}</option>`}});

  // Populate LINE & Collab filters
  const lcCos=[...new Set(LCR.map(r=>r.Country))].sort();
  const lcPls=[...new Set(LCR.map(r=>r['Platform Granular']))].sort();
  lcCos.forEach(c=>{{document.getElementById('lc-co').innerHTML+=`<option>${{c}}</option>`}});
  lcPls.forEach(p=>{{document.getElementById('lc-pl').innerHTML+=`<option>${{p}}</option>`}});

  applyWow();
  applyAnalysis();
  applyQuality();
  applyLC();
}}
init();
</script>
</body>
</html>"""
    return html


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("=" * 60)
    print("  CORE — QA Dashboard v3")
    print("=" * 60)
    print()

    wow_df = build_wow_dataset()
    quality_df, lc_df = build_quality_dataset()

    print("\nGenerating dashboard...")
    html = generate_html(wow_df, quality_df, lc_df)

    output_file = 'index.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ Dashboard saved to: {output_file}")
    print(f"   WoW: {len(wow_df):,} rows · {wow_df['week_ending'].nunique()} snapshots")
    print(f"   Quality: {len(quality_df):,} rows · {quality_df['week_ending'].nunique()} weeks")
    print(f"   LINE & Collab: {len(lc_df):,} rows")