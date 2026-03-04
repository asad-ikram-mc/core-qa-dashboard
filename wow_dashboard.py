"""
CORE Pipeline — Week on Week Impressions Dashboard
====================================================
Connects to RDS PostgreSQL, runs WoW query, generates self-contained HTML dashboard.

Usage:
    pip install psycopg2-binary pandas
    python wow_dashboard.py

Output:
    index.html
"""

import psycopg2
import pandas as pd
import json
import os
from datetime import datetime

# ── Database connection ──
# Reads from environment variables (GitHub Actions) or falls back to defaults (local)
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'prod-fluency-sprinklr-db-eu2.cpmfagf94hdi.eu-west-2.rds.amazonaws.com'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'dbname': os.environ.get('DB_NAME', 'sprinklrproddb'),
    'user': os.environ.get('DB_USER', 'tableausprinklr'),
    'password': os.environ.get('DB_PASSWORD', 'HiXy074Hi')
}

QUERY = """
WITH weekly AS (
    SELECT
        DATE_TRUNC('week', "Date"::date) + INTERVAL '6 days' AS week_ending,
        "Country",
        "Platform Granular",
        COUNT(*) AS post_count,
        SUM("Total Impressions") AS total_impressions,
        ROUND(AVG("Total Impressions")::numeric, 2) AS avg_impressions_per_post
    FROM sprinklr_table
    WHERE "Delivery" IN ('Organic', 'Boosted')
      AND "Total Impressions" IS NOT NULL
      AND "Date" IS NOT NULL
    GROUP BY 1, 2, 3
),
wow AS (
    SELECT
        w.*,
        LAG(post_count) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending) AS prev_post_count,
        LAG(total_impressions) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending) AS prev_impressions,
        LAG(avg_impressions_per_post) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending) AS prev_avg,
        post_count - LAG(post_count) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending) AS post_count_change,
        total_impressions - LAG(total_impressions) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending) AS impressions_change,
        ROUND(
            100.0 * (total_impressions - LAG(total_impressions) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending))
            / NULLIF(LAG(total_impressions) OVER (PARTITION BY "Country", "Platform Granular" ORDER BY week_ending), 0)
        , 2) AS impressions_pct_change
    FROM weekly w
)
SELECT
    week_ending, "Country", "Platform Granular",
    post_count, total_impressions, avg_impressions_per_post,
    prev_post_count, prev_impressions,
    post_count_change, impressions_change, impressions_pct_change
FROM wow
ORDER BY "Country", "Platform Granular", week_ending DESC
"""


def run_query():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("Running WoW query...")
    df = pd.read_sql(QUERY, conn)
    conn.close()
    print(f"  → {len(df)} rows fetched")
    return df


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


def generate_html(df):
    data_blob = json.dumps(df_to_json(df), default=str)
    generated_at = datetime.now().strftime('%Y-%m-%d %H:%M')

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CORE — Week on Week Analysis</title>
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

.main{{padding:20px 28px;max-width:1600px;margin:0 auto}}

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
.kpi-row{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:20px}}
.kpi{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px}}
.kpi.blue::before{{background:var(--accent)}}.kpi.green::before{{background:var(--green)}}
.kpi.amber::before{{background:var(--amber)}}.kpi.red::before{{background:var(--red)}}
.kpi.purple::before{{background:var(--purple)}}.kpi.cyan::before{{background:var(--cyan)}}
.kpi-label{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin-bottom:6px}}
.kpi-val{{font-size:24px;font-weight:700;letter-spacing:-0.5px;font-feature-settings:'tnum' 1}}
.kpi-sub{{font-size:11px;color:var(--text-dim);margin-top:3px;font-family:'JetBrains Mono',monospace}}
.up{{color:var(--green)}}.down{{color:var(--red)}}.flat{{color:var(--text-muted)}}

/* Charts */
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:20px}}
.grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:20px}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}}
.card.full{{grid-column:1/-1}}
.card h3{{font-size:12px;font-weight:600;margin-bottom:2px}}
.card .sub{{font-size:10px;color:var(--text-dim);margin-bottom:12px}}
.cw{{position:relative;height:260px}}.cw.tall{{height:340px}}.cw.short{{height:220px}}

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
th.sa::after{{content:' ▲';font-size:8px}}th.sd::after{{content:' ▼';font-size:8px}}
td{{padding:8px 12px;border-bottom:1px solid var(--border);font-family:'JetBrains Mono',monospace;font-size:11px;white-space:nowrap}}
tr:hover td{{background:rgba(59,130,246,0.03)}}
td.pos{{color:var(--green)}}td.neg{{color:var(--red)}}
.pill{{display:inline-block;padding:2px 7px;border-radius:3px;font-size:10px;font-weight:600}}
.pill-g{{background:var(--green-bg);color:var(--green)}}
.pill-r{{background:var(--red-bg);color:var(--red)}}
.pill-a{{background:var(--amber-bg);color:var(--amber)}}
.anom-row td{{background:rgba(239,68,68,0.03)}}

@media(max-width:900px){{.grid2,.grid3{{grid-template-columns:1fr}}.kpi-row{{grid-template-columns:repeat(3,1fr)}}}}
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <div class="logo">WoW</div>
    <h1>CORE Pipeline <span>— Week on Week Analysis</span></h1>
  </div>
  <div style="display:flex;align-items:center;gap:10px">
    <span class="badge">{generated_at}</span>
    <span class="badge badge-green">Live DB</span>
  </div>
</div>

<div class="main">
  <div class="filter-bar">
    <label>Country</label><select id="f-co"><option value="ALL">All</option></select>
    <div class="filter-sep"></div>
    <label>Platform</label><select id="f-pl"><option value="ALL">All</option></select>
    <div class="filter-sep"></div>
    <label>Weeks</label>
    <select id="f-wk">
      <option value="1">Last 1 week</option>
      <option value="2">Last 2 weeks</option>
      <option value="4">Last 4 weeks</option>
      <option value="8" selected>Last 8 weeks</option>
      <option value="12">Last 12 weeks</option>
      <option value="26">Last 26 weeks</option>
      <option value="52">Last 52 weeks</option>
      <option value="0">All time</option>
    </select>
    <div class="filter-sep"></div>
    <label>Anomaly Threshold</label>
    <select id="f-th">
      <option value="20">20%</option>
      <option value="30" selected>30%</option>
      <option value="50">50%</option>
    </select>
    <div class="filter-sep"></div>
    <button class="btn btn-primary" onclick="go()">Apply</button>
    <button class="btn btn-ghost" onclick="exportCSV()">↓ CSV</button>
  </div>

  <!-- KPIs -->
  <div class="kpi-row" id="kpis"></div>

  <!-- Section: Aggregate WoW -->
  <div class="sh"><span class="dot" style="background:var(--accent)"></span>Aggregate Week on Week</div>
  <div class="grid2">
    <div class="card full"><h3>Total Impressions — WoW</h3><div class="sub">Line = total · Bars = week-over-week change</div><div class="cw tall"><canvas id="c1"></canvas></div></div>
    <div class="card"><h3>Post Count per Week</h3><div class="sub">Volume of posts published</div><div class="cw"><canvas id="c2"></canvas></div></div>
    <div class="card"><h3>Avg Impressions per Post</h3><div class="sub">Are individual posts getting stronger or weaker?</div><div class="cw"><canvas id="c3"></canvas></div></div>
  </div>

  <!-- Section: Platform Breakdown -->
  <div class="sh"><span class="dot" style="background:var(--purple)"></span>Breakdown by Platform Granular</div>
  <div class="grid2">
    <div class="card full"><h3>Impressions by Platform — WoW</h3><div class="sub">Stacked area · each platform's weekly contribution</div><div class="cw tall"><canvas id="c4"></canvas></div></div>
    <div class="card"><h3>WoW % Change by Platform</h3><div class="sub">Latest week vs previous · sorted by change</div><div class="cw"><canvas id="c5"></canvas></div></div>
    <div class="card"><h3>Platform Share — Latest Week</h3><div class="sub">Impression split</div><div class="cw"><canvas id="c6"></canvas></div></div>
  </div>

  <!-- Section: Country Breakdown -->
  <div class="sh"><span class="dot" style="background:var(--cyan)"></span>Breakdown by Country</div>
  <div class="grid2">
    <div class="card full"><h3>Impressions by Country — WoW</h3><div class="sub">Stacked area · each country's weekly contribution</div><div class="cw tall"><canvas id="c7"></canvas></div></div>
    <div class="card"><h3>WoW % Change by Country</h3><div class="sub">Latest week vs previous · sorted by change</div><div class="cw"><canvas id="c8"></canvas></div></div>
    <div class="card"><h3>Country Share — Latest Week</h3><div class="sub">Impression split</div><div class="cw"><canvas id="c9"></canvas></div></div>
  </div>

  <!-- Section: Anomalies -->
  <div class="sh"><span class="dot" style="background:var(--red)"></span>Anomaly Detection</div>
  <div class="kpi-row" id="anom-kpis" style="grid-template-columns:repeat(4,1fr)"></div>
  <div class="grid2">
    <div class="card full"><h3>Anomaly Timeline</h3><div class="sub">Red dots = drops · Green dots = spikes · size = magnitude</div><div class="cw tall"><canvas id="c10"></canvas></div></div>
  </div>
  <div class="tc"><div class="ts" id="anom-tbl"></div></div>

  <!-- Section: Full WoW Table -->
  <div class="sh"><span class="dot" style="background:var(--text-muted)"></span>Full WoW Data</div>
  <div class="tc"><div class="ts" id="raw-tbl"></div></div>
</div>

<script>
const RAW={data_blob};
const C=['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#84cc16','#f97316','#6366f1','#14b8a6','#e11d48','#0ea5e9','#a855f7','#22c55e','#64748b','#d946ef','#0891b2','#dc2626','#059669'];
let charts={{}},filtered=[];

function fmt(n){{if(n==null||isNaN(n))return'—';if(Math.abs(n)>=1e9)return(n/1e9).toFixed(1)+'B';if(Math.abs(n)>=1e6)return(n/1e6).toFixed(1)+'M';if(Math.abs(n)>=1e3)return(n/1e3).toFixed(1)+'K';return Math.round(n).toLocaleString()}}
function pct(n){{if(n==null||isNaN(n))return'—';return(n>=0?'+':'')+Number(n).toFixed(1)+'%'}}
function cc(n){{return n==null?'flat':n>=0?'up':'down'}}
function ic(n){{return n==null?'':n>=0?'▲':'▼'}}
function dc(id){{if(charts[id]){{charts[id].destroy();delete charts[id]}}}}

// ── Base chart config ──
function bOpts(yLbl,stacked){{
  return{{
    responsive:true,maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{
      legend:{{display:false}},
      tooltip:{{backgroundColor:'#1a2332',borderColor:'#2d3d52',borderWidth:1,titleFont:{{family:'DM Sans'}},bodyFont:{{family:'JetBrains Mono',size:11}},callbacks:{{label:ctx=>`${{ctx.dataset.label}}: ${{fmt(ctx.parsed.y)}}`}}}}
    }},
    scales:{{
      x:{{type:'time',time:{{unit:'week',displayFormats:{{week:'MMM d'}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',maxRotation:45}}}},
      y:{{stacked:!!stacked,title:{{display:true,text:yLbl,color:'#8492a6',font:{{family:'DM Sans',size:10}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>fmt(v)}}}}
    }}
  }}
}}

function catOpts(yLbl){{
  return{{
    responsive:true,maintainAspectRatio:false,indexAxis:'y',
    plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'#1a2332',borderColor:'#2d3d52',borderWidth:1,bodyFont:{{family:'JetBrains Mono',size:11}},callbacks:{{label:ctx=>pct(ctx.parsed.x)}}}}}},
    scales:{{
      x:{{grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>pct(v)}}}},
      y:{{grid:{{display:false}},ticks:{{font:{{family:'DM Sans',size:11}},color:'#8492a6'}}}}
    }}
  }}
}}

function pieOpts(){{
  return{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'right',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}},boxWidth:10,padding:6}}}},tooltip:{{callbacks:{{label:ctx=>`${{ctx.label}}: ${{fmt(ctx.parsed)}}`}}}}}}}}
}}

// ── Init ──
function init(){{
  const cos=[...new Set(RAW.map(r=>r.Country))].sort();
  const pls=[...new Set(RAW.map(r=>r['Platform Granular']))].sort();
  const cs=document.getElementById('f-co');
  cos.forEach(c=>{{const o=document.createElement('option');o.value=c;o.textContent=c;cs.appendChild(o)}});
  const ps=document.getElementById('f-pl');
  pls.forEach(p=>{{const o=document.createElement('option');o.value=p;o.textContent=p;ps.appendChild(o)}});
  go();
}}

function go(){{
  const co=document.getElementById('f-co').value;
  const pl=document.getElementById('f-pl').value;
  const wk=parseInt(document.getElementById('f-wk').value);

  filtered=RAW.filter(r=>{{
    if(co!=='ALL'&&r.Country!==co)return false;
    if(pl!=='ALL'&&r['Platform Granular']!==pl)return false;
    return true;
  }});

  if(wk>0){{
    const allW=[...new Set(filtered.map(r=>r.week_ending))].sort().reverse().slice(0,wk);
    const ws=new Set(allW);
    filtered=filtered.filter(r=>ws.has(r.week_ending));
  }}

  renderKPIs();renderAggregate();renderPlatform();renderCountry();renderAnomalies();renderRaw();
}}

// ── Week aggregation helpers ──
function aggByWeek(d){{
  const m={{}};
  d.forEach(r=>{{const w=r.week_ending;if(!m[w])m[w]={{w,imp:0,posts:0}};m[w].imp+=(r.total_impressions||0);m[w].posts+=(r.post_count||0)}});
  return Object.values(m).sort((a,b)=>a.w.localeCompare(b.w));
}}

function groupByDimWeek(d,dim){{
  const m={{}};
  d.forEach(r=>{{const k=r[dim]||'Unknown',w=r.week_ending;if(!m[k])m[k]={{}};m[k][w]=(m[k][w]||0)+(r.total_impressions||0)}});
  return m;
}}

function latestWowByDim(d,dim){{
  // For each dimension value, get latest 2 weeks and compute wow%
  const byDim={{}};
  d.forEach(r=>{{const k=r[dim]||'Unknown';if(!byDim[k])byDim[k]={{}};const w=r.week_ending;byDim[k][w]=(byDim[k][w]||0)+(r.total_impressions||0)}});
  const result=[];
  Object.entries(byDim).forEach(([k,wks])=>{{
    const sorted=Object.entries(wks).sort((a,b)=>b[0].localeCompare(a[0]));
    if(sorted.length>=2){{
      const curr=sorted[0][1],prev=sorted[1][1];
      const pctChg=prev>0?((curr-prev)/prev*100):null;
      result.push({{name:k,curr,prev,pctChg}});
    }}
  }});
  return result.sort((a,b)=>(a.pctChg||0)-(b.pctChg||0));
}}

// ══════ KPIs ══════
function renderKPIs(){{
  const agg=aggByWeek(filtered);
  const totalImp=agg.reduce((s,r)=>s+r.imp,0);
  const totalPosts=agg.reduce((s,r)=>s+r.posts,0);
  const avgImp=totalPosts>0?totalImp/totalPosts:0;
  const L=agg.length>=1?agg[agg.length-1]:null;
  const P=agg.length>=2?agg[agg.length-2]:null;
  let iChg=null,pChg=null,aChg=null;
  if(L&&P){{
    iChg=P.imp>0?((L.imp-P.imp)/P.imp*100):null;
    pChg=P.posts>0?((L.posts-P.posts)/P.posts*100):null;
    const aL=L.posts>0?L.imp/L.posts:0,aP=P.posts>0?P.imp/P.posts:0;
    aChg=aP>0?((aL-aP)/aP*100):null;
  }}
  const th=parseInt(document.getElementById('f-th').value);
  const anoms=filtered.filter(r=>r.impressions_pct_change!=null&&Math.abs(r.impressions_pct_change)>th);

  document.getElementById('kpis').innerHTML=`
    <div class="kpi blue"><div class="kpi-label">Total Impressions</div><div class="kpi-val">${{fmt(totalImp)}}</div><div class="kpi-sub">${{agg.length}} weeks</div></div>
    <div class="kpi green"><div class="kpi-label">Latest Week Imp.</div><div class="kpi-val">${{L?fmt(L.imp):'—'}}</div><div class="kpi-sub ${{cc(iChg)}}">${{ic(iChg)}} ${{pct(iChg)}} WoW</div></div>
    <div class="kpi amber"><div class="kpi-label">Latest Week Posts</div><div class="kpi-val">${{L?fmt(L.posts):'—'}}</div><div class="kpi-sub ${{cc(pChg)}}">${{ic(pChg)}} ${{pct(pChg)}} WoW</div></div>
    <div class="kpi purple"><div class="kpi-label">Avg Imp / Post</div><div class="kpi-val">${{fmt(avgImp)}}</div><div class="kpi-sub ${{cc(aChg)}}">${{ic(aChg)}} ${{pct(aChg)}} WoW</div></div>
    <div class="kpi red"><div class="kpi-label">Anomalies Flagged</div><div class="kpi-val">${{anoms.length}}</div><div class="kpi-sub">threshold: ${{th}}%</div></div>
    <div class="kpi cyan"><div class="kpi-label">Scope</div><div class="kpi-val">${{new Set(filtered.map(r=>r.Country)).size}}C · ${{new Set(filtered.map(r=>r['Platform Granular'])).size}}P</div><div class="kpi-sub">${{fmt(totalPosts)}} posts</div></div>
  `;
}}

// ══════ AGGREGATE ══════
function renderAggregate(){{
  const agg=aggByWeek(filtered);

  dc('c1');
  const chgs=agg.map((w,i)=>i===0?0:w.imp-agg[i-1].imp);
  charts.c1=new Chart(document.getElementById('c1'),{{
    type:'bar',
    data:{{labels:agg.map(w=>new Date(w.w)),datasets:[
      {{type:'line',label:'Impressions',data:agg.map(w=>w.imp),borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.08)',fill:true,tension:0.3,pointRadius:3,pointBackgroundColor:'#3b82f6',yAxisID:'y'}},
      {{type:'bar',label:'WoW Change',data:chgs,backgroundColor:chgs.map(v=>v>=0?'rgba(16,185,129,0.35)':'rgba(239,68,68,0.35)'),borderColor:chgs.map(v=>v>=0?'#10b981':'#ef4444'),borderWidth:1,yAxisID:'y1'}}
    ]}},
    options:{{
      ...bOpts('Impressions'),
      plugins:{{...bOpts('').plugins,legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}}}},
      scales:{{...bOpts('').scales,y1:{{position:'right',title:{{display:true,text:'WoW Change',color:'#8492a6',font:{{family:'DM Sans',size:10}}}},grid:{{display:false}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>fmt(v)}}}}}}
    }}
  }});

  dc('c2');
  charts.c2=new Chart(document.getElementById('c2'),{{type:'bar',data:{{labels:agg.map(w=>new Date(w.w)),datasets:[{{label:'Posts',data:agg.map(w=>w.posts),backgroundColor:'rgba(139,92,246,0.4)',borderColor:'#8b5cf6',borderWidth:1,borderRadius:3}}]}},options:bOpts('Posts')}});

  dc('c3');
  charts.c3=new Chart(document.getElementById('c3'),{{type:'line',data:{{labels:agg.map(w=>new Date(w.w)),datasets:[{{label:'Avg/Post',data:agg.map(w=>w.posts>0?w.imp/w.posts:0),borderColor:'#f59e0b',backgroundColor:'rgba(245,158,11,0.08)',fill:true,tension:0.3,pointRadius:3,pointBackgroundColor:'#f59e0b'}}]}},options:bOpts('Avg Impressions')}});
}}

// ══════ PLATFORM BREAKDOWN ══════
function renderPlatform(){{
  const byPlat=groupByDimWeek(filtered,'Platform Granular');
  const allWks=[...new Set(filtered.map(r=>r.week_ending))].sort();
  const names=Object.keys(byPlat).sort();

  dc('c4');
  charts.c4=new Chart(document.getElementById('c4'),{{type:'line',data:{{labels:allWks.map(w=>new Date(w)),datasets:names.map((n,i)=>({{label:n,data:allWks.map(w=>byPlat[n][w]||0),borderColor:C[i%C.length],backgroundColor:C[i%C.length]+'22',fill:true,tension:0.3,pointRadius:0}}))}},options:{{...bOpts('Impressions',true),plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:6}}}}}}}}}});

  // WoW % by platform
  dc('c5');
  const wow=latestWowByDim(filtered,'Platform Granular');
  charts.c5=new Chart(document.getElementById('c5'),{{type:'bar',data:{{labels:wow.map(r=>r.name),datasets:[{{label:'WoW %',data:wow.map(r=>r.pctChg),backgroundColor:wow.map(r=>(r.pctChg||0)>=0?'rgba(16,185,129,0.4)':'rgba(239,68,68,0.4)'),borderColor:wow.map(r=>(r.pctChg||0)>=0?'#10b981':'#ef4444'),borderWidth:1,borderRadius:3}}]}},options:catOpts('WoW %')}});

  // Pie
  dc('c6');
  const latestWk=allWks[allWks.length-1];
  charts.c6=new Chart(document.getElementById('c6'),{{type:'doughnut',data:{{labels:names,datasets:[{{data:names.map(n=>byPlat[n][latestWk]||0),backgroundColor:C.slice(0,names.length),borderColor:'#111827',borderWidth:2}}]}},options:pieOpts()}});
}}

// ══════ COUNTRY BREAKDOWN ══════
function renderCountry(){{
  const byCo=groupByDimWeek(filtered,'Country');
  const allWks=[...new Set(filtered.map(r=>r.week_ending))].sort();
  const names=Object.keys(byCo).sort();

  dc('c7');
  charts.c7=new Chart(document.getElementById('c7'),{{type:'line',data:{{labels:allWks.map(w=>new Date(w)),datasets:names.map((n,i)=>({{label:n,data:allWks.map(w=>byCo[n][w]||0),borderColor:C[(i+7)%C.length],backgroundColor:C[(i+7)%C.length]+'22',fill:true,tension:0.3,pointRadius:0}}))}},options:{{...bOpts('Impressions',true),plugins:{{legend:{{display:true,position:'top',labels:{{color:'#8492a6',font:{{family:'DM Sans',size:9}},boxWidth:10,padding:6}}}}}}}}}});

  dc('c8');
  const wow=latestWowByDim(filtered,'Country');
  charts.c8=new Chart(document.getElementById('c8'),{{type:'bar',data:{{labels:wow.map(r=>r.name),datasets:[{{label:'WoW %',data:wow.map(r=>r.pctChg),backgroundColor:wow.map(r=>(r.pctChg||0)>=0?'rgba(16,185,129,0.4)':'rgba(239,68,68,0.4)'),borderColor:wow.map(r=>(r.pctChg||0)>=0?'#10b981':'#ef4444'),borderWidth:1,borderRadius:3}}]}},options:catOpts('WoW %')}});

  dc('c9');
  const latestWk=allWks[allWks.length-1];
  charts.c9=new Chart(document.getElementById('c9'),{{type:'doughnut',data:{{labels:names,datasets:[{{data:names.map(n=>byCo[n][latestWk]||0),backgroundColor:C.slice(7,7+names.length),borderColor:'#111827',borderWidth:2}}]}},options:pieOpts()}});
}}

// ══════ ANOMALIES ══════
function renderAnomalies(){{
  const th=parseInt(document.getElementById('f-th').value);
  const anoms=filtered.filter(r=>r.impressions_pct_change!=null&&Math.abs(r.impressions_pct_change)>th).sort((a,b)=>Math.abs(b.impressions_pct_change)-Math.abs(a.impressions_pct_change));
  const drops=anoms.filter(r=>r.impressions_pct_change<0);
  const spikes=anoms.filter(r=>r.impressions_pct_change>0);

  document.getElementById('anom-kpis').innerHTML=`
    <div class="kpi red"><div class="kpi-label">Total Drops</div><div class="kpi-val">${{drops.length}}</div></div>
    <div class="kpi green"><div class="kpi-label">Total Spikes</div><div class="kpi-val">${{spikes.length}}</div></div>
    <div class="kpi amber"><div class="kpi-label">Worst Drop</div><div class="kpi-val">${{drops.length?pct(drops[0].impressions_pct_change):'—'}}</div><div class="kpi-sub">${{drops.length?drops[0].Country+' · '+drops[0]['Platform Granular']:''}}</div></div>
    <div class="kpi purple"><div class="kpi-label">Biggest Spike</div><div class="kpi-val">${{spikes.length?pct(spikes[0].impressions_pct_change):'—'}}</div><div class="kpi-sub">${{spikes.length?spikes[0].Country+' · '+spikes[0]['Platform Granular']:''}}</div></div>
  `;

  // Scatter timeline
  dc('c10');
  const dropPts=drops.map(r=>({{x:r.week_ending,y:r.impressions_pct_change,r:Math.min(Math.abs(r.impressions_pct_change)/5,20),label:r.Country+' · '+r['Platform Granular']}}));
  const spikePts=spikes.map(r=>({{x:r.week_ending,y:r.impressions_pct_change,r:Math.min(Math.abs(r.impressions_pct_change)/5,20),label:r.Country+' · '+r['Platform Granular']}}));

  charts.c10=new Chart(document.getElementById('c10'),{{
    type:'bubble',
    data:{{
      datasets:[
        {{label:'Drops',data:dropPts,backgroundColor:'rgba(239,68,68,0.35)',borderColor:'#ef4444',borderWidth:1}},
        {{label:'Spikes',data:spikePts,backgroundColor:'rgba(16,185,129,0.35)',borderColor:'#10b981',borderWidth:1}}
      ]
    }},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{
        legend:{{display:true,labels:{{color:'#8492a6',font:{{family:'DM Sans',size:10}}}}}},
        tooltip:{{backgroundColor:'#1a2332',borderColor:'#2d3d52',borderWidth:1,bodyFont:{{family:'JetBrains Mono',size:11}},callbacks:{{label:ctx=>{{const p=ctx.raw;return`${{p.label}}: ${{pct(p.y)}}`}}}}}}
      }},
      scales:{{
        x:{{type:'time',time:{{unit:'week',displayFormats:{{week:'MMM d'}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568'}}}},
        y:{{title:{{display:true,text:'WoW % Change',color:'#8492a6',font:{{family:'DM Sans',size:10}}}},grid:{{color:'rgba(255,255,255,0.04)'}},ticks:{{font:{{family:'JetBrains Mono',size:9}},color:'#4a5568',callback:v=>pct(v)}}}}
      }}
    }}
  }});

  // Anomaly table
  let h='<table><thead><tr><th>Week Ending</th><th>Country</th><th>Platform</th><th>Posts</th><th>Impressions</th><th>Prev Week</th><th>Change</th><th>WoW %</th><th>Status</th></tr></thead><tbody>';
  anoms.forEach(r=>{{
    const c=r.impressions_pct_change<0?'neg':'pos';
    const pill=r.impressions_pct_change<-50?'pill-r':r.impressions_pct_change<0?'pill-a':'pill-g';
    const lbl=r.impressions_pct_change<-50?'CRITICAL':r.impressions_pct_change<0?'DROP':'SPIKE';
    h+=`<tr class="${{r.impressions_pct_change<-50?'anom-row':''}}"><td>${{r.week_ending}}</td><td style="font-family:DM Sans">${{r.Country}}</td><td style="font-family:DM Sans">${{r['Platform Granular']}}</td><td>${{r.post_count}}</td><td>${{fmt(r.total_impressions)}}</td><td>${{fmt(r.prev_impressions)}}</td><td class="${{c}}">${{fmt(r.impressions_change)}}</td><td class="${{c}}">${{pct(r.impressions_pct_change)}}</td><td><span class="pill ${{pill}}">${{lbl}}</span></td></tr>`;
  }});
  h+='</tbody></table>';
  document.getElementById('anom-tbl').innerHTML=h;
}}

// ══════ RAW TABLE ══════
let sortC=null,sortD='desc';
function renderRaw(){{
  const cols=[
    {{k:'week_ending',l:'Week Ending'}},{{k:'Country',l:'Country'}},{{k:'Platform Granular',l:'Platform'}},
    {{k:'post_count',l:'Posts'}},{{k:'total_impressions',l:'Impressions'}},{{k:'avg_impressions_per_post',l:'Avg/Post'}},
    {{k:'prev_impressions',l:'Prev Imp'}},{{k:'impressions_change',l:'Change'}},{{k:'impressions_pct_change',l:'WoW %'}}
  ];
  let data=[...filtered];
  if(sortC){{data.sort((a,b)=>{{let va=a[sortC],vb=b[sortC];if(va==null)return 1;if(vb==null)return-1;if(typeof va==='string')return sortD==='asc'?va.localeCompare(vb):vb.localeCompare(va);return sortD==='asc'?va-vb:vb-va}})}}

  let h='<table><thead><tr>';
  cols.forEach(c=>{{const s=sortC===c.k;h+=`<th class="${{s?(sortD==='asc'?'sa':'sd'):''}}" onclick="doSort('${{c.k}}')">${{c.l}}</th>`}});
  h+='</tr></thead><tbody>';
  data.forEach(r=>{{
    h+='<tr>';
    cols.forEach(c=>{{
      const v=r[c.k];const isC=c.k.includes('change');
      const cls=isC&&v!=null?(v>0?'pos':v<0?'neg':''):'';
      const txt=c.k.includes('pct')?pct(v):typeof v==='number'?fmt(v):(v||'—');
      h+=`<td class="${{cls}}" style="${{!isC&&typeof v!=='number'?'font-family:DM Sans':''}}">${{txt}}</td>`;
    }});
    h+='</tr>';
  }});
  h+='</tbody></table>';
  document.getElementById('raw-tbl').innerHTML=h;
}}
function doSort(c){{if(sortC===c)sortD=sortD==='asc'?'desc':'asc';else{{sortC=c;sortD='desc'}};renderRaw()}}

// ══════ EXPORT ══════
function exportCSV(){{
  const hs=['week_ending','Country','Platform Granular','post_count','total_impressions','avg_impressions_per_post','prev_impressions','impressions_change','impressions_pct_change'];
  let csv=hs.join(',')+`\\n`;
  filtered.forEach(r=>{{csv+=hs.map(h=>r[h]??'').join(',')+`\\n`}});
  const b=new Blob([csv],{{type:'text/csv'}});const a=document.createElement('a');
  a.href=URL.createObjectURL(b);a.download=`wow_analysis_${{new Date().toISOString().slice(0,10)}}.csv`;a.click();
}}

init();
</script>
</body>
</html>"""
    return html


if __name__ == '__main__':
    print("=" * 60)
    print("  CORE — Week on Week Dashboard Generator")
    print("=" * 60)
    print()

    df = run_query()

    print("\nGenerating dashboard...")
    html = generate_html(df)

    output_file = 'index.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ Dashboard saved to: {output_file}")
    print(f"   Open in browser or push to GitHub Pages")
    print(f"   {len(df)} rows · {df['Country'].nunique()} countries · {df['Platform Granular'].nunique()} platforms")