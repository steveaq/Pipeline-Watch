from pathlib import Path
import html
import json
import uuid
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pipeline import (ROOT, MONTHS, RULES, initialise, run_partition, fetch_source,
                      read_table, safe_query, dbt_build)

st.set_page_config(page_title='Pipeline Watch · Every row accounted for',page_icon='▦',layout='wide',initial_sidebar_state='collapsed')
st.markdown(f'<style>{(ROOT/"styles.css").read_text()}</style>',unsafe_allow_html=True)
MINT='#9fe8c8'; AMBER='#e8bd79'; RED='#ed9595'; BLUE='#89b9d6'


def raw(text): st.markdown(text,unsafe_allow_html=True)
def esc(value): return html.escape(str(value),quote=True)


def plot(fig,height=280):
    fig.update_layout(height=height,margin=dict(l=12,r=12,t=18,b=15),paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',font=dict(family='Arial, sans-serif',size=11,color='#aabbb8'),
        xaxis=dict(showgrid=False,zeroline=False),yaxis=dict(gridcolor='#263638',zeroline=False),
        legend=dict(orientation='h',y=1.13,x=0),hovermode='x unified')
    st.plotly_chart(fig,width='stretch',config={'displayModeBar':False})


def bootstrap():
    if 'warehouse_path' not in st.session_state:
        st.session_state.warehouse_path=str(ROOT/'runtime/sessions'/uuid.uuid4().hex/'warehouse.duckdb')
    path=Path(st.session_state.warehouse_path)
    if not path.exists():
        with st.spinner('Building your isolated warehouse from the bundled TLC snapshots…'):
            for month in MONTHS:
                result=run_partition(ROOT/'data/bundled'/f'green_tripdata_{month}.parquet',month,path,
                    source_url='bundled:NYC-TLC-row-capped-snapshot')
                if result['status']=='failed':
                    st.error('Warehouse bootstrap failed: '+result['error']);st.stop()
    return path


def main():
    database=bootstrap()
    runs=read_table(database,'SELECT * FROM pipeline_runs ORDER BY started_at DESC')
    latest=read_table(database,'SELECT r.* FROM pipeline_runs r JOIN partitions p ON r.run_id=p.run_id ORDER BY r.month')
    source=int(latest.source_rows.sum()); rejected=int(latest.rejected_rows.sum()); duplicates=int(latest.duplicate_rows.sum())
    silver=int(read_table(database,'SELECT count(*) AS n FROM silver_trips').n.iloc[0])
    gold=int(read_table(database,'SELECT count(*) AS n FROM zone_daily').n.iloc[0])
    count=len(latest); recent=runs.iloc[0]
    mode='FULL SOURCE' if latest['mode'].eq('live').all() else ('BUNDLED SNAPSHOTS' if latest['mode'].eq('bundled').all() else 'MIXED SOURCES')
    failed=recent['status']=='failed'
    raw('<nav><div class="brand"><span>▦</span> PIPELINE<span class="brand-light">WATCH</span><span class="version">/ BATCH OPS</span></div><span class="workspace"><i></i> LOCAL WAREHOUSE</span></nav>')
    raw(f'''<section class="hero"><div><div class="eyebrow">OBSERVABILITY FOR THE DATA BETWEEN SOURCE AND STORY</div>
    <h1>Every row.<br> <em>Accounted for.</em></h1><p>A real batch pipeline, with the receipts. Follow NYC taxi data<br> from immutable source files to tested, queryable tables.</p>
    <div class="hero-tags"><span>PYTHON</span><b>→</b><span>DUCKDB</span><b>→</b><span>DBT</span><b>→</b><span>INSIGHT</span></div></div>
    <aside class="terminal"><div class="terminal-top"><span><i></i><i></i><i></i></span><small>WAREHOUSE / STATUS</small></div>
    <div class="terminal-line"><span>source</span><b>nyc_tlc.green_trips</b></div><div class="terminal-line"><span>partitions</span><b>{count:02} / {len(MONTHS):02} published</b></div>
    <div class="terminal-line"><span>rows published</span><b>{silver:,}</b></div><div class="terminal-line"><span>execution</span><b>{mode.lower()}</b></div>
    <div class="terminal-status {'status-fail' if failed else ''}"><i></i>{'LAST ATTEMPT FAILED · DATA PRESERVED' if failed else 'PUBLISHED WITH QUALITY WARNINGS' if rejected else 'ALL CHECKS CLEAR'}</div>
    <small class="terminal-foot">RUN {esc(recent.run_id)} / {recent.finished_at:%H:%M:%S} UTC</small></aside></section>''')
    raw('<div class="section-title"><span>01 / CONTROL PLANE</span><small>EXPLICIT INPUTS. REPLAYABLE RUNS.</small></div>')
    with st.container(border=True):
        a,b,c=st.columns([1,1.6,1])
        with a: month=st.selectbox('Partition to run',MONTHS,index=2)
        with b: source_mode=st.selectbox('Input source',['Bundled snapshot · 5,000 rows','Full TLC source · download & validate'])
        with c:
            st.markdown('<div class="button-spacer"></div>',unsafe_allow_html=True)
            execute=st.button('Run partition ↗',type='primary',width='stretch')
        st.caption('Bundled mode uses the first 5,000 source rows per month. Full source downloads the official monthly file. Runs replace one partition atomically; each browser session gets an isolated warehouse.')
    if execute:
        with st.spinner('Ingesting, validating and publishing the selected partition…'):
            try:
                if source_mode.startswith('Full'):
                    path,url=fetch_source(month,database.parent/'downloads'); execution_mode='live'
                else:
                    path=ROOT/'data/bundled'/f'green_tripdata_{month}.parquet';url='bundled:NYC-TLC-row-capped-snapshot';execution_mode='bundled'
                result=run_partition(path,month,database,execution_mode,url)
                st.session_state.last_result={k:str(v) for k,v in result.items()}
                st.rerun()
            except Exception:
                st.error('The source download could not complete. No published partition was changed; try again later.')
    last_result=st.session_state.get('last_result')
    if last_result:
        if last_result['status']=='failed':
            st.error(f'Run {last_result["run_id"]} failed: {last_result["error"]} Published partitions are preserved.')
        else:
            st.success(f'Run {last_result["run_id"]} published {int(last_result["published_rows"]):,} rows. Replaced {int(last_result["previous_rows"]):,} existing rows; quarantined {int(last_result["rejected_rows"]):,}.')
    stages=[('01','SOURCE',source,'rows in current partitions','blue'),('02','QUARANTINE',rejected,'invalid source rows','amber'),
            ('03','SILVER',silver,'validated, unique trips','mint'),('04','GOLD',gold,'zone × day aggregates','mint')]
    for col,(number,name,value,detail,tone) in zip(st.columns(4),stages):
        with col: raw(f'<div class="stage {tone}"><div><span>{number}</span><b>{name}</b><i>↗</i></div><strong>{value:,}</strong><small>{detail}</small></div>')
    monitor,quality,lineage,lab=st.tabs(['Run monitor','Quality desk','Lineage & dbt','SQL workbench'])
    with monitor:
        left,right=st.columns([1.8,1],gap='large')
        with left:
            raw('<div class="panel-heading"><h2>Nothing disappears in transit.</h2><span>ROW CONSERVATION</span></div>')
            chart=go.Figure(go.Sankey(arrangement='fixed',node=dict(pad=20,thickness=16,color=[BLUE,MINT,AMBER,RED],
                label=['Source rows','Published trips','Quarantined','Duplicate copies'],x=[0,.99,.99,.99],y=[.5,.2,.7,.95],
                line=dict(width=0)),link=dict(source=[0,0,0],target=[1,2,3],value=[silver,rejected,duplicates],
                color=['rgba(159,232,200,.25)','rgba(232,189,121,.25)','rgba(237,149,149,.25)'])))
            plot(chart,270)
            st.caption(f'{source:,} input = {silver:,} published + {rejected:,} quarantined + {duplicates:,} duplicate copies. Counts describe the currently published partitions, not accumulated replay attempts.')
        with right:
            raw(f'''<aside class="ops-note"><span class="eyebrow">THE INVARIANT</span><h3>Replay the month.<br> Keep the truth.</h3><p>Every successful run replaces its month inside one database transaction. Failed schema checks leave published data intact.</p>
                <div class="note-line"></div><span class="big-ratio">{silver/source:.1%}</span><small>INPUT ROWS ACCEPTED</small><p>Rejected rows stay inspectable. They are not silently dropped or filled in.</p></aside>''')
        st.subheader('The run ledger')
        ledger=runs[['run_id','month','mode','status','source_rows','published_rows','rejected_rows','previous_rows','started_at']].copy()
        ledger['started_at']=ledger.started_at.dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        st.dataframe(ledger,hide_index=True,width='stretch')
        st.download_button('Export run ledger ↓',ledger.to_csv(index=False),'pipeline_watch_runs.csv','text/csv')
        st.subheader('Published activity')
        daily=read_table(database,'SELECT day,sum(trip_count) AS trips FROM zone_daily GROUP BY day ORDER BY day')
        fig=go.Figure(go.Bar(x=daily.day,y=daily.trips,marker_color=MINT,name='Trips'))
        plot(fig,240)
        st.caption('A row-capped snapshot may cover only part of a month. This is a pipeline demonstration, not a representative estimate of citywide demand.')
    with quality:
        st.subheader('A warning should come with evidence.')
        checks=read_table(database,'''SELECT q.check_name,sum(q.failed_rows) AS failed_rows,sum(q.checked_rows) AS checked_rows
            FROM quality_checks q JOIN partitions p ON p.run_id=q.run_id GROUP BY 1 ORDER BY failed_rows DESC''')
        checks['rule']=checks.check_name.map(RULES)
        checks['pass_rate']=1-checks.failed_rows/checks.checked_rows
        st.dataframe(checks[['rule','failed_rows','checked_rows','pass_rate']],hide_index=True,width='stretch',
            column_config={'pass_rate':st.column_config.NumberColumn('Pass rate',format='percent')})
        st.caption('A row can fail more than one check, so check failure counts overlap. Quarantine counts each rejected source row once. Bounds are explicit demo policies, not TLC-certified error labels.')
        st.subheader('Inspect the quarantine')
        selected_month=st.selectbox('Quarantine partition',MONTHS)
        rejects=read_table(database,'''SELECT q.source_row,q.reasons,q.payload FROM quarantine q
            JOIN partitions p ON p.run_id=q.run_id WHERE q.month=? ORDER BY source_row LIMIT 200''',[selected_month])
        st.dataframe(rejects[['source_row','reasons']],hide_index=True,width='stretch')
        if not rejects.empty:
            selected=st.selectbox('Source row to inspect',range(len(rejects)),format_func=lambda i:str(rejects.iloc[i].source_row))
            st.json(json.loads(rejects.iloc[selected].payload),expanded=False)
            st.download_button('Export visible quarantine ↓',rejects.to_csv(index=False),'quarantine_first_200.csv','text/csv')
        else: st.info('No quarantined rows in this published partition.')
        with st.expander('Run a safe schema-failure drill'):
            st.write('This creates a temporary copy of a bundled file with trip_distance removed. The attempt should fail, appear in the ledger, and leave the published partition unchanged.')
            if st.button('Run schema-failure drill'):
                fixture=pd.read_parquet(ROOT/'data/bundled'/f'green_tripdata_{month}.parquet').drop(columns=['trip_distance'])
                path=database.parent/'schema_failure_drill.parquet';fixture.to_parquet(path,index=False)
                result=run_partition(path,month,database,'test','local:schema-failure-drill')
                st.session_state.last_result={k:str(v) for k,v in result.items()};st.rerun()
    with lineage:
        st.subheader('The shortest route from file to fact.')
        for number,title,detail,metric in [('01','Bronze / immutable source','Checksum-addressed Parquet copies; source schema and SHA-256 stored with each run.',f'{count} current partitions'),
            ('02','Silver / validated trips','Explicit timestamp, month, distance, money and zone checks; canonical row hashes.',f'{silver:,} accepted trips'),
            ('03','Gold / zone_daily','The same SQL model powers the Python publication and the dbt materialisation.',f'{gold:,} aggregates'),
            ('04','Serving / observability','Run ledger, quality evidence, lineage and read-only SQL. Every number comes from DuckDB.',f'{len(runs)} audited attempts')]:
            raw(f'<div class="lineage-row"><span>{number}</span><div><h3>{title}</h3><p>{detail}</p></div><b>{metric}</b></div>')
        st.subheader('Exercise the dbt project')
        st.write('Build zone_daily from the published silver table and run dbt’s schema and duplicate-identity tests against this session’s warehouse.')
        if st.button('Run dbt build ↗'):
            with st.spinner('Building the model and running dbt tests…'):
                try:
                    code,log=dbt_build(database)
                    st.session_state.dbt_result={'code':code,'log':log}
                except Exception as exc: st.error(str(exc))
        if 'dbt_result' in st.session_state:
            result=st.session_state.dbt_result
            (st.success if result['code']==0 else st.error)('dbt build passed.' if result['code']==0 else 'dbt build failed. Inspect the log below.')
            with st.expander('dbt execution log'): st.code(result['log'][-16000:])
        with st.expander('Source fingerprints and schema drift'):
            st.dataframe(latest[['month','mode','source_url','source_sha256','drift_json']],hide_index=True,width='stretch')
            manifest=json.loads((ROOT/'data/bundled/manifest.json').read_text());st.json(manifest,expanded=False)
    with lab:
        st.subheader('Ask the warehouse.')
        st.caption('Read-only SQL · external file/network access disabled · one SELECT statement · maximum 500 result rows. Tables: silver_trips, zone_daily, taxi_zones, pipeline_runs, partitions, quality_checks, quarantine.')
        query=st.text_area('SQL query',value='SELECT zone, borough, sum(trip_count) AS trips,\n       round(sum(total_revenue), 2) AS revenue\nFROM zone_daily\nGROUP BY zone, borough\nORDER BY trips DESC\nLIMIT 20',height=170)
        if st.button('Execute query ↗'):
            try:
                result=safe_query(database,query)
                st.session_state.query_result=result
                st.session_state.query_sql=query
            except Exception as exc: st.error('Query rejected: '+str(exc)[:300])
        if 'query_result' in st.session_state:
            st.caption('Results for: '+st.session_state.query_sql.replace('\n',' ')[:150])
            st.dataframe(st.session_state.query_result,hide_index=True,width='stretch')
            st.download_button('Download query result ↓',st.session_state.query_result.to_csv(index=False),'query_result.csv','text/csv')
    with st.expander('About the data and this environment'):
        st.markdown('[Official NYC TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The bundled January–March 2025 green-taxi snapshots contain 5,000 rows each, in source order; provenance records include full-file and snapshot hashes. Full source mode is a real download and ingestion.')
        st.write('Each browser session has an isolated local DuckDB warehouse, created on first load. Filesystem state is temporary on most hosts. Use the CLI with a persistent disk for durable operation. This portfolio app has no production scheduler, authentication or multi-user shared warehouse.')
        st.code('python pipeline.py demo\npython pipeline.py run --month 2025-03 --mode live\npython pipeline.py dbt',language='bash')
    raw('<footer><b>PIPELINE WATCH</b><span>BUILD TRUST AT EVERY TRANSFORMATION.</span><span>PUBLIC TLC DATA / REAL EXECUTIONS</span></footer>')


if __name__=='__main__': main()
