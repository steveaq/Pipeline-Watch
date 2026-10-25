import json
from pathlib import Path
import time
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import duckdb
from pipeline import ROOT, MONTHS, COLUMNS, RULES, read_table, run_partition, fetch_source, safe_query, dbt_build
from ops_analytics import current_runs, run_metrics, quality_matrix, rejection_profile, schema_changes, partition_profile

MINT='#9fe8c8';AMBER='#e8bd79';RED='#ed9595';BLUE='#89b9d6'


def database():return Path(st.session_state.warehouse_path)
def title(heading,copy):st.title(heading);st.write(copy)
def latest():return current_runs(database())
def ledger():return read_table(database(),'SELECT * FROM pipeline_runs ORDER BY started_at DESC')


def plot(fig,height=310):
    fig.update_layout(height=height,margin=dict(l=12,r=12,t=25,b=20),paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',font=dict(family='Arial, sans-serif',size=12,color='#bfd0cb'),
        xaxis=dict(showgrid=False,zeroline=False,automargin=True),yaxis=dict(gridcolor='#2c4142',zeroline=False,automargin=True),
        legend=dict(orientation='h',y=1.14,x=0),hovermode='x unified')
    st.plotly_chart(fig,width='stretch',config={'displayModeBar':False})


def count(sql,params=None):return int(read_table(database(),sql,params).iloc[0,0])


def overview():
    current=latest();runs=ledger()
    source=int(current.source_rows.sum());accepted=count('SELECT count(*) FROM silver_trips')
    rejects=int(current.rejected_rows.sum());duplicates=int(current.duplicate_rows.sum())
    title('Pipeline overview','The counts below describe the currently published partitions. The run history also includes replays and failures, which are kept separately so they do not inflate the totals.')
    for col,label,value in zip(st.columns(4),['Source rows','Published trips','Quarantined rows','Gold aggregates'],[source,accepted,rejects,count('SELECT count(*) FROM zone_daily')]):col.metric(label,f'{value:,}')
    if runs.iloc[0].status=='failed':st.warning('The latest attempt failed. The published partitions still refer to the last successful runs; inspect the failure on Runs and replay.')
    st.subheader('Where the source rows went')
    fig=go.Figure(go.Sankey(node=dict(pad=20,thickness=18,label=['Source','Published','Quarantined','Duplicates'],color=[BLUE,MINT,AMBER,RED]),
        link=dict(source=[0,0,0],target=[1,2,3],value=[accepted,rejects,duplicates],color=['rgba(159,232,200,.3)','rgba(232,189,121,.3)','rgba(237,149,149,.3)'])))
    plot(fig,300)
    st.caption(f'{source:,} source rows = {accepted:,} unique accepted rows + {rejects:,} rejected rows + {duplicates:,} valid duplicate copies. A rejected row can violate several rules but is counted once here.')
    a,b=st.columns(2)
    with a:
        st.subheader('Acceptance by partition')
        fig=go.Figure()
        for column,name,color in [('published_rows','Published',MINT),('rejected_rows','Quarantined',AMBER),('duplicate_rows','Duplicates',RED)]:fig.add_trace(go.Bar(x=current.month,y=current[column],name=name,marker_color=color))
        fig.update_layout(barmode='stack');plot(fig,290)
    with b:
        st.subheader('Most recent processing times')
        metrics=run_metrics(runs.head(20)).sort_values('started_at')
        fig=go.Figure(go.Bar(x=metrics.run_id,y=metrics.duration_seconds,marker_color=[RED if s=='failed' else MINT for s in metrics.status],text=metrics.month))
        fig.update_yaxes(title='Seconds');plot(fig,290)
        st.caption('Time runs from the audit start to completion. Downloads and database initialisation happen before this timer. Three quick runs are not a reliable performance benchmark.')
    st.subheader('Published partitions')
    st.dataframe(current[['month','mode','source_rows','published_rows','rejected_rows','finished_at','run_id']],hide_index=True,width='stretch')
    st.caption('A warning means some records failed the configured rules, duplicates were removed, or schema drift was recorded. It does not mean publication failed.')
    st.subheader('Storage in this session')
    files=list((database().parent/'bronze').rglob('*.parquet'))
    a,b,c=st.columns(3)
    a.metric('Database size',f'{database().stat().st_size/1024/1024:.2f} MiB')
    b.metric('Bronze files',len(files));c.metric('Bronze bytes',f'{sum(f.stat().st_size for f in files)/1024/1024:.2f} MiB')
    st.caption('Bronze is addressed by file checksum. Identical replays reuse the same source copy, while changed files and failed schema-drill inputs remain available for inspection. Host filesystem state may be temporary.')


def runs():
    title('Runs and replay','Publish a month, then replay it to check that the row count stays stable. Each attempt gets a run ID; successful attempts replace that month in one transaction.')
    with st.container(border=True):
        a,b,c=st.columns([1,1.8,1])
        with a:month=st.selectbox('Month',MONTHS,index=2)
        with b:mode=st.selectbox('Source',['Bundled: first 5,000 rows','Full TLC monthly file'])
        with c:execute=st.button('Run partition',type='primary',width='stretch')
    if execute:
        try:
            with st.spinner('Loading and validating the partition...'):
                if mode.startswith('Full'):path,url=fetch_source(month,database().parent/'downloads');source_mode='live'
                else:path=ROOT/'data/bundled'/f'green_tripdata_{month}.parquet';url='bundled:NYC-TLC-row-capped-snapshot';source_mode='bundled'
                result=run_partition(path,month,database(),source_mode,url)
                st.session_state.last_result=result
        except Exception:st.error('The source download failed before ingestion. Published rows have not changed.')
    if 'last_result' in st.session_state:
        result=st.session_state.last_result
        if result['status']=='failed':st.error(f'Run {result["run_id"]}: {result["error"]}')
        else:st.success(f'Published {result["published_rows"]:,} rows for {result["month"]}. Replaced {result["previous_rows"]:,} earlier rows and quarantined {result["rejected_rows"]:,}.')
    history=ledger();metrics=run_metrics(history)
    st.subheader('Run history')
    st.dataframe(metrics[['run_id','month','mode','status','source_rows','published_rows','rejected_rows','previous_rows','duration_seconds','started_at']],hide_index=True,width='stretch')
    st.download_button('Download run history',metrics.to_csv(index=False),'pipeline_runs.csv','text/csv')
    a,b=st.columns(2)
    with a:
        st.subheader('Processing throughput')
        fig=go.Figure(go.Scatter(x=metrics.started_at,y=metrics.rows_per_second,mode='markers',marker=dict(color=[RED if s=='failed' else MINT for s in metrics.status],size=8),text=metrics.run_id))
        fig.update_yaxes(title='Source rows per processing second');plot(fig,280)
        st.caption('This excludes download time. Small files exaggerate the effect of setup overhead; compare runs with similar input sizes and modes.')
    with b:
        st.subheader('Rejection rate per attempt')
        fig=go.Figure(go.Bar(x=metrics.run_id,y=metrics.rejection_rate*100,marker_color=AMBER))
        fig.update_yaxes(title='Rejected source rows (%)');plot(fig,280)
        st.caption('A run that fails before validation can have no usable rejection rate. Its status and error in the ledger are the relevant evidence.')
    st.subheader('Compare two publications')
    successful=history[(history.month==month)&history.status.isin(['success','warning'])].head(2)
    if len(successful)==2:
        current,prior=successful.iloc[0],successful.iloc[1]
        st.write(f'The latest two successful attempts for {month} published {int(current.published_rows):,} and {int(prior.published_rows):,} rows. Their source checksums {"match" if current.source_sha256==prior.source_sha256 else "differ"}.')
        st.caption('Matching source files should produce the same partition under unchanged validation rules. The table has a primary key on month and row hash; publication deletes and reinserts that month inside the transaction.')
    else:st.info('Run this month again to compare its last two successful publications.')
    with st.expander('Test a missing-column failure'):
        st.write('This uses a temporary copy of the bundled month with trip_distance removed. The missing field should fail the contract and leave the published partition unchanged.')
        if st.button('Run schema-failure drill'):
            before=count('SELECT count(*) FROM silver_trips WHERE month=?',[month])
            fixture=pd.read_parquet(ROOT/'data/bundled'/f'green_tripdata_{month}.parquet').drop(columns=['trip_distance'])
            path=database().parent/'schema_failure_drill.parquet';fixture.to_parquet(path,index=False)
            st.session_state.last_result=run_partition(path,month,database(),'test','local:schema-failure-drill')
            after=count('SELECT count(*) FROM silver_trips WHERE month=?',[month])
            st.session_state.drill_result={'before':before,'after':after}
            st.rerun()
        if 'drill_result' in st.session_state:
            result=st.session_state.drill_result
            st.write(f'Published rows before the drill: {result["before"]:,}. After: {result["after"]:,}. The failed attempt is in the run history.')
    failed=history[history.status=='failed']
    if not failed.empty:
        st.subheader('Failure details');st.dataframe(failed[['run_id','month','started_at','error']],hide_index=True,width='stretch')


def quality():
    title('Data quality','These checks apply explicit rules to the source records. The thresholds are choices made for this demonstration, so rejected records should be inspected before calling them errors in the original data.')
    matrix=quality_matrix(database())
    summary=matrix.groupby('check_name')[['failed_rows','checked_rows']].sum()
    summary['failure_rate']=summary.failed_rows/summary.checked_rows
    a,b=st.columns(2)
    with a:
        st.subheader('Rows failing each rule')
        fig=go.Figure(go.Bar(x=summary.failed_rows,y=summary.index,orientation='h',marker_color=AMBER));plot(fig,300)
    with b:
        st.subheader('Failure rates by month')
        pivot=matrix.pivot(index='check_name',columns='month',values='failure_rate')
        fig=go.Figure(go.Heatmap(z=pivot.values*100,x=pivot.columns,y=pivot.index,colorscale=[[0,'#182c29'],[1,AMBER]],hovertemplate='%{y}<br>%{x}: %{z:.2f}%<extra></extra>'));plot(fig,300)
    summary['rule']=summary.index.map(RULES)
    st.dataframe(summary[['rule','failed_rows','checked_rows','failure_rate']],hide_index=True,width='stretch',column_config={'failure_rate':st.column_config.NumberColumn(format='percent')})
    st.caption('Check counts overlap because one source row can fail several rules. The acceptance rate on the overview counts each rejected row once.')
    reasons,rejected=rejection_profile(database())
    total=int(latest().rejected_rows.sum())
    st.caption(f'Record-level profiling below covers {len(rejected):,} of {total:,} current rejected rows, capped at 10,000 in source-row order. Check totals above cover every current source row.')
    if rejected.empty:st.info('There are no quarantined rows in the current publications.');return
    st.subheader('Accepted and rejected record distributions')
    column=st.selectbox('Field',['distance_miles','fare','total','duration_minutes'])
    fields={'distance_miles':'distance_miles','fare':'fare','total':'total','duration_minutes':"date_diff('second',pickup_at,dropoff_at)/60.0"}
    accepted=read_table(database(),f'SELECT {fields[column]} AS value FROM silver_trips ORDER BY row_id LIMIT 10000').value
    bad=rejected[column].replace([np.inf,-np.inf],np.nan).dropna()
    good=accepted.replace([np.inf,-np.inf],np.nan).dropna()
    combined=pd.concat([good,bad])
    if not combined.empty:
        lo,hi=combined.quantile([.01,.99]);fig=go.Figure()
        for name,values,color in [('Accepted',good,MINT),('Rejected',bad,AMBER)]:
            fig.add_trace(go.Histogram(x=values[values.between(lo,hi)],histnorm='probability density',nbinsx=40,name=name,opacity=.6,marker_color=color))
        fig.update_layout(barmode='overlay');fig.update_xaxes(title=column);plot(fig,320)
        st.caption('For legibility the chart omits values outside the pooled 1st and 99th percentiles. Accepted records are capped at 10,000 in row-hash order. A rejected record can have a normal value in this field and fail a different rule.')
    st.subheader('Inspect rejected records')
    month=st.selectbox('Partition',MONTHS)
    rows=rejected[rejected.month==month]
    st.dataframe(rows,width='stretch',hide_index=True)
    st.download_button('Download profiled rejections',rows.to_csv(index=False),'rejection_profile.csv','text/csv')


def contracts():
    title('Contracts and lineage','The pipeline checks required fields before it publishes a month. Source files are retained by checksum, while the audit records show which version produced the current tables.')
    st.subheader('Data flow')
    nodes={'Bronze Parquet':(.05,.65),'Validation':(.35,.65),'silver_trips':(.65,.65),'zone_daily':(.95,.65),
        'Quarantine':(.35,.1),'taxi_zones':(.65,.1)}
    edges=[('Bronze Parquet','Validation'),('Validation','silver_trips'),('Validation','Quarantine'),('silver_trips','zone_daily'),('taxi_zones','zone_daily')]
    fig=go.Figure()
    for start,end in edges:
        x1,y1=nodes[start];x2,y2=nodes[end]
        fig.add_trace(go.Scatter(x=[x1,x2],y=[y1,y2],mode='lines',line=dict(color='#6e9990',width=2),showlegend=False,hoverinfo='skip'))
    fig.add_trace(go.Scatter(x=[x for x,y in nodes.values()],y=[y for x,y in nodes.values()],text=list(nodes),mode='markers+text',textposition='top center',marker=dict(size=18,color=MINT),showlegend=False,hoverinfo='text'))
    fig.update_xaxes(visible=False,range=[-.1,1.1]);fig.update_yaxes(visible=False,range=[-.15,.95]);plot(fig,230)
    st.caption('The zone lookup joins accepted trips in the gold model. Quarantine branches off before publication. Run and partition metadata link each publication back to its source checksum.')
    st.subheader('Required fields and row rules')
    contract=pd.DataFrame({'Source field':list(COLUMNS),'Published field':list(COLUMNS.values()),'Required':True})
    st.dataframe(contract,hide_index=True,width='stretch')
    st.dataframe(pd.DataFrame({'Check':list(RULES),'Condition':list(RULES.values())}),hide_index=True,width='stretch')
    st.write('Required columns must exist. Dates and numerics are then parsed, and malformed values fail row checks. Additional columns are recorded as drift but do not stop publication. Type changes are recorded even when the conversion succeeds.')
    partitions=read_table(database(),'SELECT * FROM partitions ORDER BY month')
    st.subheader('Current source schemas')
    schemas={row.month:json.loads(row.source_schema) for _,row in partitions.iterrows()}
    st.dataframe(pd.DataFrame(schemas).rename_axis('Source column'),width='stretch')
    st.subheader('Recorded schema changes')
    changes=schema_changes(ledger())
    if changes.empty:st.info('No schema changes have been recorded relative to an earlier publication of the same month. The first publication establishes that month’s baseline.')
    else:
        st.dataframe(changes,hide_index=True,width='stretch')
        grouped=changes.groupby(['month','change']).size().unstack(fill_value=0)
        fig=go.Figure()
        for column,color in zip(grouped.columns,[MINT,AMBER,RED]):fig.add_trace(go.Bar(x=grouped.index,y=grouped[column],name=column,marker_color=color))
        fig.update_layout(barmode='group');plot(fig,260)
    st.subheader('Source versions behind the current tables')
    st.dataframe(latest()[['month','mode','source_url','source_sha256','run_id']],hide_index=True,width='stretch')
    st.write('A checksum identifies bytes, not data quality. Partition replacement happens only after validation succeeds, so a failed new version does not become the published source.')
    with st.expander('Bundled source provenance'):st.json(json.loads((ROOT/'data/bundled/manifest.json').read_text()))
    st.markdown('[NYC TLC source files and data dictionaries](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)')


def warehouse():
    title('Warehouse analytics','These charts use accepted trips in the current publications. In bundled mode they describe a small slice of each source file; they should not be used to estimate demand across New York.')
    profile=partition_profile(database())
    st.subheader('Coverage by partition')
    st.dataframe(profile,hide_index=True,width='stretch')
    st.caption('First and last pickup show the actual date coverage, which can be much shorter than a month in a row-capped snapshot. Revenue is the sum of accepted total_amount values, in dollars.')
    a,b=st.columns(2)
    with a:
        st.subheader('Pickup hour by day of week')
        hours=read_table(database(),'''SELECT isodow(pickup_at) AS weekday,hour(pickup_at) AS hour,count(*) AS trips
            FROM silver_trips GROUP BY 1,2''')
        pivot=hours.pivot(index='weekday',columns='hour',values='trips').reindex(index=range(1,8),columns=range(24),fill_value=0).fillna(0)
        fig=go.Figure(go.Heatmap(z=pivot.values,x=pivot.columns,y=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],colorscale=[[0,'#182c29'],[1,MINT]],hovertemplate='%{y}, %{x}:00<br>%{z} trips<extra></extra>'));plot(fig,310)
        st.caption('Hours use the source’s local timestamp. Uneven date coverage affects the apparent weekly pattern.')
    with b:
        st.subheader('Trips and revenue by borough')
        borough=read_table(database(),'SELECT borough,sum(trip_count) AS trips,sum(total_revenue) AS revenue FROM zone_daily GROUP BY borough ORDER BY trips DESC')
        choice=st.radio('Borough measure',['Trips','Revenue'],horizontal=True)
        field='trips' if choice=='Trips' else 'revenue'
        fig=go.Figure(go.Bar(x=borough.borough,y=borough[field],marker_color=BLUE));plot(fig,265)
    a,b=st.columns(2)
    with a:
        st.subheader('Distance and duration')
        sample=read_table(database(),'''SELECT distance_miles,total,date_diff('second',pickup_at,dropoff_at)/60.0 AS minutes
            FROM silver_trips ORDER BY row_id LIMIT 3000''')
        fig=go.Figure(go.Scattergl(x=sample.distance_miles,y=sample.minutes,mode='markers',marker=dict(color=MINT,size=4,opacity=.35)))
        fig.update_xaxes(title='Miles');fig.update_yaxes(title='Minutes');plot(fig,310)
        st.caption('Up to 3,000 accepted records in row-hash order. Passing broad validation bounds does not remove all unusual trips.')
    with b:
        st.subheader('Trip total by distance band')
        bands=read_table(database(),'''SELECT CASE WHEN distance_miles<1 THEN '1: <1 mile' WHEN distance_miles<3 THEN '2: 1-3 miles'
            WHEN distance_miles<5 THEN '3: 3-5 miles' WHEN distance_miles<10 THEN '4: 5-10 miles' ELSE '5: 10+ miles' END AS band,
            quantile_cont(total,.25) AS q25,quantile_cont(total,.5) AS median,quantile_cont(total,.75) AS q75
            FROM silver_trips GROUP BY 1 ORDER BY 1''')
        fig=go.Figure(go.Bar(x=bands.band,y=bands['median'],marker_color=AMBER,error_y=dict(type='data',symmetric=False,array=bands.q75-bands['median'],arrayminus=bands['median']-bands.q25)))
        fig.update_yaxes(title='Total amount ($)');plot(fig,310)
        st.caption('Bars show medians; whiskers show the 25th to 75th percentiles. They are spread intervals, not confidence intervals. Totals include recorded charges beyond the base fare.')
    st.subheader('Concentration across pickup zones')
    zones=read_table(database(),'SELECT zone,sum(trip_count) AS trips FROM zone_daily GROUP BY zone ORDER BY trips DESC')
    zones['cumulative_share']=zones.trips.cumsum()/zones.trips.sum()
    fig=go.Figure(go.Scatter(x=np.arange(1,len(zones)+1),y=zones.cumulative_share*100,line=dict(color=MINT,width=2)))
    fig.update_xaxes(title='Pickup zones, ranked by trip count');fig.update_yaxes(title='Cumulative share of accepted trips (%)');plot(fig,280)
    st.caption('This is concentration within the loaded records. Snapshot ordering and coverage can strongly affect it.')
    st.dataframe(zones.head(20),hide_index=True,width='stretch',column_config={'cumulative_share':st.column_config.NumberColumn(format='percent')})


def query_plan(sql):
    with duckdb.connect(str(database()),read_only=True,config={'enable_external_access':'false','threads':'2','memory_limit':'256MB'}) as db:
        statements=db.extract_statements(sql)
        if len(statements)!=1 or statements[0].type!=duckdb.StatementType.SELECT:raise ValueError('Use one SELECT statement.')
        plan=db.execute(f'EXPLAIN SELECT * FROM ({sql.strip().rstrip(";")}) user_query LIMIT 500').fetchdf()
        return '\n'.join(plan.explain_value.astype(str))


def workbench():
    title('SQL and dbt','Query the published tables, inspect the physical plan, or run the dbt model and its tests against this session’s warehouse.')
    st.caption('Tables: silver_trips, zone_daily, taxi_zones, pipeline_runs, partitions, quality_checks and quarantine. The query connection is read-only, external access is disabled, and displayed results are capped at 500 rows.')
    sql=st.text_area('SQL',value='SELECT zone, borough, sum(trip_count) AS trips,\n       round(sum(total_revenue), 2) AS revenue\nFROM zone_daily\nGROUP BY zone, borough\nORDER BY trips DESC\nLIMIT 20',height=175,key='sql_text')
    a,b=st.columns(2)
    with a:execute=st.button('Run query',type='primary')
    with b:explain=st.button('Explain query')
    if execute:
        try:
            start=time.perf_counter();result=safe_query(database(),sql);elapsed=time.perf_counter()-start
            st.session_state.query_result=result;st.session_state.query_seconds=elapsed;st.session_state.query_sql=sql
        except Exception as exc:st.error(str(exc)[:350])
    if explain:
        try:st.session_state.query_plan=query_plan(sql)
        except Exception as exc:st.error(str(exc)[:350])
    if 'query_result' in st.session_state:
        st.caption(f'{len(st.session_state.query_result):,} result rows in {st.session_state.query_seconds*1000:.1f} ms, including connection and materialisation. SQL: {st.session_state.query_sql.replace(chr(10)," ")[:180]}')
        st.dataframe(st.session_state.query_result,hide_index=True,width='stretch')
        st.download_button('Download query results',st.session_state.query_result.to_csv(index=False),'query_result.csv','text/csv')
    if 'query_plan' in st.session_state:
        st.subheader('DuckDB physical plan');st.code(st.session_state.query_plan,language='text')
        st.write('Look for the scan, aggregation, and sorting operators. EXPLAIN shows the planned operators and estimated row counts; it does not measure execution time. The outer limit is the same 500-row cap used by the query runner.')
    st.subheader('The gold model')
    st.code((ROOT/'dbt/models/marts/zone_daily.sql').read_text(),language='sql')
    st.write('Python publication and dbt use this same SQL file. The model groups accepted trips by pickup date and zone, then adds the zone and borough lookup. Aggregating before serving the dashboard reduces the number of records it needs to scan.')
    if st.button('Run dbt build'):
        try:
            with st.spinner('Running the model and dbt tests...'):code,log=dbt_build(database())
            st.session_state.dbt_result={'code':code,'log':log}
        except Exception as exc:st.error(str(exc))
    if 'dbt_result' in st.session_state:
        result=st.session_state.dbt_result
        (st.success if result['code']==0 else st.error)('dbt build passed.' if result['code']==0 else 'dbt build failed.')
        with st.expander('Execution log'):st.code(result['log'][-16000:])
    st.caption('The dbt checks cover required values and duplicate trip identities. They complement the ingestion checks; they do not establish that every accepted trip is accurate in the real world.')
