"""Read-only summaries of the current warehouse and its audit records."""
import json
import numpy as np
import pandas as pd
from pipeline import read_table


def current_runs(database):
    return read_table(database,'SELECT r.* FROM pipeline_runs r JOIN partitions p ON r.run_id=p.run_id ORDER BY r.month')


def run_metrics(runs):
    frame=runs.copy()
    frame['duration_seconds']=(frame.finished_at-frame.started_at).dt.total_seconds()
    frame['rows_per_second']=frame.source_rows/frame.duration_seconds.replace(0,np.nan)
    measured=(frame.valid_rows+frame.rejected_rows+frame.duplicate_rows).eq(frame.source_rows) & frame.source_rows.gt(0)
    frame['rejection_rate']=(frame.rejected_rows/frame.source_rows.replace(0,np.nan)).where(measured)
    frame['accepted_rate']=(frame.published_rows/frame.source_rows.replace(0,np.nan)).where(measured)
    return frame


def quality_matrix(database):
    return read_table(database,'''SELECT p.month,q.check_name,q.failed_rows,q.checked_rows,
        1.0*q.failed_rows/nullif(q.checked_rows,0) AS failure_rate
        FROM quality_checks q JOIN partitions p ON p.run_id=q.run_id ORDER BY p.month,q.check_name''')


def rejection_profile(database,limit=10000):
    if not 1<=limit<=10000:raise ValueError('Use a profile limit from 1 to 10000.')
    rows=read_table(database,'''SELECT q.* FROM quarantine q JOIN partitions p ON p.run_id=q.run_id
        ORDER BY q.month,q.source_row LIMIT ?''',[limit])
    reasons=[];payload=[]
    for _,row in rows.iterrows():
        reason_list=json.loads(row.reasons)
        reasons.extend({'month':row.month,'reason':reason} for reason in reason_list)
        item=json.loads(row.payload)
        payload.append({'month':row.month,'source_row':row.source_row,'reasons':', '.join(reason_list),
            'distance_miles':item.get('trip_distance'),'fare':item.get('fare_amount'),'total':item.get('total_amount'),
            'pickup_at':item.get('lpep_pickup_datetime'),'dropoff_at':item.get('lpep_dropoff_datetime')})
    details=pd.DataFrame(payload,columns=['month','source_row','reasons','distance_miles','fare','total','pickup_at','dropoff_at'])
    for column in ['distance_miles','fare','total']:details[column]=pd.to_numeric(details[column],errors='coerce')
    if not details.empty:
        details['duration_minutes']=(pd.to_datetime(details.dropoff_at,errors='coerce')-pd.to_datetime(details.pickup_at,errors='coerce')).dt.total_seconds()/60
    else:details['duration_minutes']=pd.Series(dtype=float)
    return pd.DataFrame(reasons,columns=['month','reason']),details


def schema_changes(runs):
    changes=[]
    for _,run in runs.iterrows():
        drift=json.loads(run.drift_json or '{}')
        for kind in ['added','removed']:
            for name in drift.get(kind,[]):
                changes.append({'run_id':run.run_id,'month':run.month,'time':run.started_at,'change':kind,'column':name,'before':'','after':''})
        for name,types in drift.get('type_changes',{}).items():
            changes.append({'run_id':run.run_id,'month':run.month,'time':run.started_at,'change':'type changed','column':name,**types})
    return pd.DataFrame(changes,columns=['run_id','month','time','change','column','before','after'])


def partition_profile(database):
    return read_table(database,'''SELECT month,count(*) AS trips,min(pickup_at) AS first_pickup,
        max(pickup_at) AS last_pickup,sum(total) AS revenue,
        quantile_cont(distance_miles,0.5) AS median_distance,
        quantile_cont(distance_miles,0.95) AS p95_distance,
        quantile_cont(date_diff('second',pickup_at,dropoff_at)/60.0,0.5) AS median_duration_minutes
        FROM silver_trips GROUP BY month ORDER BY month''')
