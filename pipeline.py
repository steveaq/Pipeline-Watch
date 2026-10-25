"""A small, real, replayable batch pipeline with a durable DuckDB audit trail."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid
import duckdb
from jinja2 import Environment, StrictUndefined
import numpy as np
import pandas as pd
import requests

ROOT=Path(__file__).parent
MONTHS=['2025-01','2025-02','2025-03']
COLUMNS={'lpep_pickup_datetime':'pickup_at','lpep_dropoff_datetime':'dropoff_at',
    'trip_distance':'distance_miles','fare_amount':'fare','total_amount':'total',
    'PULocationID':'pickup_zone','DOLocationID':'dropoff_zone'}
CORE=list(COLUMNS.values())
RULES={
    'timestamps_present':'Pickup and drop-off timestamps must be present',
    'valid_duration':'Duration must be positive and no more than 24 hours',
    'partition_match':'Pickup month must match the requested partition',
    'valid_distance':'Distance must be positive and no more than 250 miles',
    'nonnegative_money':'Fare and total must be present and nonnegative',
    'valid_zones':'Pickup and drop-off zones must exist in the TLC lookup',
}


def check_month(month):
    if not re.fullmatch(r'20\d{2}-(0[1-9]|1[0-2])',month):
        raise ValueError('Use a month in YYYY-MM format.')
    if pd.Period(month, freq='M') >= pd.Timestamp.now().to_period('M'):
        raise ValueError('Choose a completed historical month.')
    return month


def connect(path, read_only=False):
    return duckdb.connect(str(path),read_only=read_only)


def initialise(path):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    with connect(path) as db:
        db.execute('''CREATE TABLE IF NOT EXISTS pipeline_runs(
            run_id VARCHAR PRIMARY KEY, month VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
            status VARCHAR, mode VARCHAR, source_url VARCHAR, source_sha256 VARCHAR,
            source_rows BIGINT, valid_rows BIGINT, rejected_rows BIGINT, duplicate_rows BIGINT,
            previous_rows BIGINT, published_rows BIGINT, drift_json VARCHAR, error VARCHAR)''')
        db.execute('''CREATE TABLE IF NOT EXISTS silver_trips(
            row_id VARCHAR, month VARCHAR, pickup_at TIMESTAMP, dropoff_at TIMESTAMP,
            distance_miles DOUBLE, fare DOUBLE, total DOUBLE, pickup_zone INTEGER, dropoff_zone INTEGER,
            run_id VARCHAR, PRIMARY KEY(month,row_id))''')
        db.execute('''CREATE TABLE IF NOT EXISTS quarantine(
            run_id VARCHAR, month VARCHAR, source_row BIGINT, reasons VARCHAR, payload VARCHAR)''')
        db.execute('''CREATE TABLE IF NOT EXISTS quality_checks(
            run_id VARCHAR, check_name VARCHAR, failed_rows BIGINT, checked_rows BIGINT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS partitions(
            month VARCHAR PRIMARY KEY, source_schema VARCHAR, source_sha256 VARCHAR,
            run_id VARCHAR, mode VARCHAR, updated_at TIMESTAMP)''')
        zones=pd.read_csv(ROOT/'data/bundled/taxi_zone_lookup.csv')
        db.register('zone_input',zones)
        db.execute('CREATE OR REPLACE TABLE taxi_zones AS SELECT * FROM zone_input')
        render_gold(db)


def render_gold(db):
    # Execute the very same model used by dbt. No second copy of the aggregation.
    sql=(ROOT/'dbt/models/marts/zone_daily.sql').read_text()
    env=Environment(undefined=StrictUndefined)
    rendered=env.from_string(sql).render(source=lambda schema,table: f'"{table}"',config=lambda **kwargs:'')
    db.execute('CREATE OR REPLACE TABLE zone_daily AS '+rendered)


def fetch_source(month, folder):
    check_month(month)
    folder=Path(folder); folder.mkdir(parents=True,exist_ok=True)
    url=f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{month}.parquet'
    response=requests.get(url,timeout=(10,60),headers={'User-Agent':'PipelineWatch/1.0 portfolio batch pipeline'})
    response.raise_for_status()
    if not response.content.startswith(b'PAR1'):
        raise ValueError('The TLC source did not return a Parquet file.')
    path=folder/f'green_tripdata_{month}.parquet'
    temporary=path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
    temporary.write_bytes(response.content)
    os.replace(temporary,path)
    return path,url


def validate(frame, month, zone_ids):
    missing=set(COLUMNS)-set(frame)
    if missing:
        raise ValueError('Required source columns missing: '+', '.join(sorted(missing)))
    core=frame[list(COLUMNS)].rename(columns=COLUMNS).copy()
    for col in ['pickup_at','dropoff_at']:
        core[col]=pd.to_datetime(core[col],errors='coerce')
    for col in ['distance_miles','fare','total','pickup_zone','dropoff_zone']:
        core[col]=pd.to_numeric(core[col],errors='coerce')
    hours=(core.dropoff_at-core.pickup_at).dt.total_seconds()/3600
    finite=lambda columns:pd.Series(np.isfinite(core[columns].to_numpy(dtype=float)).all(axis=1),index=core.index)
    checks={
        'timestamps_present':core.pickup_at.notna() & core.dropoff_at.notna(),
        'valid_duration':hours.gt(0) & hours.le(24),
        'partition_match':core.pickup_at.dt.strftime('%Y-%m').eq(month),
        'valid_distance':finite(['distance_miles']) & core.distance_miles.gt(0) & core.distance_miles.le(250),
        'nonnegative_money':finite(['fare','total']) & core.fare.ge(0) & core.total.ge(0),
        'valid_zones':core.pickup_zone.isin(zone_ids) & core.dropoff_zone.isin(zone_ids),
    }
    checks={name:mask.fillna(False) for name,mask in checks.items()}
    validity=pd.DataFrame(checks).all(axis=1)
    valid=core.loc[validity].copy()
    # Canonical field values define a replay-stable row identity. Source month is
    # part of the primary key; repeated publications replace that partition.
    serialised=valid.astype('string').fillna('<NULL>').agg('|'.join,axis=1) if not valid.empty else pd.Series(dtype='string')
    valid['row_id']=serialised.map(lambda row:hashlib.sha256(row.encode()).hexdigest())
    duplicates=int(valid.row_id.duplicated().sum())
    valid=valid.drop_duplicates('row_id')
    rejected=[]
    for index in core.index[~validity]:
        reasons=[name for name,mask in checks.items() if not mask.loc[index]]
        rejected.append({'source_row':int(index),'reasons':json.dumps(reasons),
                         'payload':frame.loc[index].to_json(date_format='iso')})
    return valid,pd.DataFrame(rejected,columns=['source_row','reasons','payload']),checks,duplicates


def run_partition(path, month, database, mode='bundled', source_url=''):
    check_month(month)
    if mode not in ('bundled','live','test'):
        raise ValueError('Unknown source mode.')
    initialise(database)
    run_id=uuid.uuid4().hex[:12]
    started=datetime.now(timezone.utc).replace(tzinfo=None)
    source_rows=valid_rows=rejected_rows=duplicates=previous=0
    digest=''; drift={}
    with connect(database) as db:
        db.execute('''INSERT INTO pipeline_runs(run_id,month,started_at,status,mode,source_url)
                      VALUES (?,?,?,'running',?,?)''',[run_id,month,started,mode,source_url])
        in_transaction=False
        try:
            path=Path(path)
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            # Immutable bronze copy for each source checksum, including failed inputs.
            bronze=Path(database).parent/'bronze'/month/f'{digest}.parquet'
            bronze.parent.mkdir(parents=True,exist_ok=True)
            if not bronze.exists(): shutil.copyfile(path,bronze)
            source=pd.read_parquet(path).reset_index(drop=True)
            source_rows=len(source)
            if source_rows==0:
                raise ValueError('The source file is empty; no partition was replaced.')
            schema={c:str(t) for c,t in source.dtypes.items()}
            previous_schema=db.execute('SELECT source_schema FROM partitions WHERE month=?',[month]).fetchone()
            if previous_schema:
                old=json.loads(previous_schema[0])
                drift={'added':sorted(set(schema)-set(old)),'removed':sorted(set(old)-set(schema)),
                       'type_changes':{c:{'before':old[c],'after':schema[c]} for c in set(old)&set(schema) if old[c]!=schema[c]}}
            zones=set(db.execute('SELECT LocationID FROM taxi_zones').fetchdf().LocationID)
            clean,rejected,checks,duplicates=validate(source,month,zones)
            valid_rows=len(clean); rejected_rows=len(rejected)
            if source_rows != valid_rows+rejected_rows+duplicates:
                raise ValueError('Row conservation failed.')
            if clean.empty:
                raise ValueError('No rows passed validation; the previous partition is unchanged.')
            previous=db.execute('SELECT count(*) FROM silver_trips WHERE month=?',[month]).fetchone()[0]
            clean['month']=month; clean['run_id']=run_id
            clean=clean[['row_id','month']+CORE+['run_id']]
            rejected['run_id']=run_id; rejected['month']=month
            quality=pd.DataFrame([{'run_id':run_id,'check_name':name,'failed_rows':int((~mask).sum()),'checked_rows':source_rows} for name,mask in checks.items()])
            db.execute('BEGIN TRANSACTION'); in_transaction=True
            db.register('clean_input',clean)
            db.execute('DELETE FROM silver_trips WHERE month=?',[month])
            db.execute('INSERT INTO silver_trips SELECT * FROM clean_input')
            if not rejected.empty:
                db.register('reject_input',rejected[['run_id','month','source_row','reasons','payload']])
                db.execute('INSERT INTO quarantine SELECT * FROM reject_input')
            db.register('quality_input',quality)
            db.execute('INSERT INTO quality_checks SELECT * FROM quality_input')
            db.execute('DELETE FROM partitions WHERE month=?',[month])
            db.execute('INSERT INTO partitions VALUES (?,?,?,?,?,?)',[month,json.dumps(schema),digest,run_id,mode,datetime.now(timezone.utc).replace(tzinfo=None)])
            render_gold(db)
            has_drift=any(bool(v) for v in drift.values())
            status='warning' if rejected_rows or duplicates or has_drift else 'success'
            db.execute('''UPDATE pipeline_runs SET finished_at=?,status=?,source_sha256=?,source_rows=?,
                valid_rows=?,rejected_rows=?,duplicate_rows=?,previous_rows=?,published_rows=?,drift_json=?,error='' WHERE run_id=?''',
                [datetime.now(timezone.utc).replace(tzinfo=None),status,digest,source_rows,valid_rows,rejected_rows,duplicates,previous,valid_rows,json.dumps(drift),run_id])
            db.execute('COMMIT'); in_transaction=False
        except Exception as exc:
            if in_transaction: db.execute('ROLLBACK')
            error=str(exc)[:500]
            db.execute('''UPDATE pipeline_runs SET finished_at=?,status='failed',source_sha256=?,
                source_rows=?,valid_rows=?,rejected_rows=?,duplicate_rows=?,previous_rows=?,published_rows=0,drift_json=?,error=? WHERE run_id=?''',
                [datetime.now(timezone.utc).replace(tzinfo=None),digest,source_rows,valid_rows,rejected_rows,duplicates,previous,json.dumps(drift),error,run_id])
        result=db.execute('SELECT * FROM pipeline_runs WHERE run_id=?',[run_id]).fetchdf().iloc[0].to_dict()
    return result


def read_table(database, sql, parameters=None):
    with connect(database,read_only=True) as db:
        return db.execute(sql,parameters or []).fetchdf()


def safe_query(database, sql):
    # A read-only connection alone does not stop SELECT read_csv('/private/file').
    # Disable external access and accept exactly one SELECT, with a result cap.
    with duckdb.connect(str(database),read_only=True,config={'enable_external_access':'false','threads':'2','memory_limit':'256MB'}) as db:
        statements=db.extract_statements(sql)
        if len(statements)!=1 or statements[0].type!=duckdb.StatementType.SELECT:
            raise ValueError('Use exactly one SELECT statement.')
        return db.execute(f'SELECT * FROM ({sql.strip().rstrip(";")}) AS user_query LIMIT 500').fetchdf()


def dbt_build(database):
    executable=Path(sys.executable).with_name('dbt')
    command=str(executable) if executable.exists() else shutil.which('dbt')
    if not command: raise RuntimeError('Install requirements.txt to enable dbt validation.')
    environment={**os.environ,'PIPELINE_WATCH_DB':str(Path(database).resolve())}
    result=subprocess.run([command,'build','--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(ROOT/'dbt'),
        '--no-use-colors'],capture_output=True,text=True,env=environment,timeout=120)
    return result.returncode,result.stdout+'\n'+result.stderr


def main():
    parser=argparse.ArgumentParser(description='Replayable NYC TLC ingestion and quality pipeline')
    parser.add_argument('command',choices=['run','demo','dbt'])
    parser.add_argument('--month',default='2025-01')
    parser.add_argument('--mode',choices=['bundled','live'],default='bundled')
    parser.add_argument('--database',default=str(ROOT/'runtime/warehouse.duckdb'))
    args=parser.parse_args()
    if args.command=='dbt':
        code,log=dbt_build(args.database);print(log);return code
    months=MONTHS if args.command=='demo' else [args.month]
    failed=False
    for month in months:
        check_month(month)
        if args.mode=='live':
            path,url=fetch_source(month,Path(args.database).parent/'downloads')
        else:
            path=ROOT/'data/bundled'/f'green_tripdata_{month}.parquet';url='bundled:NYC-TLC-row-capped-snapshot'
        result=run_partition(path,month,args.database,args.mode,url)
        print(json.dumps({k:str(v) if isinstance(v,(datetime,pd.Timestamp)) else v for k,v in result.items()},default=str))
        failed |= result['status']=='failed'
    return 1 if failed else 0


if __name__=='__main__':
    raise SystemExit(main())
