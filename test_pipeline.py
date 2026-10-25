from pathlib import Path
import tempfile
import unittest
import pandas as pd
from streamlit.testing.v1 import AppTest
from pipeline import ROOT, run_partition, read_table, safe_query


def fixture():
    return pd.DataFrame({'lpep_pickup_datetime':pd.to_datetime(['2025-01-02 12:00','2025-01-02 13:00','2025-01-02 14:00']),
        'lpep_dropoff_datetime':pd.to_datetime(['2025-01-02 12:15','2025-01-02 13:30','2025-01-02 14:20']),
        'trip_distance':[2.,3.,4.], 'fare_amount':[12.,15.,19.], 'total_amount':[15.,18.,22.],
        'PULocationID':[74,75,74], 'DOLocationID':[75,74,75]})


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.database=self.root/'warehouse.duckdb';self.source=self.root/'source.parquet'

    def run_frame(self,frame):
        frame.to_parquet(self.source,index=False)
        return run_partition(self.source,'2025-01',self.database,'test')

    def test_replay_is_idempotent_and_ledger_retains_both_attempts(self):
        first=self.run_frame(fixture());second=self.run_frame(fixture())
        self.assertEqual(first['published_rows'],3)
        self.assertEqual(second['previous_rows'],3)
        self.assertEqual(read_table(self.database,'SELECT count(*) n FROM silver_trips').n.iloc[0],3)
        self.assertEqual(read_table(self.database,'SELECT count(*) n FROM pipeline_runs').n.iloc[0],2)

    def test_invalid_duplicate_and_valid_rows_conserve_input(self):
        frame=fixture();invalid=frame.iloc[[0]].copy();invalid['trip_distance']=-1
        result=self.run_frame(pd.concat([frame,frame.iloc[[0]],invalid],ignore_index=True))
        self.assertEqual(result['source_rows'],5)
        self.assertEqual(result['rejected_rows'],1)
        self.assertEqual(result['duplicate_rows'],1)
        self.assertEqual(result['published_rows'],3)
        self.assertEqual(read_table(self.database,'SELECT count(*) n FROM quarantine').n.iloc[0],1)

    def test_missing_schema_preserves_published_partition(self):
        self.run_frame(fixture())
        failed=self.run_frame(fixture().drop(columns=['trip_distance']))
        self.assertEqual(failed['status'],'failed')
        self.assertIn('trip_distance',failed['error'])
        self.assertEqual(read_table(self.database,'SELECT count(*) n FROM silver_trips').n.iloc[0],3)

    def test_changed_partition_replaces_old_rows_and_records_schema_drift(self):
        self.run_frame(fixture())
        frame=fixture().iloc[:2].copy();frame['new_optional_field']=1
        result=self.run_frame(frame)
        self.assertEqual(result['published_rows'],2)
        self.assertIn('new_optional_field',result['drift_json'])
        self.assertEqual(read_table(self.database,'SELECT sum(trip_count) n FROM zone_daily').n.iloc[0],2)

    def test_zero_valid_rows_preserves_previous_data(self):
        self.run_frame(fixture());frame=fixture();frame['trip_distance']=-1
        result=self.run_frame(frame)
        self.assertEqual(result['status'],'failed')
        self.assertEqual(read_table(self.database,'SELECT count(*) n FROM silver_trips').n.iloc[0],3)

    def test_sql_is_readonly_and_cannot_read_external_files(self):
        self.run_frame(fixture())
        self.assertEqual(safe_query(self.database,'SELECT count(*) n FROM silver_trips').n.iloc[0],3)
        for query in ['DELETE FROM silver_trips','SELECT 1; SELECT 2',"SELECT * FROM read_csv('/etc/passwd')"]:
            with self.assertRaises(Exception):safe_query(self.database,query)

    def test_app_bootstrap_and_replay(self):
        app=AppTest.from_file('app.py',default_timeout=45).run()
        self.assertFalse(app.exception)
        database=app.session_state['warehouse_path']
        before=read_table(database,'SELECT count(*) n FROM silver_trips').n.iloc[0]
        app.button[0].click().run()
        self.assertFalse(app.exception)
        self.assertEqual(before,read_table(database,'SELECT count(*) n FROM silver_trips').n.iloc[0])


if __name__=='__main__': unittest.main()
