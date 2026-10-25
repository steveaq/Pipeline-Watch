from pathlib import Path
import uuid
import streamlit as st
from pipeline import ROOT, MONTHS, run_partition

st.set_page_config(page_title='Pipeline Watch',page_icon='▦',layout='wide',initial_sidebar_state='expanded')
st.markdown(f'<style>{(ROOT/"styles.css").read_text()}</style>',unsafe_allow_html=True)
if 'warehouse_path' not in st.session_state:
    st.session_state.warehouse_path=str(ROOT/'runtime/sessions'/uuid.uuid4().hex/'warehouse.duckdb')
database=Path(st.session_state.warehouse_path)
if not database.exists():
    with st.spinner('Loading the bundled taxi records...'):
        for month in MONTHS:
            result=run_partition(ROOT/'data/bundled'/f'green_tripdata_{month}.parquet',month,database,source_url='bundled:NYC-TLC-row-capped-snapshot')
            if result['status']=='failed':st.error(result['error']);st.stop()
with st.sidebar:
    st.markdown('### ▦ Pipeline Watch')
    st.caption('NYC green-taxi batch pipeline')
    st.caption('This browser session has its own DuckDB database. Start with the bundled records or download a complete month on the Runs and replay page.')
    st.caption('The bundled files contain the first 5,000 rows from each of three 2025 source files. They are not representative samples of the city.')
st.markdown('<div class="app-bar"><b>Pipeline Watch</b><span>NYC TLC / Python / DuckDB / dbt</span></div>',unsafe_allow_html=True)
pages=[st.Page('views/overview.py',title='Overview',url_path='overview',default=True),
    st.Page('views/runs.py',title='Runs and replay',url_path='runs'),
    st.Page('views/quality.py',title='Data quality',url_path='quality'),
    st.Page('views/contracts.py',title='Contracts and lineage',url_path='contracts'),
    st.Page('views/warehouse.py',title='Warehouse analytics',url_path='warehouse'),
    st.Page('views/workbench.py',title='SQL and dbt',url_path='workbench')]
st.navigation(pages).run()
