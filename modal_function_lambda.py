import duckdb
import os
from deltalake import write_deltalake
import modal
from fastapi import Body

DELTA_TABLE_PATH = "s3://indystats-modal-data/ducklamb"
DAILY_TABLE_PATH = "s3://indystats-modal-data/ducklambcummulative"

def etl_foo(bucket:str, key:str):
    conn = duckdb.connect()
    conn.query(
        """
            INSTALL httpfs;
            LOAD httpfs;
                CREATE SECRET secretaws (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN
            );
        """
    )
    
    print('grab data...')
    print('bucket:', bucket)
    print('key:', key)
    
    df = conn.query(
        f"""
            CREATE TABLE data as (
            SELECT CAST(date as DATE) date, serial_number, model, capacity_bytes, failure, datacenter, cluster_id, vault_id, pod_id, pod_slot_num
            FROM read_csv('s3://{bucket}/{key}', header=true, delim = ',' , ignore_errors=true)
            );
            SELECT * FROM data;
        """
    ).arrow()
    print('data shape:', df.shape, flush=True)
    print('write to deltalake...')
    write_deltalake(
        DELTA_TABLE_PATH,
        df,
        mode="append",
        storage_options={"AWS_S3_ALLOW_UNSAFE_RENAME": "true"},
    )
    print('create current table...')
    conn.query(
        f"""
        CREATE TABLE current as (
            SELECT CAST(date as DATE) date, model, failure_rate
            FROM delta_scan('{DAILY_TABLE_PATH}')
            WHERE CAST(date as DATE) IN (SELECT DISTINCT CAST(date as DATE) FROM data)
        );
        """
    )
    print('create cumulative table...')
    cummulative_df = conn.query(
        """
        CREATE TABLE pickles as (
                        SELECT date, model, failure_rate
                        FROM current
                        UNION ALL
                        SELECT date, model, failure as failure_rate
                        FROM data
                        );
        SELECT CAST(date as STRING) as date, model, CAST(SUM(failure_rate) as INT) as failure_rate
        FROM pickles
        GROUP BY date, model;
        """
    ).arrow()
    
    date_partitions = (
        conn.execute("SELECT DISTINCT CAST(date as DATE) date FROM data;")
        .fetchdf()["date"]
        .tolist()
    )
    partition_filters = [("date", "=", x.strftime("%Y-%m-%d")) for x in date_partitions]
    print('partition filters:', partition_filters)
    predicate = " AND ".join([f"{col} {op} '{val}'" for col, op, val in partition_filters])
    print('predicate:', predicate)
    print('write cumulative table...')
    write_deltalake(
        DAILY_TABLE_PATH,
        cummulative_df,
        mode="overwrite",
        predicate=predicate,
        schema_mode="overwrite",
        storage_options={"AWS_S3_ALLOW_UNSAFE_RENAME": "true"},
    )
    print('done!')

image = (
    modal.Image.debian_slim()
    .pip_install("deltalake", "duckdb", "getdaft[deltalake]", "numpy", "pandas", "fastapi")
)
app = modal.App("duckdb-deltalake-s3-lambda", image=image)

@app.function(secrets=[modal.Secret.from_name("aws-secret")])
@modal.fastapi_endpoint(method="POST", requires_proxy_auth=True)
def run_etl(bucket: str = Body(...), key: str = Body(...)):
    etl_foo(bucket=bucket, key=key)
