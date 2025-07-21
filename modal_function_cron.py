import duckdb
from deltalake import write_deltalake
import modal
import boto3
import datetime

BUCKET="indystats-modal-data"
KEY='raw/'
DELTA_TABLE_PATH = f"s3://{BUCKET}/ducklamb"
DAILY_TABLE_PATH = f"s3://{BUCKET}/ducklambcummulative"

def etl_foo(bucket:str, key:str, lookback_days:int=1):
    ## get csvs created in the last day
    records = check_last_modified(bucket=bucket, key=key, lookback_days=lookback_days)
    if records:
        print(f'{len(records)} file[s] found!')
        filepaths = ", ".join(f"'s3://{bucket}/{r}'" for r in records) ## combine into string list for duckdb read_csv
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
        df = conn.query(
            f"""
                CREATE TABLE data as (
                SELECT CAST(date as DATE) date, serial_number, model, capacity_bytes, failure, datacenter, cluster_id, vault_id, pod_id, pod_slot_num
                FROM read_csv([{filepaths}], header=true, delim = ',' , ignore_errors=true)
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
        
    else:
        print('no data found!', flush=True)

# extra function to capture recent files
def check_last_modified(bucket:str, key:str, lookback_days:int=1):
    lookback = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days)
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    records = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for obj in page.get('Contents', []):
            if obj['LastModified'] >= lookback and '.csv' in obj['Key']:
                records.append(obj['Key'])
    return records

image = (
    modal.Image.debian_slim()
    .pip_install("deltalake", "duckdb", "getdaft[deltalake]", "numpy", "pandas", "boto3")
)

app = modal.App("duckdb-deltalake-s3-cron", image=image)

@app.function(
    secrets=modal.Secret.from_name("aws-secret"),
    schedule=modal.Cron("0 6 * * *"),
    timeout=3000,
)
def run_etl(bucket:str=BUCKET, key:str=KEY):
    etl_foo(bucket=bucket, key=key)