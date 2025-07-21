import daft

BUCKET="indystats-modal-data"
DELTA_TABLE_PATH = f"s3://{BUCKET}/ducklamb"
DAILY_TABLE_PATH = f"s3://{BUCKET}/ducklambcummulative"

columns_to_keep = ['date','serial_number','model','capacity_bytes','failure','datacenter','cluster_id','vault_id','pod_id','pod_slot_num']

### this block creates the delta table files ###
df = daft.read_csv('2024-07-01.csv', allow_variable_columns=True)

df_cleaned = df.select(*columns_to_keep)
df_cleaned.write_deltalake(DELTA_TABLE_PATH)

df = daft.from_pydict({"date": ['2024-12-30'], 'model': ['ST4000DM000'], 'failure_rate': [0]})
df.write_deltalake(DAILY_TABLE_PATH, partition_cols=['date'])

################################################

# ### this block creates the raw file (to be grabbed by the cron) ###
# import pandas as pd
# (
#     pd.read_csv('2024-07-04.csv')
#     [columns_to_keep]
#     .head(150) # limiting the size for quicker testing...
#     .to_csv("2024-07-04.csv")
# )
# ##################################################################
