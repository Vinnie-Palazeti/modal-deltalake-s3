### Modal Labs + DuckDB + AWS Lambda

This repo rips off the code from Data Engineering Central's [DuckDBwithAWSLambda repo](https://github.com/danielbeach/DuckDBwithAWSLambda) and the corresponding [Substack post](https://dataengineeringcentral.substack.com/p/aws-lambda-duckdb-and-delta-lake), and adds Modal Labs as the layer that runs the data pipeline.

#### Modal Labs
Modal is infrastucture as python code. In ~7-8 lines of code I set up an ETL data pipeline that can handle an arbitrary large amount of data. Pretty cool.

#### Modal Web Endpoint
Modal functions can be setoff via a request to a web endpoint. This is nice, because Modal cannot natively listen for AWS events (ie. s3 object creation). Therefore, I setup a very minimal AWS lambda function to pass along the event (and credentials) to the Modal Endpoint.

```python
@app.function(secrets=[modal.Secret.from_name("aws-secret")])
@modal.fastapi_endpoint(method="POST", requires_proxy_auth=True)
def run_etl(bucket: str = Body(...), key: str = Body(...)):
    etl_foo(bucket=bucket, key=key)
```

Important to note that by default this endpoint is *publically available*. We probably don't just anybody making requests. Modal provides `requires_proxy_auth` we requires requests to pass along a `Modal Token` and `Modal Secret` as header variables.

#### AWS Lambda Function

I really hate using the AWS Console. It's a skill issue. I added the AWS CLI commands required for this project in the `aws_commands.txt`. Make sure to replace the region, profile, bucket, etc.

These commands setup the lambda function, give it a role & permissions, and add the secret headers.

#### Modal Cron Job

If you don't want to mess with the AWS lambda setup, you could just grab all of the data recently placed in the bucket each day. 


#### Thoughts

This was my first go at using Modal Labs. It's extremely easy to use. The idea of setting up a cronjob ETL script in a few lines of code is still a little wild to me. Though, I am a lowly Data Scientist. 
