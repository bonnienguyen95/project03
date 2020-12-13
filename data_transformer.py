import json
import time
import boto3
import pandas as pd
import yfinance as yf

kinesis = boto3.client('kinesis', 'us-east-1')
stock_df = pd.DataFrame(columns = ['name', 'ts', 'high', 'low']) 

tickers = ["FB", "SHOP", "BYND", "NFLX", "PINS", "SQ", "TTD", "OKTA", "SNAP", "DDOG"]
for ticker in tickers:
    data = yf.download(
        tickers = ticker,
        start="2020-12-01", end="2020-12-02",
        interval = "5m",
        group_by = 'ticker')
    modified = data.reset_index()
    modified = modified.assign(name=ticker)    
    modified = modified[['name', 'Datetime', 'High', 'Low']]
    modified = modified.rename(columns={"Datetime": "ts", "High": "high", 'Low': 'low'})
    stock_df = stock_df.append(modified)

stock_df["ts"] = stock_df.ts.astype('str')
stock_json = stock_df.to_json(orient='records')
parsed = json.loads(stock_json)

def lambda_handler(event, context):
    for data_point in parsed:
        data_point = str(data_point)+"\n"
        kinesis.put_record(
                    StreamName="STA9760F2020_stream2",
                    Data=data_point,
                    PartitionKey="partitionkey")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
