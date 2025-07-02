import boto3
import pandas as pd


def load_to_dynamodb(df, aws_config):
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=aws_config['region'],
        aws_access_key_id=aws_config['access_key'],
        aws_secret_access_key=aws_config['secret_key']
    )
    table = dynamodb.Table(aws_config['dynamodb_table'])

    with table.batch_writer() as batch:
        for _, row in df.iterrows():
            item = {
                k: v if not isinstance(v, list) else [str(x) for x in v]  # convert lists to string values
                for k, v in row.to_dict().items()
                if v is not None
            }
            batch.put_item(Item=item)
