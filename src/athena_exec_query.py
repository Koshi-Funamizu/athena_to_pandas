from time import sleep
import boto3


def athena_exec_query(db_name: str, sql: str, athena_output_location: str):
    client = boto3.client('athena', 'us-west-2')

    # クエリ実行
    post_query = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
        'Database': db_name
        },
        ResultConfiguration={
        'OutputLocation': athena_output_location
        })

    # Query ID 取得
    query_id = post_query['QueryExecutionId']

    # 進捗状況表示
    while True:
        add_partition_response = client.get_query_execution(QueryExecutionId=query_id)
        state = add_partition_response['QueryExecution']['Status']['State']
        print(f"Athena query state: {state}")

        if state in ['RUNNING']:
            sleep(1)
        else:
            break

    # 失敗ならばエラー
    if state == 'FAILED':
        raise RuntimeError(f'Athena Query Error. status:{state}')

    # クエリ結果取得
    response = client.get_query_results(QueryExecutionId=query_id)
    return response
