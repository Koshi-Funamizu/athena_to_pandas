import boto3
import time
from time import sleep
import pandas as pd
import sys

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


# DB
db_name = "test"

# ログ出力場所
athena_output_location = 's3://path/to/'

# クエリ
# sql = sys.argv[1]
sql = 'select * from athena_test limit 100;'

# クエリタイム計測スタート
start = time.time()

# Query 実行
response = athena_exec_query(db_name, sql, athena_output_location)

# クエリタイム結果表示
elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

# 結果格納
result = []

# 整型
for rows in response['ResultSet']['Rows']:
	tmp = [data for data in rows['Data']]
	tmp2 = []
	for key in tmp:
		if len(key) > 0:
			tmp2.append(list(key.values())[0])
		else:
			tmp2.append('')
			print(tmp2)

	result.append(tmp2)

# パンダズ化
df = pd.DataFrame(result[1:], columns=result[0])
