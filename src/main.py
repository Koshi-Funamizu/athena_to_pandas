import sys
import time
import pandas as pd
from athena_exec_query import athena_exec_query

# Athena DB
# db_name = os.environ['DB_NAME'] # 環境変数からDB名取得
db_name = ''

# ログ出力場所
# athena_output_location = os.environ['LOG_LOCATION'] # 環境変数からログ出力パス取得
athena_output_location = ''

# Athena クエリ
# sql = os.environ['SQL'] # 環境変数からクエリ取得
sql = 'select * from xxxxxx limit 100;'

# クエリタイム計測スタート
start = time.time()

# Athena クエリ実行
response = athena_exec_query(db_name, sql, athena_output_location)

# クエリタイム結果表示
elapsed_time = time.time() - start
print(f'elapsed_time: {elapsed_time} [sec]')

# 結果整型
result = []
for rows in response['ResultSet']['Rows']:
    tmp = [data for data in rows['Data']]
    tmp2 = []
    for key in tmp:
        if len(key) > 0:
            tmp2.append(list(key.values())[0])
        else:
            tmp2.append('')
    # print(tmp2)
    result.append(tmp2)

# パンダズ化
df = pd.DataFrame(result[1:], columns=result[0])
