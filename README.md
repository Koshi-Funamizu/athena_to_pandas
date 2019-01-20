# athena_to_pandas
AthenaにBoto3からクエリを投げ、Pandas DFに格納するプログラム

# 前提条件
Python 3.6以降
pandas, boto3がインストールされていること

# 実行結果
```
>>> import sys
>>> import time
>>> import pandas as pd
>>> from athena_exec_query import athena_exec_query
>>>
>>> # DB
... db_name = "rds_dl_blackswan_sandbox_dev"
>>>
>>> # ログ出力場所
... athena_output_location = 's3://kfunamizu/logs/'
>>>
>>> # クエリ
... # sql = sys.argv[1]
... sql = 'select * from ps4_evt_276_acct_big_app_session_end limit 100;'
>>>
>>> # クエリタイム計測スタート
... start = time.time()
>>>
>>> # Athena クエリ実行
... response = athena_exec_query(db_name, sql, athena_output_location)
Athena query state: RUNNING
Athena query state: RUNNING
Athena query state: RUNNING
Athena query state: SUCCEEDED
>>>
>>> # クエリタイム結果表示
... elapsed_time = time.time() - start
>>> print(f'elapsed_time:{elapsed_time} [sec]')
elapsed_time:3.348675489425659 [sec]
>>>
>>> # 結果整型
... result = []
>>> for rows in response['ResultSet']['Rows']:
...     tmp = [data for data in rows['Data']]
...     tmp2 = []
...     for key in tmp:
...         if len(key) > 0:
...             tmp2.append(list(key.values())[0])
...         else:
...             tmp2.append('')
...     # print(tmp2)
...     result.append(tmp2)
...
>>> # パンダズ化
... df = pd.DataFrame(result[1:], columns=result[0])
>>> df
              log_timestamp             host method            path code format_version ... first_boot_ind save_data_ind ps_now_download_ind  year month day
0   2018-10-09 00:12:07.000     xxxx   GET  /updptl/ps4/ci  200              c ...              0             1                   0  2018    10   9
1   2018-10-09 00:12:07.000    xxxx    GET  /updptl/ps4/ci  200              c ...              0             0                   0  2018    10   9
2   2018-10-09 00:12:07.000   xxxx    GET  /updptl/ps4/ci  200              c ...              0             1                   0  2018    10   9
3   2018-10-09 00:12:07.000    xxxx    GET  /updptl/ps4/ci  200              c ...              0             0                   0  2018    10   9
```
