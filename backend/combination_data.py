import sys
import pandas as pd
import time
import json
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)

with open("../frontend/src/data/analysis.json", 'w+') as f:
    sys.stdout = f
    file = {}
    engine = create_engine('mysql+pymysql://root:452820@localhost:3306/hive')
    sql_query = 'select * from buy_data'
    buy_data = pd.read_sql_query(sql_query, engine)


    # 将timestamp转换成datetime【%Y-%m-%d %H:%M:%S】
    def timestamp_datetime(value):
        format = '%Y-%m-%d %H:%M:%S'
        value = time.localtime(value)
        dt = time.strftime(format, value)
        return dt  # str


    # 时间，datetime64[ns]
    buy_data['time'] = pd.to_datetime(buy_data.timestamp.apply(timestamp_datetime))
    buy_data['day'] = buy_data.time.dt.day
    buy_data['hour'] = buy_data.time.dt.hour
    buy_data['minute'] = buy_data.time.dt.minute

    file['描述0'] = '对购买数据进行基础分析\n'
    # 购买数据基本特征
    file['描述1'] = '购买数据有 {} 行 {} 列'.format(buy_data.shape[0], buy_data.shape[1])

    file['描述2'] = '数据中有' + str(len(buy_data['user_id'].unique())) + '位不同的用户' + str(
        len(buy_data['item_id'].unique())) + '件不同的商品\n'

    # 出现频率最高的各类型数据
    file['描述3'] = '下面是各个列值出现次数最高的次数\n'
    temp = {}
    for x in ['user_id', 'item_id', 'category_id', 'type', 'time', 'day', 'hour', 'minute']:
        temp[x] = buy_data[x].value_counts().head().to_json()
    file['购买数据分析'] = temp

    file['描述4'] = '\n注意到 5000010 号商品有多达 76169 次购买记录，但最多的被购买类型 1511622034 仅有 151 次购买记录，但某个商品应该属于某个类型，我们认为源数据有一定的问题\n'

    engine = create_engine('mysql+pymysql://root:452820@localhost:3306/hive')
    sql_query = 'select * from streaming_old limit 20000000'
    stream_data = pd.read_sql_query(sql_query, engine)

    stream_data.rename(columns={'userId': 'user_id', 'itemId': 'item_id', 'categoryId': 'category_id'}, inplace=True)

    # 将流数据纳入分析
    mergeData = pd.merge(buy_data, stream_data, 'outer', on=['item_id', 'user_id', 'category_id'])

    file['描述6'] = '对流数据进行基础分析\n'
    file['描述7'] = '下面是各个列值出现次数最高的次数\n'
    # 出现频率最高的各类型数据
    temp = {}
    for x in ['ipAddr', 'sessionId', 'date', 'url', 'user_id', 'item_id', 'category_id', 'isSecondKill', 'password',
              'authCode', 'success', 'type', 'time', 'day', 'hour', 'minute']:
        temp[x] = mergeData[x].value_counts().head().to_json()
    file['流数据分析'] = temp

    # 成交率
    file['描述8'] = '计算商品的成交率：'
    buy_count = pd.DataFrame(
        mergeData.loc[(mergeData['type'] == 'buy')]['item_id'].value_counts()).reset_index().rename(
        columns={'index': 'item_id', 'item_id': 'buy_count'})
    mergeData = pd.merge(mergeData, buy_count, 'left', on=['item_id'])
    interact_count = pd.DataFrame(
        mergeData.loc[(mergeData['url'] != 'NaN')]['item_id'].value_counts()).reset_index().rename(
        columns={'index': 'item_id', 'item_id': 'interact_count'})
    mergeData = pd.merge(mergeData, interact_count, 'left', on=['item_id'])
    mergeData['deal_rate'] = mergeData['buy_count'] / mergeData['interact_count']
    file['商品成交率'] = mergeData[['item_id', 'deal_rate']].head(10).to_json()

    # 商品平均复购率
    file['描述10'] = '计算商品的平均复购率：'
    isSecondKill = mergeData['isSecondKill'].value_counts()[0] / mergeData.shape[0]
    file['商品平均复购率'] = isSecondKill

    # 热门商品
    file['描述12'] = '寻找热门商品：'
    file['热门商品'] = \
        (mergeData.sort_values(by='buy_count', ascending=False).sort_values(by='interact_count', ascending=False))[
            'item_id'].value_counts().head(10).to_json()

    f.write(json.dumps(file, ensure_ascii=False))
    f.close()
