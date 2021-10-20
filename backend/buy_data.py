import pymysql
import json
import time, datetime

# 连接远端数据库
myConnection = pymysql.connect(port=3306, user="root", password="452820", database="hive")

# buy_data每个用户只有一次购买记录，用户角度中四个关注点无法进行判断

# 种类角度：最受欢迎角度
cur = myConnection.cursor()
cur.execute('select category_id, count(*) as times from buy_data group by category_id order by times desc limit 10')
result = cur.fetchall()
print("购买次数最多的十个种类ID：")
print(result)
mostPopularC = []
for ret in result:
    mostPopularC.append({"categoryID": ret[0], "count": ret[1]})
cur.close()

# 最近热销角度
# 最后购买记录的时间：1970--01--13 02:46:51
# 最早购买记录的时间：1970--01--01 08:00:02
cur = myConnection.cursor()
cur.execute(
    'select category_id, count(*) as times from buy_data where buy_data.timestamp > 413211 group by category_id order by times desc limit 10')
result = cur.fetchall()
print("最近一周内购买次数前十的种类ID：")
print(result)
recentPopularC = []
for ret in result:
    recentPopularC.append({"categoryID": ret[0], "count": ret[1]})
cur.close()

# 最受欢迎的商品
cur = myConnection.cursor()
cur.execute('select item_id, count(*) as times from buy_data group by item_id order by times desc limit 10')
result = cur.fetchall()
print("购买次数最多的十个商品ID：")
print(result)
mostPopularI = []
for item in result:
    mostPopularI.append({"itemID": item[0], "count": item[1]})
cur.close()

# 最近热销的商品
cur = myConnection.cursor()
cur.execute(
    'select item_id, count(*) as times from buy_data where buy_data.timestamp > 413211 group by item_id order by times desc limit 10')
result = cur.fetchall()
print("最近一周内购买次数前十的商品ID：")
print(result)
recentPopularI = []
for item in result:
    recentPopularI.append({"itemID": item[0], "count": item[1]})
cur.close()

myConnection.close()

with open("../frontend/src/data/Category.json", 'w+') as f:
    jsonObject = {'mostPopular': mostPopularC, 'recentPopular': recentPopularC}
    f.write(json.dumps(jsonObject))
    f.close()

with open("../frontend/src/data/Item.json", 'w+') as f:
    jsonObject = {'mostPopular': mostPopularI, 'recentPopular': recentPopularI}
    f.write(json.dumps(jsonObject))
    f.close()
