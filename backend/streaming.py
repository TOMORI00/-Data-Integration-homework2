import json

import pymysql

myConnection = pymysql.connect(host="localhost", port=3306, user="root", password="root", database="streaming")

# 获取撞库机器人
cursor = myConnection.cursor()
print(myConnection)
print(cursor)
sql = "SELECT distinct userId , count(case when success = '1' then '1' end )/count(*) as successrate FROM " \
      "streaming where url = '/user/login' group by userId order by successrate limit 4753 "
cursor.execute(sql)
rr = cursor.fetchall()
Robot1 = []
result1 = []
for row in rr:
    print(row)
    Robot1.append({"userID": row[0]})
    result1.append(row[0])

'''for i in result1:
    try:
        sql_delete = "DELETE FROM streaming WHERE userId= '%s'"
        cursor.execute(sql_delete, i)
        myConnection.commit()
        print("done" + str(i))
    except Exception as e:
        print(e)'''

cursor.close()
print("____________________________________________")

# 获取抢单机器人
cursor = myConnection.cursor()
print(myConnection)
print(cursor)
sql = "SELECT distinct userId , count(case when url = '/item/buy' then '1' end )/count(case when url = '/item/cart'" \
      "then '1' end )as successrate from streaming where url = '/item/buy' or url = '/item/cart' group by userId " \
      "order by successrate desc limit 2221 "
cursor.execute(sql)
rr = cursor.fetchall()
Robot2 = []
result2 = []
for row in rr:
    print(row)
    Robot2.append({"userID": row[0]})
    result2.append(row[0])

'''for i in result2:
    try:
        sql_delete = "DELETE FROM streaming_old WHERE userId= '%s'"
        cursor.execute(sql_delete, i)
        myConnection.commit()
        print("done"+str(i))
    except Exception as e:
        print(e)
'''
cursor.close()

print("____________________________________________")

# 获取刷单机器人
cursor = myConnection.cursor()
print(myConnection)
print(cursor)
# sql1 = "SELECT * FROM streaming_old WHERE timestampdiff(MINUTE, SYSDATE(), send_time) <=3 AND timestampdiff(MINUTE, SYSDATE(), send_time) >= 0 "
sql2 = "SELECT distinct userId, count(itemId) as num from streaming where url = '/item/buy' group by userId order by num desc limit 4835 "
cursor.execute(sql2)
rr = cursor.fetchall()
Robot3 = []
result3 = []
for row in rr:
    print(row)
    Robot3.append({"userID": row[0]})
    result3.append(row[0])

'''for i in result3:
    try:
        sql_delete = "DELETE FROM streaming_old WHERE userId= '%s'"
        cursor.execute(sql_delete, i)
        myConnection.commit()
        print("done"+str(i))
    except Exception as e:
        print(e)'''

cursor.close()
print("____________________________________________")

# 获取爬虫机器人
cursor = myConnection.cursor()
print(myConnection)
print(cursor)
sql = "SELECT ipAddr, count(distinct userId) as num from streaming where url = '/user/login' group by ipAddr order by num desc " \
      "limit 100 "

cursor.execute(sql)
rr = cursor.fetchall()
Robot4 = []
result4 = []
for row in rr:
    print(row)
    sql2 = "SELECT distinct userId from streaming where ipAddr =" + "'" + str(row[0]) + "'"
    # print(sql2)
    cursor.execute(sql2)
    rrtemp = cursor.fetchall()
    for row1 in rrtemp:
        # print(row1)
        Robot4.append({"userId": row1[0]})
        result4.append(row1[0])

'''for i in result4:
    try:
        sql_delete = "DELETE FROM streaming_old WHERE userId= '%s'"
        cursor.execute(sql_delete, i)
        myConnection.commit()
        print("done"+str(i))
    except Exception as e:
        print(e)
'''
cursor.close()


with open("../frontend/src/data/streaming_old.json", 'w+') as f:
    jsonObject = {'Robot1': Robot1, 'Robot2': Robot2, 'Robot3': Robot3, 'Robot4': Robot4}
    f.write(json.dumps(jsonObject))
    f.close()
