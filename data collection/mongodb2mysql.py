import pymongo
import pymysql


def getValue(requestBody, key):
    try:
        try:
            return int(requestBody[key])
        except:
            return requestBody[key]
    except:
        return None


def mongo_test():
    myConnection = pymongo.MongoClient("mongodb://localhost:27017/")
    myDB = myConnection["homework2"]
    myCollection = myDB["streaming"]
    return myCollection

    # count = 0
    # rows = myCollection.find()
    # for document in rows:
    #     value = document["value"]
    #     if "[IPADDR=" in value:
    #         IPADDR = value.split("IPADDR=")[1].split("] [")[0]
    #     else:
    #         IPADDR = None
    #     SESSIONID = value.split("[SESSIONID=")[1].split("] ")[0]
    #     url = value.split(": uri=")[1].split(" | ")[0]
    #     if (url == "/user/login"):
    #         date = value.split(" DEBUG [")[0].split("] ")[2]
    #     else:
    #         date = value.split(" DEBUG [")[0].split("] ")[1]
    #     try:
    #         requestBody = eval(value.split("requestBody=")[1])
    #     except:
    #         requestBody = eval(value.split("requestBody = ")[1])
    #     userId = getValue(requestBody, "userId")
    #     itemId = getValue(requestBody, "itemId")
    #     categoryId = getValue(requestBody, "categoryId")
    #     isSecondKill = getValue(requestBody, "isSecondKill")
    #     password = getValue(requestBody, "password")
    #     authCode = getValue(requestBody, "authCode")
    #     success = getValue(requestBody, "success")
    #     print(count, IPADDR, SESSIONID, date, url, userId, itemId, categoryId, isSecondKill, password, authCode, success)
    #     count += 1


def mysql_test():
    myConnection = pymysql.connect(host="42.192.54.221", port=3306, user="root", password="123456", database="hive")
    return myConnection


if __name__ == '__main__':
    mongodb = mongo_test()
    mysql = mysql_test()
    cursor = mysql.cursor()

    # sql = "select * from buy_data limit 50"
    # cursor.execute(sql)
    # rows = cursor.fetchall()
    # for row in rows:
    #     print(row)

    count = 0
    for document in mongodb.find():
        value = document["value"]
        SESSIONID = value.split("[SESSIONID=")[1].split("] ")[0]
        url = value.split(": uri=")[1].split(" | ")[0]
        try:
            requestBody = eval(value.split("requestBody = ")[1])
        except:
            requestBody = eval(value.split("requestBody=")[1])
        try:
            date = value.split(" DEBUG [")[0].split("] ")[2]
        except:
            date = value.split(" DEBUG [")[0].split("] ")[1]
        if url == "/user/login":
            IPADDR = value.split("IPADDR=")[1].split("] [")[0]
            userId = getValue(requestBody, "userId")
            password = getValue(requestBody, "password")
            authCode = getValue(requestBody, "authCode")
            success = getValue(requestBody, "success")
            sql = "insert into streaming" \
                  "(id, ipAddr, sessionId, date, url, userId, password, authCode, success)" \
                  "values" \
                  "('%d','%s','%s','%s','%s','%d','%s','%s','%d')" % \
                  (count, IPADDR, SESSIONID, date, url, userId, password, authCode, success)
            try:
                cursor.execute(sql)
                mysql.commit()
            except:
                mysql.rollback()
        else:
            userId = getValue(requestBody, "userId")
            itemId = getValue(requestBody, "itemId")
            categoryId = getValue(requestBody, "categoryId")
            if url == "/item/buy":
                isSecondKill = getValue(requestBody, "isSecondKill")
                sql = "insert into streaming" \
                      "(id, sessionId, date, url, userId, itemId, categoryId, isSecondKill)" \
                      "values" \
                      "('%d','%s','%s','%s','%d','%d','%d','%d')" % \
                      (count, SESSIONID, date, url, userId, itemId, categoryId, isSecondKill)
                cursor.execute(sql)
                try:
                    mysql.commit()
                except:
                    mysql.rollback()
            else:
                sql = "insert into streaming" \
                      "(id, sessionId, date, url, userId, itemId, categoryId)" \
                      "values" \
                      "('%d','%s','%s','%s','%d','%d','%d')" % \
                      (count, SESSIONID, date, url, userId, itemId, categoryId)
                cursor.execute(sql)
                try:
                    mysql.commit()
                except:
                    mysql.rollback()
        count += 1
    mysql.close()
