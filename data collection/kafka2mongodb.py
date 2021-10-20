from kafka import KafkaConsumer
import pymongo

consumer = KafkaConsumer(
    'foobar',
    bootstrap_servers='172.29.4.17:9092',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='student',
    sasl_plain_password='nju2021',
)

# 多个 consumer 可以重复消费相同的日志，每个 consumer 只会消费到它启动后产生的日志，不会拉到之前的余量
dataClient = pymongo.MongoClient(host="localhost:27017", username="root", password="hyzyj2007")
db = dataClient['dataIntegration']
collection = db['robots']

for msg in consumer:
    line = msg.value.decode("utf-8")
    collection.insert_one({"value": line})
