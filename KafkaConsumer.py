from kafka import TopicPartition, KafkaConsumer,KafkaProducer
from Utilities import *
#consumer = KafkaConsumer('snowtopic',bootstrap_servers=['localhost:9092'])
Index=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['KAFKA_TOPIC_SNOWDB'])
consumer = KafkaConsumer(configdata['KAFKA'][Index]['TOPIC'],bootstrap_servers=eval(configdata['KAFKA'][Index]['BOOTSTRAP_SERVERS']))	
for message in consumer:	
	print(message.value.decode('utf-8'))

### From Here Write Code To Send Data Into Hive Tables
	#SnowPushViaKafka(1,message.value.decode('utf-8'))
	#listd=eval(message.value.decode('utf-8'))
	#for j in listd:
	#	print(j)
