from kafka import TopicPartition, KafkaConsumer,KafkaProducer
from SalesForce import *

while True:
	tableData=GetApiSalesForce(
	configdata['SALESFORCE']['GETAPI']['GetAccountDetail']['EndPointURL']+
	configdata['SALESFORCE']['GETAPI']['GetAccountDetail']['QueryOrHeaderParameters'])

	#GetDataFromMYSQL('')
	#print(configdata['KAFKA'][0]['BOOTSTRAP_SERVERS'])
	#producer=KafkaProducer(bootstrap_servers=['localhost:9092'])
	#future=producer.send('snowtopic',key=b'TableData',value=tableData.encode('utf-8'))
	Index=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['KAFKA_TOPIC_SNOWDB'])
	producer=KafkaProducer(bootstrap_servers=eval(configdata['KAFKA'][Index]['BOOTSTRAP_SERVERS']))
	#future=producer.send(configdata['KAFKA'][Index]['TOPIC'],key=b'TableData',value=tableData.encode('utf-8'))
	future=producer.send(configdata['KAFKA'][Index]['TOPIC'],key=b'TableData',value=str(tableData).encode('utf-8'))
	#future=producer.send(configdata['KAFKA'][Index]['TOPIC'],key=b'TableData',value=str(tableData['records'][0]['Name']).encode('utf-8'))
	future.get(timeout=10)
	time.sleep(5)
	#producer.flush()
	#print(tableData)
