import pyodbc
import json
import boto3
import time
import datetime
import requests
import os
import zipfile
import shutil
import re
import sys
import math
import platform
#import xmltodict
from pytz import timezone
import logging

starttime=datetime.datetime.now()

with open('Configuration.json') as json_file:
	configdata=json.load(json_file)

if os.name=='nt':
	configdata['OSTypePath']="\\"
	#configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']=configdata['LOCAL_FILE_PATH_WINDOW']
elif os.name=='posix':
	configdata['OSTypePath']="/"
	### Get Kerberos Ticket To Secure Login
	os.system("echo "+configdata['Password']+"|kinit "+configdata['Username'].split('\\')[1])

if configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH'][0:2]=="//": 
	configdata['OSTypePath']="/"

EntityName=configdata['SYSTEM_PARAMETERS']['ENTITY_NAME']

def EpochTimeConversion(Time,Format):
	utc_time = datetime.datetime.strptime(Time, Format)
	epoch_time = (utc_time - datetime.datetime(1970, 1, 1)).total_seconds()
	#print(epoch_time)
	return int(epoch_time)
	

def DBConnection(DBType,Index):
	if configdata['SYSTEM_PARAMETERS']['CREDENTIAL']=='SECRET_MANAGER':
		configdata['DATABASE']['MSSQL'][Index]['PASSWORD']=GetDataFromSecretManager('DBPASSWORD')

	if DBType.upper()=='MSSQL':		
		return MSSQLConnection(Index)	
	elif DBType.upper()=='MYSQL':
		return MYSQLConnection(Index)
	elif DBType.upper()=='SNOWFLAKE':
		return SNOWFLAKEConnection(Index)

def MSSQLConnection(Index):
	try:
		if  configdata['DATABASE']['MSSQL'][Index]['AUTH_TYPE'].upper()=="WINLOGIN":
			cnxn= pyodbc.connect(r"Driver="+configdata['DATABASE']['MSSQL'][Index]['DRIVER']+";Server="+configdata['DATABASE']['MSSQL'][Index]['SERVER']+";Database="+configdata['DATABASE']['MSSQL'][Index]['NAME']+";Trusted_Connection=yes;")
			LogMessageInProcessLog("MSSQL Secure DB Connection Succesfull")
			return cnxn

		else:
			cnxn= pyodbc.connect(r"Driver="+configdata['DATABASE']['MSSQL'][Index]['DRIVER']+";Server="+configdata['DATABASE']['MSSQL'][Index]['SERVER']+";Database="+configdata['DATABASE']['MSSQL'][Index]['NAME']+';UID='+configdata['DATABASE']['MSSQL'][Index]['USERNAME']+';PWD='+ configdata['DATABASE']['MSSQL'][Index]['PASSWORD'])		
			LogMessageInProcessLog("MSSQL DB Connection Succesfull")
			return cnxn
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))

def MYSQLConnection(Index):
	import mysql.connector
	try:	
		cnxn= mysql.connector.connect(
		host=configdata['DATABASE']['MYSQL'][Index]['SERVER'],
		database=configdata['DATABASE']['MYSQL'][Index]['NAME'],
		user=configdata['DATABASE']['MYSQL'][Index]['USERNAME'],
		password=configdata['DATABASE']['MYSQL'][Index]['PASSWORD'])
		
		LogMessageInProcessLog("MYSQL DB Connection Succesfull")			
		
		return cnxn
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))


def SNOWFLAKEConnection(Index):
	try:
		cnxn= pyodbc.connect("DSN="+configdata['DATABASE']['SNOWFLAKE'][Index]['DSN']+';UID='+configdata['DATABASE']['SNOWFLAKE'][Index]['USERNAME']+';PWD='+ configdata['DATABASE']['SNOWFLAKE'][Index]['PASSWORD'])
	
		LogMessageInProcessLog("SnowFlake DB Connection Succesfull")
		return cnxn		
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))


### Print Log Message	
def LogMessageInProcessLog(LogMessage):	
	
	if configdata['PROCESS_LOG']['ENABLE_FILE_LOGGING'].upper()=='YES':
		logging.error(LogMessage)
	if configdata['PROCESS_LOG']['PRINT_LOG_MESSGAE_ON_CONSOLE'].upper()=='YES':
		print(LogMessage)
	#	print("calll insid..",LogMessage)

### Log Configuration 

def SetProcessLogConfiguration():
	LogFilePath=configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']+configdata['OSTypePath']+"Logs"+configdata['OSTypePath']
		
	LogLevel=configdata['PROCESS_LOG']['LOG_LEVEL'].upper().replace("ERROR","40").replace("WARNING","30").replace("INFO","20").replace("DEBUG","10") ##ERROR 40|WARNING	30|INFO	20|DEBUG 10
	
	if not os.path.exists(LogFilePath):
		os.makedirs(LogFilePath)	
	
	LogFileName=LogFilePath+configdata['PROCESS_LOG']['LOG_FILE_NAME']+starttime.strftime('%Y%m%d')+".txt"
	
	if 	os.path.exists(LogFileName):
		CurrentLogFileSize=os.stat(LogFileName).st_size
		FileCreatedDaysBack= (EpochTimeConversion(str(datetime.datetime.now().astimezone(timezone(configdata['SYSTEM_PARAMETERS']['TIMEZONE']))).split('.')[0],'%Y-%m-%d %H:%M:%S')- os.stat(LogFileName).st_ctime) //86400
	
		if (FileCreatedDaysBack  >=configdata['PROCESS_LOG']['LOG_FILE_RETENTION_PERIOD'] or CurrentLogFileSize >= configdata['PROCESS_LOG']['LOG_FILE_MAX_FILE_SIZE']):
			print("\nLog File Is Deleted Current File Size <{}> Has Crossed The Permiiited Limit <{}>Big Or Its <{}> Days Old In System -Created Date <{}>\n".format(CurrentLogFileSize,configdata['LOG_FILE_MAX_FILE_SIZE'],FileCreatedDaysBack,time.ctime(os.stat(LogFileName).st_ctime)))
			try:
				os.remove(LogFileName)
			except Exception as e:
				print(e)
		
	logging.basicConfig(filename=LogFileName,format='%(asctime)s %(message)s', filemode='a',level=int(LogLevel))

def GetDataFromMYSQL(QueryString):	
	try:
		data=[]
		coxn=DBConnection('MYSQL',GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['MYSQL1']))
		cursorr = coxn.cursor()
		cursorr.execute('SELECT * FROM world.city')
		for j in cursorr:
			data.append(j)
			#print(j)
			
		return str(data)[1:-1] ## send table data as values()tuple
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		coxn.close()

def SnowTest(idx):	
	coxn=DBConnection('SNOWFLAKE',idx)
	cursorr = coxn.cursor()
	try:
		#cursorr.execute("SELECT current_version()")
		#one_row = cursorr.fetchall()
		#print(one_row)
		
		#cursorr.execute("CREATE  DATABASE IF NOT EXISTS  SNOWDBKK")
		#cursorr.execute("CREATE  SCHEMA IF NOT EXISTS  SNOWDBKK.SNOWSCHEMAKK")
		#cursorr.execute("CREATE  TABLE IF NOT EXISTS  SNOWDBKK.SNOWSCHEMAKK.SNOWTABLEKK(Name Varchar(30),Age Number(10),City Varchar(20))")
		#cursorr.execute("INSERT INTO SNOWDBKK.SNOWSCHEMAKK.SNOWTABLEKK VALUES('Kamlesh',10,'Blr'),('Kamlesh1',101,'Blr1')")
		cursorr.execute("SELECT CC_CALL_CENTER_SK,CC_MANAGER,CC_MARKET_MANAGER from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER Limit 10")
		for j in cursorr.fetchall():
			print(j)
			
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		coxn.commit()
		coxn.close()
global ses
ses=0

def SnowPushViaKafka(idx,DataFromKafka):	
	global ses
	global coxn
	if ses==0:		
		coxn=DBConnection('SNOWFLAKE',idx)
		ses=1
	cursorr = coxn.cursor()
	try:
		
		cursorr.execute("CREATE  DATABASE IF NOT EXISTS  SNOWDBKK")
		cursorr.execute("CREATE  SCHEMA IF NOT EXISTS  SNOWDBKK.SNOWSCHEMAKK")
		cursorr.execute("CREATE  TABLE IF NOT EXISTS SNOWDBKK.SNOWSCHEMAKK.SNOWTABLEKK( \
			ID    Number(10), \
			NAME  Varchar(45), \
			COUNTRYCODE   Varchar(45), \
			DISTRICT Varchar(45),\
			POPULATION  Number(10))"
		)
		
		#for data in eval(DataFromKafka):
		#	print(str(data))
		cursorr.execute("INSERT INTO SNOWDBKK.SNOWSCHEMAKK.SNOWTABLEKK VALUES"+str(DataFromKafka))

			
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		coxn.commit()
		#coxn.close()

def CreteDynamicTableAsPerFileHeader(Index,FileName):
	with open(FileName, 'r') as file:
		global TableStructure
		TableStructure=""			
		for header_data in file.readlines():
			#print(header_data)
			for col in header_data.replace("\n","").split(configdata['DATABASE']['SNOWFLAKE'][Index]['FILE_FIELD_DELIMITER']):
				TableStructure+= col + "  varchar(100),"
			TableStructure="("+TableStructure[:-1]+")"
				#print(TableStructure)
			break
	
	global ses
	
	if ses==0:		
		global coxn	
		coxn=DBConnection('SNOWFLAKE',Index)
		ses=1
	cursorr = coxn.cursor()
	try:
		#/Create Database
		cursorr.execute("CREATE  DATABASE IF NOT EXISTS  "+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['NAME'])
		
		#Create Schema
		cursorr.execute("CREATE  SCHEMA IF NOT EXISTS  "+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA'])
		
		#Create Table 
		DynamicTableName=configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['TABLE_PREFIX']+FileName.split(".")[0].upper()
		
		cursorr.execute("CREATE  TABLE IF NOT EXISTS "+ DynamicTableName+ TableStructure)
		return DynamicTableName
		
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		coxn.commit()
		#coxn.close()

def LoadFileInStageArea(Index,FileName):
	DynamicTableName=CreteDynamicTableAsPerFileHeader(Index,FileName)
	
	global ses
	if ses==0:		
		global coxn	
		coxn=DBConnection('SNOWFLAKE',Index)
		ses=1
	cursorr = coxn.cursor()
	try:
	
		cursorr.execute("CREATE FILE FORMAT IF NOT EXISTS "+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['FILE_FORMAT_NAME']+\
			" TYPE = '"+configdata['DATABASE']['SNOWFLAKE'][Index]['FILE_FORMAT_TYPE']+\
			"' FIELD_DELIMITER= '"+configdata['DATABASE']['SNOWFLAKE'][Index]['FILE_FIELD_DELIMITER']+\
			"' SKIP_HEADER = 1;")
		   
		cursorr.execute("CREATE OR REPLACE STAGE "+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['STAGE_NAME']+\
		" FILE_FORMAT = "+\
			configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
			configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"."+\
			configdata['DATABASE']['SNOWFLAKE'][Index]['FILE_FORMAT_NAME']+";")
		
		#### Need to work on this how to automate the data load process using put command which need snowql
		LoadCommand=(configdata['DATABASE']['SNOWFLAKE'][Index]["SNOWSQL_CMD"]+\
		#" -a "+configdata['DATABASE']['SNOWFLAKE'][Index]['CONNECTION_URI'].split(".")[0]+\
		#" -u "+configdata['DATABASE']['SNOWFLAKE'][Index]['USERNAME']+\
		#" -P "+configdata['DATABASE']['SNOWFLAKE'][Index]['PASSWORD']+\
		#" -w "+configdata['DATABASE']['SNOWFLAKE'][Index]['WAREHOUSE']+\
		" -q "+'"Put file://'+configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']+FileName +\
		" @"+configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['STAGE_NAME']+'";')
		LogMessageInProcessLog(LoadCommand)
		os.system(LoadCommand)
		
		#### Copy stage data into table
		CopyQuery="COPY INTO "+DynamicTableName+\
		" FROM @"+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"."+\
		configdata['DATABASE']['SNOWFLAKE'][Index]['STAGE_NAME']+\
		"/"+FileName+";"
		
		LogMessageInProcessLog(CopyQuery)
		cursorr.execute(CopyQuery)

		'''
		CREATE FILE FORMAT mycsvformat
		   TYPE = "CSV"
		   FIELD_DELIMITER= ','
		   SKIP_HEADER = 1;
		   
		CREATE OR REPLACE STAGE my_stage    FILE_FORMAT = mycsvformat;

		Put file://C:\\Users\\kamlesh.pant\\Downloads\\result.csv @my_stage;

		Put file://D:\\SnowFlake-UR\\SNOWFLAKE\\indicity.csv @MYSQLSTAGE;
		
		copy into copysnowkk from '@mysqlstage/indicity.csv';

		create table copysnowkk as select * from snowtablekk where 1=2;

		select * From copysnowkk;
		'''
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(e))
	finally:
		cursorr.close()
		coxn.commit()
		#coxn.close()


def GetItemIndex(IndexForItem):	
	for k,v in configdata.items():	
		if isinstance(v,dict):
			#print(v,type(v))
			for k1,v1 in v.items():
				if isinstance(v1,list):	
					idx=0
					for litem in v1:					
						#print(litem)
						if isinstance(litem,dict):
							#print(litem)
							for k2,v2 in litem.items():
								#print(litem[k2])
								if IndexForItem in litem[k2]:
									print(litem[k2])
									return idx 
						idx+=1
		
		
		if isinstance(v,list):	
			idx=0
			for litem in v:					
				#print(litem)
				if isinstance(litem,dict):
					#print(litem)
					for k2,v2 in litem.items():
						#print(litem[k2])
						if IndexForItem in litem[k2]:
							print(litem[k2])
							return idx 
				idx+=1
		
						
#print(GetItemIndex('SnowflakeDSIIDriver'))
	
#######################API INTERFACE RELATED#####################3

def GetSelectQueryResultInJSON(SelectQuery):
	#DbConn = DBConnection('SNOWFLAKE',GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1']))
	Cursor = DbConn.cursor()

	ConcatJsonOutput=[]
	LogMessageInProcessLog(SelectQuery)
	
	ExtractedData=Cursor.execute(SelectQuery)	
	for j in ExtractedData.fetchall():
		#LogMessageInProcessLog(j)	
		ConcatJsonOutput.append(json.loads(j[0].replace("\n","")))
		
	Cursor.close()
	#DbConn.close()
	return (ConcatJsonOutput)

def GetCompleteDatabaseDetails(ObjectType):
	Index=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1'])
	#DBData=json.loads(GetSelectQueryResultInJSON("SELECT DISTINCT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS FOR JSON AUTO,INCLUDE_NULL_VALUES"))
	DBData=GetSelectQueryResultInJSON("SELECT OBJECT_CONSTRUCT ('TABLE_CATALOG',TABLE_CATALOG,'TABLE_SCHEMA',TABLE_SCHEMA,'TABLE_NAME',TABLE_NAME) FROM  "+configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+".INFORMATION_SCHEMA.COLUMNS ")#WHERE TABLE_SCHEMA='"+configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']+"'")
	if ObjectType.upper()=="DATABASE":
		return {'DATABASE':list(set([val['TABLE_CATALOG'] for val in DBData]))}
		
	if ObjectType.upper()=="SCHEMA":
		return {'SCHEMA':list(set([val['TABLE_SCHEMA'] for val in DBData]))}
	
	if ObjectType.upper()=="TABLE":
		configdata['TABLE']={}
		#return (list(set([val['TABLE_NAME'] for val in DBData])))
		dabasename=(list(set([val['TABLE_CATALOG'].upper() for val in DBData])))
		schema=(list(set([val['TABLE_SCHEMA'].upper() for val in DBData])))
		for val in DBData:
			if val['TABLE_CATALOG'].upper() in dabasename:
				if val['TABLE_SCHEMA'].upper() in schema:
					configdata['TABLE'].update({val['TABLE_SCHEMA']:val['TABLE_NAME']})
		#[configdata['TABLE'].update({val['TABLE_SCHEMA']:(list(set([val['TABLE_NAME'] for val in DBData if val['TABLE_SCHEMA'] '#== DBData[0]['TABLE_SCHEMA'] ])))}) for val in DBData]
		return configdata['TABLE']

def GetCompleteDatabaseTableDetails(Schema):
		Index=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1'])
		
		#DBData=json.loads(GetSelectQueryResultInJSON("SELECT DISTINCT TABLE_NAME FROM SNOWDBKK.INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_CATALOG='"+DBname+"' AND TABLE_SCHEMA='"+Schema+"' ORDER BY 1 FOR JSON AUTO,INCLUDE_NULL_VALUES"))
		
		DBData=GetSelectQueryResultInJSON("SELECT OBJECT_CONSTRUCT ('TABLE_CATALOG',TABLE_CATALOG,'TABLE_SCHEMA',TABLE_SCHEMA,'TABLE_NAME',TABLE_NAME) FROM "+configdata['DATABASE']['SNOWFLAKE'][Index]['NAME']+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='"+Schema +"'") #configdata['DATABASE']['SNOWFLAKE'][Index]['SCHEMA']
		
		tableList=list(set([val['TABLE_NAME'] for val in DBData]))
		tableList.sort()		
		return {"TABLE":tableList}

def GetTableColumns(TableName):
	#DBIndex=DBIndex=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1'])
	#DbConn = DBConnection('SNOWFLAKE',DBIndex)
	Cursor = DbConn.cursor()

	DBname=TableName.split('.')[0]
	Schema=TableName.split('.')[1]
	TableName=TableName.split('.')[2]
	#SelectQuery="SELECT Name FROM sys.columns WHERE object_id = OBJECT_ID('"+TableName+"')"
	SelectQuery="SELECT COLUMN_NAME FROM \
	(SELECT ORDINAL_POSITION,COLUMN_NAME FROM "+DBname+".INFORMATION_SCHEMA.COLUMNS WHERE \
	TABLE_SCHEMA='"+Schema+"' AND table_name='"+TableName+"' ORDER BY ORDINAL_POSITION ASC)"
	print(SelectQuery)	
	ColNmaes=""
	ExtractedData=Cursor.execute(SelectQuery)	
	for j in ExtractedData.fetchall():
		ColNmaes+='"'+j[0]+'"'+":"+'"'+str(j[0])+'",'
	Cursor.close()
	#DbConn.close()
	#print(ColNmaes)
	return ("{"+ColNmaes[0:-1]+"}")

def GetTableColumnsInDict(TableName):
	#DBIndex=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1'])
	#DbConn = DBConnection('SNOWFLAKE',DBIndex)
	Cursor = DbConn.cursor()
	
	TabFQDNDetails=TableName.split('.')
	SelectQuery="SELECT COLUMN_NAME,DATA_TYPE FROM "+TabFQDNDetails[0]+".INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_SCHEMA ='"+TabFQDNDetails[1].replace('[','').replace(']','')+"' And  TABLE_NAME='"+TabFQDNDetails[2]+"'"
	print(SelectQuery)
	ColDataMapDict={}
	ExtractedData=Cursor.execute(SelectQuery)	
	for j in ExtractedData.fetchall():
		ColDataMapDict[j[0]]=j[1]

	Cursor.close()
	#DbConn.close()
	return ColDataMapDict
	
def GetEntityDetailsBySearchStringGeneric(SearchString,EntityName):
	Datatypes="date,datetimeoffset,datetime2,smalldatetime,datetime,time,char,varchar,text"
	Datatypes+=",DATE,DATETIMEOFFSET,DATETIME2,SMALLDATETIME,DATETIME,TIME,CHAR,VARCHAR,TEXT"
	
	SearchString=SearchString.replace('"',"")
	
	#SelectQuery="SELECT  * FROM "+configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+EntityName+" WHERE "
	
	JsonColDataMap=GetTableColumnsInDict(configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+EntityName)
	
	SelectQuery="SELECT  OBJECT_CONSTRUCT ("
	QueryJsonString=""
	for col in JsonColDataMap.keys():
		QueryJsonString+=",'"+col+"',"+col
	
	SelectQuery+=QueryJsonString[1:]+") FROM "+configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+EntityName+" WHERE "
	
	#print('JsonColDataMap',JsonColDataMap)
		
	onlyonecolumn=0
	condpart=""

	#if configdata['EnableDataSearchAcrossAttributes'].upper()=="YES":
	#configdata['WebFormAndTableColumnMapping']=json.loads(GetTableColumns(configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+configdata['EntityName']))

	#configdata['ConditionColumn']={"COUNTRYCODE":"IND"}
	#print('ConditionColumn DB..'+str(configdata['ConditionColumn']))
	###test and remoeb ater
	
	LogMessageInProcessLog(configdata['WebFormAndTableColumnMapping'])

	
	for dbcolname in configdata['WebFormAndTableColumnMapping'].keys():
		#print(JsonColDataMap[dbcolname])
		if  dbcolname not in configdata['ConditionColumn'].keys():
			continue
		if onlyonecolumn==0:
			if JsonColDataMap[dbcolname]  in Datatypes.split(','):
				condpart+=dbcolname+" = '"+configdata['ConditionColumn'][dbcolname]+"'"	
			else:
				condpart+=dbcolname+" = "+configdata['ConditionColumn'][dbcolname]
			if configdata['EnableDataSerachWithLIKE'].upper()=="YES" and JsonColDataMap[dbcolname]  in Datatypes.split(','):
				condpart=condpart[::-1].replace('=',' EKIL ',1)[::-1][0:-1]+"%'"
				#print('if dbcolname..'+condpart)
			onlyonecolumn+=1
		else:
			if JsonColDataMap[dbcolname]  in Datatypes.split(','):
				condpart+=" AND "+dbcolname+" = '"+configdata['ConditionColumn'][dbcolname]+"'"
			else:
				condpart+=" AND "+dbcolname+" = "+configdata['ConditionColumn'][dbcolname]	
				#print('else top dbcolname..'+condpart)
			if configdata['EnableDataSerachWithLIKE'].upper()=="YES" and JsonColDataMap[dbcolname]  in Datatypes.split(','):
				condpart=condpart[::-1].replace('=',' EKIL ',1)[::-1][0:-1]+"%'"
				#print('else dbcolname..'+condpart)
	#condpart+=" FOR JSON AUTO,INCLUDE_NULL_VALUES"
	
	#condpart_case_insensitive=""
	#for part in condpart.split("AND")
	#	if 'LIKE' in part:
	#		condpart_case_insensitive+='upper('+condpart.split("LIKE")[0]+')'+' LIKE '+'upper('+	condpart.split("LIKE")[1]
	#	else:
	#		condpart_case_insensitive+=' AND '+ part
		
	SelectQuery+=condpart
	
	LogMessageInProcessLog('SelectQuery'+SelectQuery)
	QueryData=GetSelectQueryResultInJSON(SelectQuery)
	
	#LogMessageInProcessLog('QueryData'+str(QueryData))
	
	return (QueryData)	
####################MAIN#####################3

SetProcessLogConfiguration()

####Enable this when Snoflake DB is needed

#global DbConn
#DBIndex=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1'])
#DbConn = DBConnection('SNOWFLAKE',DBIndex)
#configdata['DB_NAME']=configdata['DATABASE']['SNOWFLAKE'][DBIndex]['NAME']
#configdata['DB_TABLE_PREFIX']="."+configdata['DATABASE']['SNOWFLAKE'][DBIndex]['SCHEMA']+"."
#configdata['EntityName']=configdata['DATABASE']['SNOWFLAKE'][DBIndex]['TABLE_NAME']

#DbConn.close()
#LogMessageInProcessLog(GetCompleteDatabaseDetails('DATABASE'))
#LogMessageInProcessLog(GetCompleteDatabaseDetails('SCHEMA'))
#LogMessageInProcessLog(GetCompleteDatabaseTableDetails('SNOWSCHEMAKK'))
#LogMessageInProcessLog(GetTableColumns('SNOWTABLEKK'))

#GetDataFromMYSQL('')
#SnowTest(1)
#SnowTest(0)
#LoadFileInStageArea(1,"indicity.csv")
#CreteDynamicTableAsPerFileHeader(1,'indicity.csv')
#print(GetItemIndex('127.0.0.1'))
#print(GetEntityDetailsBySearchStringGeneric('ind','SNOWTABLEKK'))