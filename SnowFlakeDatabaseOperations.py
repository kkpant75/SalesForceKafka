from Utilities import *
	
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

	#if configdata['WEBAPI']['EnableDataSearchAcrossAttributes'].upper()=="YES":
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
			if configdata['WEBAPI']['EnableDataSerachWithLIKE'].upper()=="YES" and JsonColDataMap[dbcolname]  in Datatypes.split(','):
				condpart=condpart[::-1].replace('=',' EKIL ',1)[::-1][0:-1]+"%'"
				#print('if dbcolname..'+condpart)
			onlyonecolumn+=1
		else:
			if JsonColDataMap[dbcolname]  in Datatypes.split(','):
				condpart+=" AND "+dbcolname+" = '"+configdata['ConditionColumn'][dbcolname]+"'"
			else:
				condpart+=" AND "+dbcolname+" = "+configdata['ConditionColumn'][dbcolname]	
				#print('else top dbcolname..'+condpart)
			if configdata['WEBAPI']['EnableDataSerachWithLIKE'].upper()=="YES" and JsonColDataMap[dbcolname]  in Datatypes.split(','):
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
global DbConn
DBIndex=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['SNOWFLAKE1'])
DbConn = DBConnection('SNOWFLAKE',DBIndex)
configdata['DB_NAME']=configdata['DATABASE']['SNOWFLAKE'][DBIndex]['NAME']
configdata['DB_TABLE_PREFIX']="."+configdata['DATABASE']['SNOWFLAKE'][DBIndex]['SCHEMA']+"."
configdata['EntityName']=configdata['DATABASE']['SNOWFLAKE'][DBIndex]['TABLE_NAME']

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