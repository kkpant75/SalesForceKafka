import flask
import socket
from flask import request, jsonify
from SnowFlakeDatabaseOperations import *
	
app = flask.Flask(__name__)
app.config["DEBUG"] = True

configdata['NonMDMSearch']=''

@app.route('/', methods=['GET'])
def home():
	return "<body bgcolor=33daff><h1 style='color: blue;text-align:center;'>Welcome To "+socket.gethostname()+" "+request.method+"</h1><h2 style='color: red;text-align:center;' >Welcome To Web View Of SnowFlake WH Data</h2></body>"


#############################################SnowFlake APIS###############################################
  
@app.route('/cust360/v1/entity/search/<searchText>', methods=['GET'])
def api_GetMethodForCust360EntityWebSearch(searchText):	
	
	if len(searchText)<=1 and int(searchText) not in range(0,9):
		return "<h1 style='color: blue;text-align:center;'>Error : <br><br>Please Provide <br><br>Valid SearchString In Web URL/Page</h1>"
	
	if len(configdata['ConditionColumn'])==0:
		entityData=""
	else:
		#entityData=GetEntityDetailsBySearchString(searchText,"Individual")
		entityData=GetEntityDetailsBySearchStringGeneric(searchText,configdata['EntityName'])
	#print("in api call",searchText,entityData,len(entityData))
	#return ("<h4 style='color: blue;'>"+entityData+"</h4>")
	if len(entityData)==0:
		return "<body bgcolor=powderblue><h1 style='color: red;text-align:center;'><br><br>No Records Available For Searched String{"+searchText+"}</h1></body>"
	
	HTMLPageDetial=""
	#entityData=json.loads(json.dumps(entityData))
	Count=1
	
	if configdata['WEBAPI']['WebContentDisplayType'].upper()!="TABLE":
		for listdict in entityData:			
			HTMLPageDetial+='<h2 style="color: blue;text-align:left;">Record# '+str(Count)+'</h2>'
			for key,value in listdict.items():
				#print('key',key,'value',value,len(str(value)),len(searchText))
				if searchText.upper() in str(value).upper().strip() :
					HTMLPageDetial+='<label style="background-color:yellow;">'+key+' :</label> <input type="text" disabled size='+str(len(str(value)))+' value="'+str(value).replace('None',' NULL')	+'" style="background-color:Chartreuse ;color: blue;text-align:left;font-weight: bold;"><br>'
				else:
					HTMLPageDetial+='<label>'+key+' : </label> <input type="text" disabled size='+str(len(str(value)))+' value="'+str(value).replace('None',' NULL')	+'" style="background-color:snow ;color: blue;text-align:left;font-weight: bold;"><br>'
			Count+=1

######Table View
	if configdata['WEBAPI']['WebContentDisplayType'].upper()=="TABLE":		
		HTMLPageDetial="<head><style>table, th, td {  border: 1px solid blue; color: black;}</style></head>"
		HTMLPageDetial+='<table >'
		for listdict in entityData:			
			Tableheader="<tr><th>RecordNumber</th>"
			TableValues='<tr><td>'+str(Count)+'</td>'
			for key,value in listdict.items():
				if Count==1:
					Tableheader+='<th>'+key+'</th>'
		
				if searchText.upper() in str(listdict[key]).upper().strip() :
					TableValues+='<td style="background-color:#FFFF33">'+str(listdict[key])+'</td>'
				else:
					TableValues+='<td>'+str(listdict[key]).replace('None',' NULL')+'</td>'							
			if Count==1:
				HTMLPageDetial+=Tableheader+' </tr> '

			HTMLPageDetial+=TableValues+'</tr>'	
			Count+=1

		HTMLPageDetial+='</table>'	
		
			
	#print(HTMLPageDetial)	
	entityDataWeb="<body bgcolor=powderblue>"+HTMLPageDetial+"</body>"
	return (entityDataWeb)
	

@app.route('/cust360/v1/entity/GetData', methods=['POST'])
def GetMdmData():
	try:
		searchData="NoInput"
		ListSearchMap={}
		
		if configdata['WEBAPI']['EnableDataSearchAcrossAttributes'].upper()=="YES":
			
			configdata['WebFormAndTableColumnMapping']=json.loads(GetTableColumns(configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+configdata['EntityName']))

		#LogMessageInProcessLog("configdata['WebFormAndTableColumnMapping']"+str(configdata['WebFormAndTableColumnMapping']))
		
		for dbcolname,formFieldName in  configdata['WebFormAndTableColumnMapping'].items():
			if len(request.form[dbcolname])>0:
				searchData=request.form[dbcolname]
				ListSearchMap[dbcolname]=searchData
		configdata['ConditionColumn']=ListSearchMap
		#print('ConditionColumn Web..'+str(ListSearchMap))	
		return api_GetMethodForCust360EntityWebSearch(searchData)
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		configdata['NonMDMSearch']=""
		print("exception in dataserach:"+str(e)+configdata['EntityName'])
		return '<body style="background-color: powderblue;"><form action="DataSearch" method="post">'+'<input type="submit" value="Page Refresh Get"  style="float: left;  background-color: blue;color: white;"> </form></body>'


@app.route('/GetFullDataFromDBForSelectCombo/<ObjectType>', methods=['GET'])
def GetFullDataFromDBForSelectCombo(ObjectType):
	return GetCompleteDatabaseDetails(ObjectType)

@app.route('/GetFullTables/', methods=['GET'])
def GetFullTables():
	if 'Schema' in request.args:
		Schema=request.args['Schema']
	return GetCompleteDatabaseTableDetails(Schema)
	
		
###Gel Full Access
@app.route('/cust360/v1/entity/GetCompleteAccess', methods=['POST'])
def GetCompleteAccess():

	entityDataWeb="<html><body>\
		<h1 style='color: blue;text-align:left;'><br>Provide Details For Search</h1>"\
		'<form action="DataSearch" method="post">' \
		'<label  style="color: blue;text-align:left;font-weight: bold;" for="DatabaseName">Choose A Database Name:  &nbsp; &nbsp;</label>'\
		'<select name="DatabaseName" id="DatabaseName">'
	
	FormLayout=""
	
	Result=GetCompleteDatabaseDetails('DATABASE')['DATABASE']
	#print('Result..'+str(Result))
	for queryData in Result:
		FormLayout+='<option value="'+queryData+'" >'+queryData+'</option>'
		dbname=queryData
	FormLayout+='</select><br><br><label style="color: blue;text-align:left;font-weight: bold;" for="DatabaseScehma">Choose A Database Schema: </label>'\
		'<select name="DatabaseScehma" id="DatabaseScehma">'
	
	
	#dbname=request.form.get("DatabaseName")
	#print('dbname'+dbname)
	

	Result=GetCompleteDatabaseDetails('SCHEMA')['SCHEMA']
	for queryData in Result:
		FormLayout+='<option value="'+queryData+'" >'+queryData+'</option>'
	FormLayout+='</select><br><br><label  style="color: blue;text-align:left;font-weight: bold;" for="TableName">Choose A Database Table:  &nbsp; &nbsp;</label>'\
		'<select name="TableName" id="TableName">'
	
	initSchema=Result[0]
	Result=GetCompleteDatabaseTableDetails(initSchema)['TABLE']
	for queryData in Result:
		FormLayout+='<option value="'+queryData+'" >'+queryData+'</option>'
		
	entityDataWeb+=FormLayout+'</select><br><br>	<input type="submit" value=Submit> </form></body><html>'
	entityDataWeb+='<script>\
	let schema = document.getElementById("DatabaseScehma");\
	\
	schema.onchange=function() \
	{\
		fetch("/GetFullTables/?Schema="+schema.value).then(function(response){\
		response.json().then(function(data){\
		let tableNames="";\
		for (let tabl of data.TABLE)\
			{\
				tableNames+="<option>"+tabl+"</option>"\
			}\
		TableName.innerHTML=tableNames;\
		});\
	});\
	}\
	</script>'
	
	configdata['NonMDMSearch']='YES' # system internal variable
	
	return entityDataWeb

#	if(document.getElementById("TableName").value.length===0)\
#		alert("Please Select Schema For Full Access...");\

@app.route('/cust360/v1/entity/searchmdmjs', methods=['GET'])
def javascript():
	javscriptvar=\
	'<!DOCTYPE html>\
	<html>\
	<body>\
	<h3>A demonstration of how to access a SELECT element</h3>\
	<select id="mySelect" size="4">\
	<option>Apple</option>\
	<option>Pear</option>\
	<option>Banana123</option>\
	<option>Orange</option>\
	</select>\
	<br>\
	<select id="mySelect1">\
	<option>init</option>\
	</select>\
	<p>Click the button to get the number of option elements found in a drop-down list.</p>\
	<button onclick="myFunction()">Try it</button>\
	<p id="demo"></p>\
	<script>\
	var x = document.getElementById("mySelect");\
	x.onchange=function() {\
	\
	mySelect1.innerHTML="<option>"+x.value+"</option>";\
	}\
	</script>\
	</body>\
	</html>'
	return javscriptvar

@app.route('/cust360/v1/entity/searchmdmjs1', methods=['GET'])
def javascript1():
	javscriptvar=\
	'<!DOCTYPE html>\
	<html>\
	<head>\
	<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>\
	<script>\
	$(document).ready(function(){\
	  $("p").click(function(){\
 		$(this).hide();\
	  });\
	});\
	</script>\
	</head>\
	<body>\
\
	<p>If you click on me, I will disappear.</p>\
	<p>Click me away!</p>\
	<p>Click me too!</p>\
\
	</body>\
	</html>'
	return javscriptvar
	
@app.route('/cust360/v1/entity/DataSearch', methods=['GET','POST'])
def api_GetMethodForCust360EntityWebSearchInput():
	#global entityDataWeb
	#print("configdata['NonMDMSearch'].."+configdata['NonMDMSearch'])
	try:
		if configdata['NonMDMSearch'].upper()=="YES":
			#print("inside...")
			configdata['DB_NAME']=request.form.get('DatabaseName')
			configdata['DB_TABLE_PREFIX']='.'+request.form.get('DatabaseScehma')+'.'
			configdata['EntityName']=request.form.get('TableName')
			configdata['WEBAPI']['EnableDataSearchAcrossAttributes']='YES' ### for non defualt table search must be set to across
			configdata['NonMDMSearch']=""
			#print("request.form.get('TableName')...."+request.form.get('TableName'))
	
	# except Exception as e:
		# configdata['NonMDMSearch']=""
		# #configdata['DB_NAME']=configdataOrig['DB_NAME']
		# #configdata['DB_TABLE_PREFIX']=configdataOrig['DB_TABLE_PREFIX']
		# #configdata['EntityName']=configdataOrig['EntityName']		
		# print("exception in dataserach:"+str(e)+configdataOrig['EntityName'])
		# # return '<script type="text/javascript" src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>\
			# # <a id="link" href="/cust360/v1/entity/DataSearch">link text</a>\
			# # <script type="text/javascript">'\
			# # "$('a#link').click(function(){ fetch('/cust360/v1/entity/DataSearch') })\
		# # </script>"
		# return '<form action="DataSearch" method="post">'+'<input type="submit" value="Page Refresh"  style="float: right;"> </form>'
		# #return "<h1 style='color: red;text-align:center;'>Go Back Instead Of Reload </h1>"	
		
		# #return ('<html><body><form method=post action=GetUserSpecificRestricted></form></body><html>')
		# #return '<html><body><script> ("/cust360/v1/entity/DataSearch")</script></body><html>'
	
		SearchEntity=configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+configdata['EntityName']
		
		if configdata['WEBAPI']['EnableDataSearchAcrossAttributes'].upper()=="YES":
			configdata['WebFormAndTableColumnMapping']=json.loads(GetTableColumns(configdata['DB_NAME']+configdata['DB_TABLE_PREFIX']+configdata['EntityName']))

			#configdata['WebFormAndTableColumnMapping']=json.loads(GetTableColumns(configdata['EntityName']))
			print(configdata['WebFormAndTableColumnMapping'])	
		
		
		entityDataWeb="<html><body>"
		if configdata['EnableUserToControlSearch'].upper()=='YES':
			if configdata['EnableUserFullAccessToSearch'].upper()=='YES':
				entityDataWeb+='<form action="GetCompleteAccess" method="post">'+'<input type="submit" value="Search Another Table"  style="float: right;"> </form>'
			else:
				entityDataWeb+='<form action="GetUserSpecificRestricted" method="post">'+'<input type="submit" value="Search Another Table"  style="float: right;"> </form>'
		
		entityDataWeb+="<h1 style='color: blue;text-align:left;'><br>Provide Search Details For Any One Or Combination Of The Attributes For </h1>"\
			'<form action="GetData" method="post">' 
		entityDataWeb+="<h2 style='color: red;text-align:right;'>"+SearchEntity+"</h2>"
		if len(configdata['WebFormAndTableColumnMapping'])==0: 
			entityDataWeb+='<br>	<input type="submit" value="Search"  hidden><br><br>'
		else:
			entityDataWeb+='<br>	<input type="submit" value="Search" ><br><br>'
		
		FormLayout=""
		for dbcolname,formFieldName in configdata['WebFormAndTableColumnMapping'].items():
			FormLayout+='<label for="'+dbcolname+'" style="background-color:snow ;color: blue;text-align:left;font-weight: bold;">'+formFieldName+':</label><br>'\
			'<input type="text" id="'+dbcolname+'" name="'+dbcolname+'"><br>'
		
		if len(configdata['WebFormAndTableColumnMapping'])==0:    ## if no column appear for data search disable button
			entityDataWeb+=FormLayout+'<br>	<input type="submit" value="Search" disabled> </form>'
		else:
			entityDataWeb+=FormLayout+'<br>	<input type="submit" value="Search" > </form>'	
			
		entityDataWeb+='</body><html>'

		#print(entityDataWeb)
		return entityDataWeb
	except Exception as e:
		configdata['NonMDMSearch']=""
		print("exception in dataserach:"+str(e)+configdata['EntityName'])
		return '<form action="DataSearch" method="post">'+'<input type="submit" value="Page Refresh"  style="float: right;"> </form>'
	

###############################################################################################################    

app.run(socket.gethostname(),configdata['WEBAPI']['WebPort'],threaded=True)