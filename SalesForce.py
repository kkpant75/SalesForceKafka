from Utilities import *
		
def GetAuthAccessToken():
# Get Authorization token
	try:
		#global refreshToken
		AuthURL = configdata['SALESFORCE']['AUTHENTICATION']['AuthURL']+\
		'?username='	+configdata['SALESFORCE']['AUTHENTICATION']['UserName']+\
		'&password='		+configdata['SALESFORCE']['AUTHENTICATION']['Password']+\
		'&grant_type='	+configdata['SALESFORCE']['AUTHENTICATION']['GrantType']+\
		'&client_id='	+configdata['SALESFORCE']['AUTHENTICATION']['ClientID']+\
		'&client_secret='+configdata['SALESFORCE']['AUTHENTICATION']['ClientSecret']
		
		Headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
								
		requestRespose = requests.post(AuthURL,headers=Headers)
		response_dict=requestRespose.json()	
		accesstoken=response_dict['access_token']
		#refreshToken=response_dict['refresh_token']
		LogMessageInProcessLog('Access Token Starts At..'+str(datetime.datetime.fromtimestamp(int(response_dict['issued_at'])/1000)))
		return accesstoken
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(response_dict))

#print(GetAuthAccessToken()
def GetApiSalesForce(GetUrlEndPoint):
	responseGet=""
	try:		
		get_request = requests.get(GetUrlEndPoint, headers = {"Authorization":"Bearer " + GetAuthAccessToken(),'Content-Type' : 'application/json'})
		responseGet=dict(get_request.json())
		return responseGet#['mappingId']
	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(responseGet))#exit(0)


def PostApiSalesForce(PostUrlEndPoint,BodyPayLoad):
	try:		
		post_request=requests.post(PostUrlEndPoint,data=BodyPayLoad ,headers = {"Authorization":"Bearer " + GetAuthAccessToken(),"Content-Type" : "application/json"})

		responsePost=dict(post_request.json())
		return responsePost

	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		raise Exception(str(responsePost))#exit(0)


#print(
#GetApiSalesForce(configdata['SALESFORCE']['GETAPI']['GetAccountDetail']['EndPointURL']+
#configdata['SALESFORCE']['GETAPI']['GetAccountDetail']['QueryOrHeaderParameters']))

