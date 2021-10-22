from Utilities import *
	
def GetFileFromSFTP(FtpAction,ConfIndex):
	import pysftp
	import datetime
	import zipfile

	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None   
	
	if FtpAction!="MOVE_FILE_TO_PROCESSED":
		LogMessageInProcessLog("Downloading File .....")
	### Data Process Start Time
	starttime=datetime.datetime.now()
	
	with pysftp.Connection(configdata['SFTP'][ConfIndex]['SERVER'], username=configdata['SFTP'][ConfIndex]['USERNAME'], password=configdata['SFTP'][ConfIndex]['PASSWORD'],cnopts=cnopts) as sftp:	
		#"SFTP_FILE_PATTERN": "^%Y%m.*_A.*\\.zip$",  ### use this pattern inf date based needed
		#searchPattern=datetime.datetime.now().strftime(configdata['SFTP']['SFTP_FILE_PATTERN'])
		searchPattern=configdata['SFTP'][ConfIndex]['FILE_PATTERN']
		#print(searchPattern)	
		fileList=[k for k in sftp.listdir(configdata['SFTP'][ConfIndex]['GET_PATH']) if  re.search(searchPattern, k)]
		
		if FtpAction!="MOVE_FILE_TO_PROCESSED":
			LogMessageInProcessLog("File For DownLoad..."+str(fileList))
		TargetPath=configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']+configdata['SYSTEM_PARAMETERS']['ENTITY_NAME']+configdata['OSTypePath']
		
		if not os.path.exists(TargetPath):
			os.makedirs(TargetPath)
		
		if len(fileList)>0:
			for curFile in fileList:
				if FtpAction=="MOVE_FILE_TO_PROCESSED":
					try:
						sftp.rename(configdata['SFTP'][ConfIndex]['GET_PATH'] + curFile, configdata['SFTP'][ConfIndex]['GET_PATH']+"/Processed/" + curFile)
						return
					except Exception as e:
						LogMessageInProcessLog("\n\nLooks Like File <{}> Already Processed Once - Please Check In Server <{}> And Rename It Or Delete It\t\t".format(curFile,configdata['SFTP'][ConfIndex]['SERVER'])+str(e))
						raise
				sftp.get(configdata['SFTP'][ConfIndex]['GET_PATH']+curFile,TargetPath+curFile)
		else:
			LogMessageInProcessLog("\n\nFile Not Available For Current Month/Year")
			exit(0)
	endtime=datetime.datetime.now()
	Unzippedfile=UnzippFile(TargetPath+curFile)
	LogMessageInProcessLog("Download Executiontime(seconds)...."+str((endtime-starttime).total_seconds()))
	return Unzippedfile
	
def PutFileInSFTP(SourcePath,TargetPath,ConfIndex):
	import pysftp
	import datetime
	import zipfile
	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None   
	LogMessageInProcessLog("Uploading File ....."+SourcePath)
	
	### Data Process Start Time
	starttime=datetime.datetime.now()
	
	with pysftp.Connection(configdata['SFTP'][ConfIndex]['SERVER'], username=configdata['SFTP'][ConfIndex]['USERNAME'], password=configdata['SFTP'][ConfIndex]['PASSWORD'],cnopts=cnopts) as sftp:	
		sftp.put(SourcePath,TargetPath)

	endtime=datetime.datetime.now()
	LogMessageInProcessLog("Upload Executiontime(seconds)...."+str((endtime-starttime).total_seconds()))

###SNS Message
def Publish(subject,EmailBodyContent):
	sns = boto3.client('sns',region_name=configdata['AWS']['AWS_REGION'])
	sns.publish(
		TargetArn=configdata['AWS']['AWS_SNS_ARN'],
		Subject= subject,
		Message= EmailBodyContent)
	return

def PutFileInS3(FileToUpload):
	starttime=datetime.datetime.now()

	LogMessageInProcessLog(f"Uploading <{FileToUpload}> File To S3 Bucket ..."+configdata['AWS'] ['S3_BUCKET_NAME']+ "/"+configdata['AWS']['S3_OBJECT_NAME'])
	try:
		s3 = boto3.client('s3',region_name=configdata['AWS']['AWS_REGION'])
		## avoid multiple files in case its failed in previous job
		boto3.resource('s3').Bucket(configdata['AWS'] ['S3_BUCKET_NAME']).objects.filter(Prefix=configdata['AWS']['S3_OBJECT_NAME']).delete()
		s3.upload_file(FileToUpload,configdata['AWS'] ['S3_BUCKET_NAME'], configdata['AWS']['S3_OBJECT_NAME']+"/"+FileToUpload.split(configdata['OSTypePath'])[-1])

		### in case yiu want implement multipart uplaod then use this code
		# from boto3.s3.transfer import TransferConfig
		# config = TransferConfig(multipart_threshold=1024 * 25, 
                        # max_concurrency=10,
                        # multipart_chunksize=1024 * 25,
                        # use_threads=True)
		# s3 = boto3.resource(service_name='s3', region_name=configdata['AWS']['AWS_REGION'])
		# s3.Object(configdata['AWS'] ['S3_BUCKET_NAME'], configdata['AWS'] ['S3_BUCKET_NAME']+"/"+FileToUpload.split(configdata['OSTypePath'])[-1]).upload_file(FileToUpload,Config=config)
		
		#data = open(FileToUpload, 'rb')
		#s3.Bucket(configdata['AWS'] ['S3_BUCKET_NAME']).put_object(Key=configdata['AWS'] ['S3_BUCKET_NAME']+"/test", Body=data
		endtime=datetime.datetime.now()
		LogMessageInProcessLog("S3 Upload Executiontime(seconds)...."+str((endtime-starttime).total_seconds()))

	except Exception as e:
		LogMessageInProcessLog("Exception Raiased in "+sys._getframe().f_code.co_name +"...."+str(e))
		return e

def GetFileFromFTP(MoveToProcessedFolder,ConfIndex,FileToMove):
	from ftplib import FTP
	ftp = FTP(configdata['FTP'][ConfIndex]['SERVER'])
	ftp.login (configdata['FTP'][ConfIndex]['USERNAME'],configdata['FTP'][ConfIndex]['PASSWORD'])	
	#ftp.retrlines('LIST')
	
	### Data Process start Time
	starttime=datetime.datetime.now()
	searchPattern=configdata['FTP'][ConfIndex]['FILE_PATTERN']
	
	#print('File....:',FileName)
	ftp.cwd(configdata['FTP'][ConfIndex]['GET_PATH'])

	if MoveToProcessedFolder=='MOVE_FILE_TO_PROCESSED':
		ftp.rename(configdata['FTP'][ConfIndex]['GET_PATH'] + FileToMove, configdata['FTP'][ConfIndex]['GET_PATH']+"/Processed/" + FileToMove)
		return

	fileList=[k for k in ftp.nlst() if  re.search(searchPattern, k)]
	LogMessageInProcessLog(fileList)
	if len(fileList)>0:		
		for fname in fileList:
			FileName=fname			
			LogMessageInProcessLog('Todayfile.....'+FileName)
			TargetPathFile=configdata['SYSTEM_PARAMETERS']['LOCAL_FILE_PATH']+FileName			
			file2Write=open(TargetPathFile, 'wb')
			ftp.retrbinary('RETR %s' % (FileName),file2Write.write,1024)
			return fileList
	else:
		LogMessageInProcessLog("\n\nFile Not Available For Current Month/Year")
		exit(0)			
	ftp.close()
	
	### Data Process end Time
	endtime=datetime.datetime.now()	
	LogMessageInProcessLog("Executiontime(seconds)...."+str((endtime-starttime).total_seconds()))

def GetFileFromFTPAndLoadInSnowFlake():
	FtpServerIndex=GetItemIndex(configdata['SEARCH_STRING_FOR_INDEX']['FTP_LOCAL'])
	FileList=GetFileFromFTP("GETFILE",FtpServerIndex,None)
	for file in FileList:
		LogMessageInProcessLog(file	)
		LoadFileInStageArea(GetItemIndex('mpa43641'),file)
		GetFileFromFTP('MOVE_FILE_TO_PROCESSED',FtpServerIndex,file) ##Move File In Processed
		
GetFileFromFTPAndLoadInSnowFlake()		
