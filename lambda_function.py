import boto3
from io import BytesIO, StringIO
from botocore.exceptions import ClientError
from datetime import datetime
from zipFiles import zipFiles
from pathlib import Path
from dateutil.relativedelta import *

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket='icsphotos'
    folder = event['folder']
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
    allFiles = response['Contents']
    
    files_to_zip_dic = selectFilesToZip(allFiles, folder)

    for month in files_to_zip_dic:
      zipFiles(s3_client, s3_resource, bucket, files_to_zip_dic[month], month, folder)
      for file in files_to_zip_dic[month]:
        try:
          print('Deleting ' + file)
          #s3_resource.Object(bucket, file).delete()
        except ClientError as e:
          print(e.response)

        
    print('All files zipped successfully!')
    
    
def selectFilesToZip(allFiles, folder):
  
    #counter = 0
    today = datetime.today()
    currentMonth = datetime(today.year, today.month, 1)
    files_to_zip_dic = {}

    for i in allFiles:
      fileName = str(i['Key'])
      
      if fileName == folder:
        continue
      
      try:
        fileDate = getFileDateTime(fileName)
      except ValueError as exp:
        print ('Invalid fileName ' + fileName)
        continue
      
      if fileDate >= currentMonth:
        continue
      
      #counter+= 1
      #if counter > 5:
      #  break
      
      fileMonth = datetime(fileDate.year, fileDate.month, 1)
      
      if fileMonth in files_to_zip_dic:
        files_to_zip = files_to_zip_dic[fileMonth]
      else:
        files_to_zip = []

      files_to_zip.append(fileName)
      
      files_to_zip_dic.update({fileMonth : files_to_zip})
      

    return files_to_zip_dic
    
def getFileDateTime(fileName):
  
  filenameWithoutExtension = Path(fileName).stem
  filenameWithoutExtension = filenameWithoutExtension.split("(")[0]
  filenameWithoutSpaces = filenameWithoutExtension.split(" ")[0]
  splitedByUnderscore = filenameWithoutSpaces.split("_")
  
  if "IMG" in fileName:
    date = splitedByUnderscore[0]
    date_time_obj = datetime.strptime(date, 'IMG%Y%m%d%H%M%S')
  elif "VID" in fileName:
    date = splitedByUnderscore[0]
    date_time_obj = datetime.strptime(date, 'VID%Y%m%d%H%M%S')
  elif "_" in filenameWithoutExtension:
    date = splitedByUnderscore[0] + splitedByUnderscore[1]
    date_time_obj = datetime.strptime(date, '%Y%m%d%H%M%S')
  else:
    raise ValueError()
    
  #print ("The date is", date_time_obj)
  
  return date_time_obj
