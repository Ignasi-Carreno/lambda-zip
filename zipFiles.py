import os
import tempfile
from io import BytesIO, StringIO
from zipfile import ZipFile, ZIP_DEFLATED
from botocore.exceptions import ClientError
from datetime import datetime

"""
Zip files and upload it to s3
"""
def zipFiles(s3_resource, bucket, files_to_zip, fileMonth, folder):

    MAX_SIZE = 314572800 # -> 300 mb
    
    currentSizeBytes = 0
    tempFile = tempfile.NamedTemporaryFile('w', suffix='.tar.gz', delete=False)
    zipFile = ZipFile(tempFile.name, 'w', compression=ZIP_DEFLATED, allowZip64=True)
    fileName =  fileMonth.strftime('%Y-%m') + '_' + str(int(datetime.now().timestamp()))
    
    # we download all files to tmp directory of lambda for that we create directory structure in /tmp same as s3 files structure (subdirectory)
    for KEY in files_to_zip:
      try:
          local_file_name = '/tmp/'+KEY
          if not os.path.isdir(os.path.dirname(local_file_name)):
            print("Making directory " + os.path.dirname(local_file_name))
            os.makedirs(os.path.dirname(local_file_name))
  
          print('Downloading next file: ' + KEY)
          s3_resource.Bucket(bucket).download_file(KEY, local_file_name)
          
          currentSizeBytes += os.path.getsize(local_file_name)
          print('Current Size: ' + str(currentSizeBytes))
          
          if currentSizeBytes >= MAX_SIZE:
            print('Max size reached, we will create new zip file')
            zippedFileName = 'zipped/' + folder + fileName +'.tar.gz'
            
            print('Uploading current zip')
            s3_resource.meta.client.upload_file(tempFile.name, bucket, zippedFileName)
            
            print('Removing current zip')
            currentSizeBytes = 0
            zipFile.close()
            tempFile.close()
            os.remove(tempFile.name)
            
            print('Making new zip')
            tempFile = tempfile.NamedTemporaryFile('w', suffix='.tar.gz', delete=False)
            zipFile = ZipFile(tempFile.name, 'w', compression=ZIP_DEFLATED, allowZip64=True)
            fileName = fileMonth.strftime('%Y-%m') + '_' + str(int(datetime.now().timestamp()))
          
          zipFile.write(local_file_name)
          os.remove(local_file_name)
          
      except ClientError as e:
          print(e.response)
    
    #At the end of the bucle we also sent the last zip file to s3
    print('Uploading last zip')
    zippedFileName = 'zipped/' + folder + fileName +'.tar.gz'
    s3_resource.meta.client.upload_file(tempFile.name, bucket, zippedFileName)
    zipFile.close()
    tempFile.close()
