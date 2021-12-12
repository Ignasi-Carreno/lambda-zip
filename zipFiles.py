import os
import tempfile
from io import BytesIO, StringIO
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime

"""
Zip files and upload it to s3
"""
def zipFiles(s3_client, s3_resource, bucket, files_to_zip, fileMonth, folder):

    fileName =  fileMonth.strftime('%Y-%m') + '_' + str(int(datetime.now().timestamp()))
    zippedFileName = 'zipped/' + folder + fileName +'.tar.gz'
    archive = BytesIO()
    
    with ZipFile(archive, 'w', compression=ZIP_DEFLATED) as zip_archive:
      for file in files_to_zip:
        with zip_archive.open(file, 'w') as file1:
          file1.write((s3_resource.Object(bucket, file).get())['Body'].read())
    
    archive.seek(0)
    s3_client.upload_fileobj(archive, bucket, zippedFileName)
    archive.close()
