import os
import tempfile
from io import BytesIO, StringIO
from zipfile import ZipFile, ZIP_DEFLATED
from botocore.exceptions import ClientError
from datetime import datetime

"""
Zip files and upload it to s3
"""
def zipFilesOld(s3_resource, bucket, files_to_zip, fileMonth, folder):

    fileName =  fileMonth.strftime('%Y-%m') + '_' + str(int(datetime.now().timestamp()))
    
    # we download all files to tmp directory of lambda for that we create directory structure in /tmp same as s3 files structure (subdirectory)
    for KEY in files_to_zip:
        try:
            local_file_name = '/tmp/'+KEY
            if not os.path.isdir(os.path.dirname(local_file_name)):
              print("Making directory " + os.path.dirname(local_file_name))
              os.makedirs(os.path.dirname(local_file_name))
    
            s3_resource.Bucket(bucket).download_file(KEY, local_file_name)
        except ClientError as e:
            print(e.response)

    #now create empty zip file in /tmp directory use suffix .zip if you want 
    with tempfile.NamedTemporaryFile('w', suffix='.tar.gz', delete=False) as f:
      with ZipFile(f.name, 'w', compression=ZIP_DEFLATED, allowZip64=True) as zip:
        for file in files_to_zip:
          zip.write('/tmp/'+file)

    #once zipped in temp copy it to your preferred s3 location 
    zippedFileName = 'zipped/' + folder + fileName +'.tar.gz'
    s3_resource.meta.client.upload_file(f.name, bucket, zippedFileName)
