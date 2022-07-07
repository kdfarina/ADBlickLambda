import json
import urllib.parse
from osgeo import gdal
from osgeo import ogr
from osgeo import gdalconst
from osgeo import osr
import boto3
import os
import code


def lambda_handler(event, context):
 
    bucket = event['Records'][0]['s3']['bucket']['name']
    s3key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "YES")
    gdal.SetConfigOption('CPL_VSIL_CURL_NON_CACHED','/vsis3/')
    gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES")

    
    kmz_file='/vsis3/{0}/{1}'.format(bucket, s3key)
    print(kmz_file)
    driver = ogr.GetDriverByName('KML')
    print(driver)
    data_source = driver.Open(kmz_file, gdalconst.GA_ReadOnly)
    print(data_source)
    
    print('ENTROOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')


        
 
    
