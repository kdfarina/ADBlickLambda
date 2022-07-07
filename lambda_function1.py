import json
import boto3
import os
from osgeo import gdal

try:
    import ogr
    import osr
    import gdalconst
    import gdal
except:
    from osgeo import ogr
    from osgeo import osr
    from osgeo import gdalconst
    from osgeo import gdal
import code
import urllib.parse



        
def open_kmz(kmz_file):
    #print('------------------open_kmz(kmz_file)------------------')
    driver = ogr.GetDriverByName('KML')
    data_source = driver.Open(kmz_file, gdalconst.GA_ReadOnly)
    if data_source is None:
        print("Failed to open file." )
        #exit()
    print(data_source)

    return data_source

def set_output_filename(input_filename, geom_type):
    #print('------------------set_output_filename(input_filename, geom_type)------------------')
    # set the output filename by appending the geometry type to input filename
    dir, filename = os.path.split(input_filename)
    output_filename = os.path.splitext(filename)[0] + '_' + geom_type + '.shp'
    output_shapefile = os.path.join(dir, output_filename)
    return output_shapefile

def create_output_datastore(shp_name, bucket):
    #print('------------------create_output_datastore(shp_name)------------------')
    driver = ogr.GetDriverByName('ESRI Shapefile')
    # remove the shapefile if it already exists
    
    if key_exists(shp_name, bucket):
        print("Removing previous version of %s" % shp_name)
        s3.delete_object(Bucket=bucket, Key=shp_name)
        
    #if os.path.exists(shp_name):
    #    print("Removing previous version of %s" % shp_name)
    #    os.remove(shp_name)

    # create the shapefile
    try:
        output_datastore = driver.CreateDataSource(shp_name)
    except:
        print("Could not create shapefile %s ." % shp_name)
    return output_datastore

def create_output_layer(datastore, geom_type):
    #print('------------------create_output_layer(datastore, geom_type)------------------')
    # create the output layer with SRS from input
    print(datastore)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    new_pts_layer = datastore.CreateLayer('layer1', geom_type = geom_type, srs=srs)

    # if error creating layer, print message and exit
    if new_pts_layer is None:
        print('Error creating layer.' )
        #sys.exit(1)
    return new_pts_layer



def add_fields(layer):
    # add standard KMZ fields to an existing layer
    #print('------------------add_fields(layer)------------------')
    fields = {
    'Name': 50,
    'description': 128,
    'icon': 10,
    'snippet': 128,
    'layer_name': 50
    }
    
    
    field = ogr.FieldDefn('id', ogr.OFTInteger)   
    layer.CreateField(field)
     

    for field_name, field_length in fields.items():
        field = ogr.FieldDefn(field_name, ogr.OFTString)
        field.SetWidth(field_length)
        layer.CreateField(field)

def prefix_exits(bucket, prefix):
    #print('------------------prefix_exits(bucket, prefix)------------------')
    s3_client = boto3.client('s3')
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in res


def key_exists(mykey, mybucket):
    #print('------------------key_exists(mykey, mybucket)------------------')
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=mybucket)
    if response:
        for obj in response['Contents']:
            if mykey == obj['Key']:
                return True
    return False



def lambda_handler(event, context):
    
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "FALSE")
    #gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE","YES")
    #gdal.SetConfigOption('CPL_CURL_VERBOSE', 'YES')
    #gdal.SetConfigOption('CPL_DEBUG','YES')
    ##gdal.SetConfigOption('AWS_VIRTUAL_HOSTING','FALSE')
    ##gdal.SetConfigOption('AWS_S3_ENDPOINT', 'localhost:9000')
    ##gdal.SetConfigOption('AWS_HTTPS', 'NO')
    gdal.SetConfigOption('CPL_VSIL_CURL_NON_CACHED',"/vsis3/")


    bucket = event['Records'][0]['s3']['bucket']['name']

    s3key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'],encoding = 'utf-8')
    
    kmz_file='/vsis3/{0}/{1}'.format(bucket, s3key)
    
    
    if ((kmz_file.split('.')[1]) == 'kml'):
        #print('------------------kmz_converter(pkmz_file)------------------')
        # open the input KMZ file
        data_source = open_kmz(kmz_file)
    
        # create the output shapefiles
        points_shp_name = set_output_filename(kmz_file, 'points')   
        lines_shp_name = set_output_filename(kmz_file, 'lines')   
        polygons_shp_name = set_output_filename(kmz_file, 'polygons')
        
        points_datastore = create_output_datastore(points_shp_name, bucket)
        points_layer = create_output_layer(points_datastore, ogr.wkbMultiPoint)

        if points_layer is not None:
            add_fields(points_layer)
            
        points_layer2 = create_output_layer(points_datastore, ogr.wkbNone)
        if points_layer2 is not None:
            add_fields(points_layer2)
         
        lines_datastore = create_output_datastore(lines_shp_name, bucket)
        lines_layer = create_output_layer(lines_datastore, ogr.wkbMultiLineString)

        if lines_layer is not None:
            add_fields(lines_layer)

        polygons_datastore = create_output_datastore(polygons_shp_name, bucket)
        polygons_layer = create_output_layer(polygons_datastore, ogr.wkbMultiPolygon)

        if polygons_layer is not None:
            add_fields(polygons_layer)

        # loop through the layers
        feature_counter = 0
        points_counter = 0
        lines_counter = 0
        polygons_counter = 0
        
        layer_count = data_source.GetLayerCount()

        for i in range(0, layer_count):
            layer = data_source.GetLayer(i)
            layer_info = {}
            layer_info['feature_count'] = layer.GetFeatureCount()
            layer_info['name'] = layer.GetName()

            # loop through the features in each layer
            for feature in layer: 
                feature_counter += 1
                geom = feature.GetGeometryRef()

                if (geom != None):
                    geom_type = geom.GetGeometryName()
                else: 
                    geom_type=''
                    
                field_names = ['Name', 'descriptio', 'icon', 'snippet']
    
                if geom_type in ('POINT', 'MULTIPOINT'):                
                    points_counter += 1
                    if points_layer is not None:
                        #daLayer = points_layer.GetLayer(0)
                        #layer_defn = daLayer.GetLayerDefn()
                        layer_defn = points_layer.GetLayerDefn()
                        out_feature = ogr.Feature(layer_defn)
                        out_geom = ogr.ForceToMultiPoint(geom)
                    #else:
                    #    continue
    
                elif geom_type in ('LINESTRING', 'MULTILINESTRING'):
                    lines_counter += 1
                    if lines_layer is not None:
                        #daLayer = lines_layer.GetLayer(0)
                        #layer_defn = daLayer.GetLayerDefn()
                        layer_defn = lines_layer.GetLayerDefn()
                        out_feature = ogr.Feature(layer_defn)
                        out_geom = ogr.ForceToMultiLineString(geom)
                    #else:
                    #    continue
    
                elif geom_type in ('POLYGON', 'MULTIPOLYGON'):
                    polygons_counter += 1
                    if polygons_layer is not None:
                        daLayer = polygons_layer.GetLayer(0)
                        layer_defn = daLayer.GetLayerDefn()
                    
                        #layer_defn = polygons_layer.GetLayerDefn()
                        out_feature = ogr.Feature(layer_defn)
                        out_geom = ogr.ForceToMultiPolygon(geom)
                    #else:
                    #    continue
                #else:
                #    continue          
    
                # convert to 2D
                out_geom.FlattenTo2D()
                # set the output feature geometry
                out_feature.SetGeometry(out_geom)           
    
                # set the output feature attributes
                for field_name in field_names:
                    try:
                        out_feature.SetField(field_name, feature.GetField(field_name))
                    except:
                        pass          
        
                out_feature.SetField('layer_name', layer.GetName())
                out_feature.SetField('id', feature_counter)
        
                # write the output feature to shapefile
                if geom_type in ('POINT', 'MULTIPOINT'):               
                    points_layer.CreateFeature(out_feature)
                elif geom_type in ('LINESTRING', 'MULTILINESTRING'):                
                    lines_layer.CreateFeature(out_feature)
                elif geom_type in ('POLYGON', 'MULTIPOLYGON'):               
                    polygons_layer.CreateFeature(out_feature)
                # clear the output feature variable
                out_feature = None
    
            # reset the layer reading in case it needs to be re-read later
            layer.ResetReading()
            
    
        #print counts
        print('\nSUMMARY COUNTS')
        print("Feature count: %s" % feature_counter)
        print("Points count: %s" % points_counter)
        print("Lines count: %s" % lines_counter)
        print("Polygons count: %s" % polygons_counter)
    
        # cleanup
        points_datastore = None
        points_layer = None
        lines_datastore = None
        lines_layer = None
        polygons_datastore = None
        polygons_layer = None

        # remove empty output shapefiles
        driver = ogr.GetDriverByName('ESRI Shapefile')

        if points_counter == 0:
            driver.DeleteDataSource(points_shp_name)
        if lines_counter == 0:
            driver.DeleteDataSource(lines_shp_name)
        if polygons_counter == 0:
            driver.DeleteDataSource(polygons_shp_name)
