import sys
import boto3
import json
import gzip

# enter your region, s3 bucket, amount of exports to show
region = 'us-east-1' 
s3Bucket = 'YOURBUCKETNAME'

#max exports to choose to restore
MaxExportsList = 5

# Use Amazon S3 and DynamoDB client
s3 = boto3.client('s3')
dynamo = boto3.client('dynamodb', region_name=region)

# Input the table name
DynamoDB_table = input('Enter the table name: ')

# Describe the table to get the Arn
try:
    descTable = dynamo.describe_table(TableName=DynamoDB_table)
except:
    sys.exit('[ERROR] There was an issue finding that table\'s exports')

tableArn = descTable['Table']['TableArn']

# Get the list of exports from dynamo default set to 5
exportList = dynamo.list_exports(
    TableArn=tableArn,
    MaxResults=MaxExportsList
)

# Print export list of exports for this table
index = 0
print('Choose an Export: ')

for export in exportList['ExportSummaries']:

    desc = dynamo.describe_export(ExportArn=export['ExportArn'])
    status = desc['ExportDescription']['ExportStatus']
    date = desc['ExportDescription']['ExportTime']
    print(str(index)+') '+status+' '+date.strftime('%x %X'))
    index = index+1
    s3Folder = desc['ExportDescription']['ExportManifest'].replace(
        "manifest-summary.json", "data/")
    s3Bucket = desc['ExportDescription']['S3Bucket']
    export.update({'s3Folder': s3Folder})
    export.update({'s3Bucket': s3Bucket})

# Allow user to input the export number
userChoice = int(input('Enter the number:'))
if userChoice > MaxExportsList:
    sys.exit('That export does not exist please try again later... ')
# debug
# print(exportList['ExportSummaries'][userChoice]['s3Bucket'])
# print(exportList['ExportSummaries'][userChoice]['s3Folder'])

# Get the objects in the folder for that export
# if you have multiple tables in the same data folder (not common) this will not work and this script should be modified
selectedBucket = exportList['ExportSummaries'][userChoice]['s3Bucket']
selectedFolder = exportList['ExportSummaries'][userChoice]['s3Folder']
export_objects = s3.list_objects_v2(
    Bucket=selectedBucket, Prefix=selectedFolder)
# debug
# print(export_objects)

# ask user if table should be deleted before restore. 
# this is a data export only so it won't drop the table and start fresh
delTable = ''
while delTable.lower() not in ('y', 'n'):
    delTable = input(
        'Make sure to delete all items from table to start fresh. Have you done this? (y/n): ').lower()


# Restore the table
for export_object in export_objects['Contents']:

    # Read objects
    s3_data_obj = s3.get_object(Bucket=s3Bucket, Key=export_object['Key'])

    # Import data into DDB using BatchWrite
    data = []
    with gzip.open(s3_data_obj['Body'], mode='rt') as items:
        for line in items:
            item = json.loads(line)
            data.append({'PutRequest': item})
    print(data)
    # Batch write in batches of 20
    for i in range(0, len(data), 20):
        sub_arr = data[i:i + 20]
        writeReturned = dynamo.batch_write_item(
            RequestItems={DynamoDB_table: sub_arr})
        if len(writeReturned['UnprocessedItems']):
            print('Restored with msg: \n' +
                  str(writeReturned['UnprocessedItems']))
        else:
            print('All Items processed successfully')
