import boto3
import csv

try:
    s3 = boto3.resource('s3',
        region_name='us-east-1',
        aws_access_key_id=MY_ACCESS_KEY,
        aws_secret_access_key=MY_SECRET_KEY
    )
except:
    print("this may already exist")

#create a bucket
try:
    s3.create_bucket(Bucket='datacont-name', CreateBucketConfiguration={
        'LocationConstraint': 'us-east-1'})
except:
    print("this may already exist")
bucket = s3.Bucket("cloud-hw2")
bucket.Acl().put(ACL='public-read')

#upload a new object into the bucket
body = open(r'C:\Users\meizh\Downloads\1501\test.txt', 'rb')
o = s3.Object('cloud-hw2', 'test.txt').put(Body=body )
s3.Object('cloud-hw2', 'test.txt').Acl().put(ACL='public-read')


# Now, we will create DynamoDB table
# Firstly, creat a DB
dyndb = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=MY_ACCESS_KEY,
    aws_secret_access_key=MY_SECRET_KEY)

try:
 table = dyndb.create_table(
     TableName='DataTable',
     KeySchema=[
         {
             'AttributeName': 'PartitionKey',
             'KeyType': 'HASH'
          },
         {
             'AttributeName': 'RowKey',
             'KeyType': 'RANGE'
         }
     ],
     AttributeDefinitions=[
         {
             'AttributeName': 'PartitionKey',
             'AttributeType': 'S'
         },
         {
             'AttributeName': 'RowKey',
             'AttributeType': 'S'
         },
     ],
     ProvisionedThroughput={
         'ReadCapacityUnits': 5,
         'WriteCapacityUnits': 5
     }
 )
except:
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)



table = dyndb.Table("DataTable")
with open(r'c:\users\meizh\Downloads\1660\experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    #count=50
    for item in csvf:
        body = open(r'c:\users\meizh\Downloads\1660\\' + item[3], 'rb')
        s3.Object('cloud-hw2', item[3]).put(Body=body)
        md = s3.Object('cloud-hw2', item[3]).Acl().put(ACL='public-read')

        url = "https://cloud-hw2.s3.amazonaws.com/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1] , 'description' : item[4], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")
       # count+=1
response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
print(response['Item'])
