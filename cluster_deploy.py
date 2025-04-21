import boto3
import json
from botocore.exceptions import ClientError
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

# AWS CREDENTIALS
KEY                = config.get("AWS","KEY")
SECRET             = config.get("AWS","SECRET")

# IAM ROLES
IAM_ROLE_NAME      = config.get("IAM_ROLE","IAM_ROLE_NAME")

# CLUSTER CONFIGURATIONS
CLUSTER_IDENTIFIER = config.get("INFRASTRUCTURE","CLUSTER_IDENTIFIER")
CLUSTER_TYPE       = config.get("INFRASTRUCTURE","CLUSTER_TYPE")
NODE_TYPE          = config.get("INFRASTRUCTURE","NODE_TYPE")
NUM_NODES          = config.get("INFRASTRUCTURE","NUM_NODES")

# DATABASE CONFIGURATIONS
DB_NAME            = config.get("CLUSTER","DB_NAME")
DB_USER            = config.get("CLUSTER","DB_USER")
DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DB_PORT            = config.get("CLUSTER","DB_PORT")

print("*******************************************")
print("Establishing boto3 iam and redshift clients")

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-east-1'
                  )

redshift = boto3.client('redshift',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name="us-east-1"
                       )

print("**********************************************************")
print("Creating IAM Role with permissions to use Redshift service")

try:
    dwhRole = iam.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)

print("************************************************")
print("Attaching S3 Read Only access policy to IAM Role")

iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

ARN = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

print("***********************************")
print('Creating cluster...')

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=CLUSTER_TYPE,
        NodeType=NODE_TYPE,
        NumberOfNodes=int(NUM_NODES),

        #Identifiers & Credentials
        DBName=DB_NAME,
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        PubliclyAccessible=True,
        
        #Roles (for s3 access)
        IamRoles=[ARN]
    )
except Exception as e:
    print(e)

while redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] != 'available':
    False
else:

    print("Cluster created!")
    print('Waiting for cluster availability...')
    
    while redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterAvailabilityStatus'] != 'Available':
        False
    else:
        print('Cluster available!')

myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]

HOST = myClusterProps['Endpoint']['Address']

print("**********************************")
print("Validation AWS Redshift connection")

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
conn.close()

print('Connected to Redshift!')

print("*************************************************************")
print("Use the following for HOST and ARN variables in DWH Config:\n")
print("*************************************************************")

print("[CLUSTER]")
print("HOST=" + HOST + "\n")
print("[IAM_ROLE]")
print("ARN=" + ARN)