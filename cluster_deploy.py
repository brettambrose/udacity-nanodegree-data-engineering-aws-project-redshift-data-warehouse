import boto3
import json
import psycopg2
import configparser
import os

dwh_config = configparser.ConfigParser()
dwh_config.read("dwh.cfg")

aws_creds_path = os.path.expanduser("~\\.aws\\credentials")
aws_creds = configparser.ConfigParser()
aws_creds.read(aws_creds_path)

aws_config_path = os.path.expanduser("~\\.aws\\config")
aws_config = configparser.ConfigParser()
aws_config.read(aws_config_path)

# CLUSTER
CLUSTER_IDENTIFIER    = dwh_config.get("CLUSTER","CLUSTER_IDENTIFIER")
CLUSTER_TYPE          = dwh_config.get("CLUSTER","CLUSTER_TYPE")
NODE_TYPE             = dwh_config.get("CLUSTER","NODE_TYPE")
NUM_NODES             = dwh_config.get("CLUSTER","NUM_NODES")

# DATABASE
DB_HOST               = dwh_config.get("DB","DB_HOST")
DB_NAME               = dwh_config.get("DB","DB_NAME")
DB_USER               = dwh_config.get("DB","DB_USER")
DB_PASSWORD           = dwh_config.get("DB","DB_PASSWORD")
DB_PORT               = dwh_config.get("DB","DB_PORT")

# IAM
IAM_ROLE_NAME         = dwh_config.get("IAM_ROLE","IAM_ROLE_NAME")

# AWS CREDENTIALS & CONFIG
KEY                   = aws_creds.get("default", "aws_access_key_id")
SECRET                = aws_creds.get("default", "aws_secret_access_key")
REGION                = aws_config.get("default", "region")

print("**********************************************")
print("Establishing boto3 resources and clients...")

iam_client = boto3.client('iam',
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=REGION
                         )

redshift = boto3.client('redshift',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name=REGION
                       )

ec2 = boto3.resource('ec2',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name=REGION
                     )

ec2_client = boto3.client("ec2",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=REGION
                          )

print("**********************************************")
print("Creating IAM Role")

try:
    dwhRole = iam_client.create_role(
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

print("**********************************************")
print("Updating local .aws/config file with Role ARN")

try:
    aws_config["profile Redshift"]
    print("Role ARN already exists in .aws/config")

    IAM_ROLE_ARN = aws_config.get("profile Redshift","role_arn")
    
except:
    try:
        role_arn = iam_client.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

        aws_config_override = configparser.ConfigParser()
        aws_config_override.read(aws_config_path)

        aws_config_override["profile Redshift"] = {"role_arn": role_arn}
        
        with open(aws_config_path, "w") as configfile:
            aws_config_override.write(configfile)
            
        print("Role ARN added to .aws/config ")

        aws_config.read(aws_config_path)

        IAM_ROLE_ARN = aws_config.get("profile Redshift","role_arn")

    except Exception as e:
        print(e)

print("**********************************************")
print("Attaching policies to IAM Role")

try:
    iam_client.attach_role_policy(RoleName=IAM_ROLE_NAME,
                                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                 )

except Exception as e:
    print(e)

print("**********************************************")
print("Creating cluster...")

try:
    response = redshift.create_cluster(
        ClusterType=CLUSTER_TYPE,
        NodeType=NODE_TYPE,
        NumberOfNodes=int(NUM_NODES),
        DBName=DB_NAME,
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        PubliclyAccessible=True,
        IamRoles=[IAM_ROLE_ARN]
    )
    
except Exception as e:
    print(e)

print("**********************************************")
print("Waiting for cluster availability...")

while redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] != 'available':
    False

else:
    while redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterAvailabilityStatus'] != 'Available':
        False
    
    else:
        clusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        clusterHost = clusterProps['Endpoint']['Address']

        print(f"{clusterHost} now available")

print("**********************************************")
print("Adding Cluster endpoint to dwh.cfg file...")

if len(DB_HOST) == 0:
    try:
        dwh_config_override = configparser.ConfigParser()
        dwh_config_override.read("dwh.cfg")

        dwh_config_override["DB"]["DB_HOST"] = clusterHost

        with open("dwh.cfg", "w") as configfile:
            dwh_config_override.write(configfile)
    
        print("Cluster endpoint added to dwh.cfg")

        dwh_config.read("dwh.cfg")

        DB_HOST = dwh_config.get("DB","DB_HOST")

    except Exception as e:
        print(e)
else:
    print("Cluster endpoint already exists in dwh.cfg")
    
print("**********************************************")
print("Specifying ingress rules to default sec group")

try:
    group_id = ec2_client.describe_security_groups()["SecurityGroups"][0]["GroupId"]
    defaultSg = ec2.SecurityGroup(group_id)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DB_PORT),
        ToPort=int(DB_PORT)  
    )

except Exception as e:
    print(e)
    
print("**********************************************")
print("Validating cluster availability...")

try:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    conn.close()

    print("Successfully connected to cluster")

except Exception as e:
    print(e)