import boto3
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

print("**********************************************")
print("Deleting Cluster...")

try:
    redshift.delete_cluster(
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        SkipFinalClusterSnapshot=True
        )
    
    print(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0])

except Exception as e:
    print(e)

print("**********************************************")
print("Detatching IAM Role policies...")

try:
    iam_client.detach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )

except Exception as e:
    print(e)

print("**********************************************")
print("Deleting IAM Role")

try:
    iam_client.delete_role(RoleName=IAM_ROLE_NAME)

except Exception as e:
    print(e)