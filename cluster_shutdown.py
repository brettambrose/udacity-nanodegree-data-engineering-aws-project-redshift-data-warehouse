import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get("AWS","KEY")
SECRET = config.get("AWS","SECRET")
IAM_ROLE_NAME = config.get("IAM_ROLE","IAM_ROLE_NAME")
CLUSTER_IDENTIFIER = config.get("INFRASTRUCTURE","CLUSTER_IDENTIFIER")

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
print("*******************")
print("Deleting Cluster...")

redshift.delete_cluster( ClusterIdentifier=CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

print(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0])

print("*******************************")
print("Detatching IAM Role policies...")

iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

print("*****************")
print("Deleting IAM Role")

iam.delete_role(RoleName=IAM_ROLE_NAME)