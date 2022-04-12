# Shutdown cluster and clean up
import pandas as pd
import configparser
import boto3
import logging
import sys
from time import sleep


def to_df(properties):
    """Formats properties into a DataFrame.

    Args:
        properties: the results from a redshift.describe_cluster AWS API call

    """
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName', 'Endpoint', 
                  'NumberOfNodes', 'VpcId']
    x = [(k, v) for k,v in properties.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

# Setup Logging
logging.basicConfig(#filename='data_warehouse.log', 
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler('data_warehouse.log'),
                              logging.StreamHandler(sys.stdout)]
                    )

# Grab our configurations
config_file='dwh.cfg'
config=configparser.ConfigParser()
config.read(config_file)

# Initialize AWS Resources
# ec2 = boto3.resource('ec2',
#                      region_name=config['AWS']['REGION'],
#                      aws_access_key_id=config['AWS']['KEY'],
#                      aws_secret_access_key=config['AWS']['secret'])
# s3 = boto3.resource('s3',
#                     region_name=config['AWS']['REGION'],
#                     aws_access_key_id=config['AWS']['KEY'],
#                     aws_secret_access_key=config['AWS']['SECRET'])
iam = boto3.client('iam',
                    region_name=config['AWS']['REGION'],
                    aws_access_key_id=config['AWS']['KEY'],
                    aws_secret_access_key=config['AWS']['SECRET'])
redshift = boto3.client('redshift',
                    region_name=config['AWS']['REGION'],
                    aws_access_key_id=config['AWS']['KEY'],
                    aws_secret_access_key=config['AWS']['SECRET'])

# Shutdown Cluster
logging.info('Starting Shutdown Process')
try:
    redshift.delete_cluster(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'], 
                            SkipFinalClusterSnapshot=True)
    deleted_cluster_props = redshift.describe_clusters(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
    to_df(deleted_cluster_props)
except Exception as e:
    logging.error(e)

# Wait for Cluster to be deleted
logging.info("Waiting for Cluster to be Deleted")
n_count = 0
try:
    while True:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
        _df = to_df(cluster_props)
        done = _df[_df['Key']=='ClusterStatus']['Value'].values[0]
        if done == 'deleting':
            n_count += 1
            sys.stdout.write(f'\rProgress: {"="*n_count}')
            sys.stdout.flush()
        sleep(3)
except Exception as e:
    logging.error(e)
logging.info("Cluster has finished deletion")

# Detach Policy
logging.info("Detach Policy")
try:
    iam.detach_role_policy(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'],
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
except Exception as e:
    logging.error(e)
logging.info("IAM role policy detached")

# Delete Role
logging.info("Delete Role")
try:
    iam.delete_role(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'])
except Exception as e:
    logging.error(e)
logging.info("Shutdown Completed")
