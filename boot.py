import configparser
import sys
#from venv import create
import boto3
import pandas as pd
from io import BytesIO
import json
import psycopg2
import datetime
import logging

import etl
import create_tables


def to_df(properties):
    """Formats properties into a DataFrame.

    Args:
    properties: the results from a redshift.describe_cluster AWS API call

    """
    pd.set_option('display.max_colwidth', 20)
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName', 'Endpoint', 
                  'NumberOfNodes', 'VpcId']
    #
    x = [(k, v) for k,v in properties.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Setup Logging
logging.basicConfig(#filename='data_warehouse_log.log', 
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler('data_warehouse.log'),
                              logging.StreamHandler(sys.stdout)]
                    )
start_time = datetime.datetime.now()
logging.info("Starting Up")

# Grab our configurations
config_file = 'dwh.cfg'
config = configparser.ConfigParser()
config.read(config_file)

# Initialize AWS Resources
ec2 = boto3.resource('ec2',
                     region_name=config['AWS']['REGION'],
                     aws_access_key_id=config['AWS']['KEY'],
                     aws_secret_access_key=config['AWS']['secret'])
s3 = boto3.resource('s3',
                    region_name=config['AWS']['REGION'],
                    aws_access_key_id=config['AWS']['KEY'],
                    aws_secret_access_key=config['AWS']['SECRET'])
iam = boto3.client('iam',
                    region_name=config['AWS']['REGION'],
                    aws_access_key_id=config['AWS']['KEY'],
                    aws_secret_access_key=config['AWS']['SECRET'])
redshift = boto3.client('redshift',
                    region_name=config['AWS']['REGION'],
                    aws_access_key_id=config['AWS']['KEY'],
                    aws_secret_access_key=config['AWS']['SECRET'])

# Create IAM Role
try:
    logging.info("Creating new IAM Role")
    dwhRole = iam.create_role(      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_role
        Path='/',    # this is default anyways. Used for organizing roles: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html
        RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'],
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {
                                            'Service': 'redshift.amazonaws.com'
                                          }
                            }],
             'Version': '2012-10-17'
             }
        )
    )
except Exception as e:
    logging.error(e)

# Attach Policy
# We want to give this role S3 Read-only Access
logging.info('Attaching Policy')
# Enter the Policy ARN from AWS 'Policies' where AmazonS3ReadOnlyAccess is selected.
iam.attach_role_policy(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'],
                       PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'   
                      )['ResponseMetadata']

# Get IAM Role ARN
logging.info("Getting IAM role Amazon Resource Number (ARN)")
roleArn = iam.get_role(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME']) 
logging.info(f"ARN: {roleArn['Role']['Arn']}")

# Update our dwh.cfg to include our Role ARN
# This might need to be more explicitly done, but for now we can just update the variable
config['IAM_ROLE']['ARN'] = roleArn['Role']['Arn']
logging.info("Creating Cluster")
try:
    response = redshift.create_cluster(
        # Hardware
        ClusterType=config['CLUSTER']['DWH_CLUSTER_TYPE'],
        NodeType=config['CLUSTER']['DWH_NODE_TYPE'],
        NumberOfNodes=int(config['CLUSTER']['DWH_NUM_NODES']),

        # Identifiers & Credentials
        DBName=config['CLUSTER']['DB_NAME'],
        ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],
        MasterUsername=config['CLUSTER']['DB_USER'],
        MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],

        # Roles
        IamRoles=[config['IAM_ROLE']['ARN']]
    )
except Exception as e:
    logging.error(e)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
to_df(myClusterProps)

# Wait for cluster to be available
logging.info("Waiting for Cluster to come online")
from time import sleep
n_count = 0
try:
    while True:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
        _df = to_df(myClusterProps)
        done = _df[_df['Key']=='ClusterStatus']['Value'].values[0]
        if done == 'creating':
            n_count += 1
            sys.stdout.write(f'\rProgress: {"="*n_count}')
            sys.stdout.flush()
        else:
            break
        sleep(5)
except Exception as e:
    logging.error(e)

config['CLUSTER']['HOST']=myClusterProps['Endpoint']['Address']
# myClusterProps['IamRoles'][0]['IamRoleArn']
logging.info(f"Cluster is created. Endpoint: {myClusterProps['Endpoint']['Address']}")

logging.info("Authorizing security group")
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(config['CLUSTER']['DB_PORT']),
        ToPort=int(config['CLUSTER']['DB_PORT'])
    )
except Exception as e:
    logging.error(e)
    pass

# Run query on empty Redshift database to make sure connection is operational
logging.info("Testing connection to database with attributes:")
logging.info([config['CLUSTER']['DB_NAME'],
                        config['CLUSTER']['DB_USER'],
                        config['CLUSTER']['DB_PASSWORD'],
                        config['CLUSTER']['HOST'],
                        config['CLUSTER']['DB_PORT']])
conn = psycopg2.connect(dbname=config['CLUSTER']['DB_NAME'],
                        user=config['CLUSTER']['DB_USER'],
                        password=config['CLUSTER']['DB_PASSWORD'],
                        host=config['CLUSTER']['HOST'],
                        port=config['CLUSTER']['DB_PORT'],
                        connect_timeout=10
)
cur = conn.cursor()
cur.execute('SELECT * FROM pg_stat_activity LIMIT 1')
conn.commit()
test_data = cur.fetchall()
logging.info(f"SELECT FROM pg_stat_activity: {test_data}")

logging.info('Creating Tables (create_tables.py)')
create_tables.main()

logging.info('Run ETL (etl.py)')
etl.main()

end_time = datetime.datetime.now()
logging.info(f"Finished. Total Elapsed Time: {end_time-start_time}")