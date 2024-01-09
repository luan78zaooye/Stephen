import boto3
import os

# get temporary secury credential
role_arn = 'arn:aws:iam::251338191197:role/Jenkins-Role'
role_name = 'Jenkins-Role'
sts = boto3.client('sts')
stsresponse = sts.assume_role(RoleArn=role_ran, RoleSessionName=role_name)

# setup session token
os.environ['AWS_ACCESS_KEY_ID'] = stsresponse['Credentials']['AccessKeyId']
os.environ['AWS_SECRET_ACCESS_KEY'] = stsresponse['Credentials']['SecretAccessKey']
os.environ['AWS_SECURITY_TOKEN'] = stsresponse['Credentials']['SessionToken']

creds = {}
creds['AWS_ACCESS_KEY_ID'] = srsresponse['Credentials']['AccessKeyId']
creds['AWS_SECRET_ACCESS_KEY'] = srsresponse['Credentials']['SecretAccessKey']
creds['AWS_SECURITY_TOKEN'] = srsresponse['Credentials']['SessionToken']

botosession = boto3.session.Session(**creds)
client_redshift_serverless = botosession.client('redshift-serverless', region_name = 'us-west-2')

# create namespace
client_redshift_serverless.create_namespace(
    adminUserPassword='admin',
    adminUsername='Lsh900809',
    dbName='test_sihan',
    defaultIamRoleArn='arn:aws:iam::251338191197:role/redshift_role',
    iamRoles=[
        'redshift_role'
    ],
    kmsKeyId='5dd84b8a-4989-4f2f-99fb-073e9499b6a9',
    logExports=[
        'useractivitylog'|'userlog'|'connectionlog',
    ],
    manageAdminPassword=False,
    namespaceName='namespace',
    tags=[
        {
            'key': 'Type',
            'value': 'db'
        },
    ]
)

# create workgroup
client_redshift_serverless.create_workgroup(
    baseCapacity=8,
    enhancedVpcRouting=False,
    maxCapacity=8,
    namespaceName='namespace',
    port=5439,
    publiclyAccessible=True,
    securityGroupIds=[
        'sg-011e043c0037cb8e7'
    ],
    subnetIds=[
        'subnet-0ccf709939a778510',
	'subnet-079ee92704b5f0a17',
	'subnet-016aea78f570cdb45',
	'subnet-0e7705781de7962e4'
    ],
    tags=[
        {
            'key': 'Type',
            'value': 'db'
        },
    ],
    workgroupName='workgroup'
)

# create endpoint serverless
client_redshift_serverless.create_endpoint_access(
    endpointName='sihan_endpoint',
    subnetIds=[
        'subnet-0ccf709939a778510',
	'subnet-079ee92704b5f0a17',
	'subnet-016aea78f570cdb45',
	'subnet-0e7705781de7962e4'
    ],
    vpcSecurityGroupIds=[
        'sg-011e043c0037cb8e7'
    ],
    workgroupName='workgroup'
)
