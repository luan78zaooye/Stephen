import boto3

# get temporary secury credential
"""
role_arn = 'arn:aws:iam::251338191197:role/Jenkins-Role'
role_name = 'Jenkins-Role'
sts = boto3.client('sts')
stsresponse = sts.assume_role(RoleArn=role_arn, RoleSessionName=role_name)


creds = {}
creds['AWS_ACCESS_KEY_ID'] = 'AKIATVBHMOFO72ZLH6FA'
creds['AWS_SECRET_ACCESS_KEY'] = 'dsadaefad43da'
creds['AWS_SECURITY_TOKEN'] = 'dadwaacsc34faef'
"""

botosession = boto3.session.Session(aws_access_key_id='AKIATVBHMOFO72ZLH6FA')

client_secret_manager = botosession.client('secretsmanager', region_name = 'us-west-2')

# create secret
client_secret_manager.create_secret(
    Name='rdsls',
    ClientRequestToken='a97e745a-0e2e-4d7a-a420-f8ccf580ecf4',
    Description='create secret',
    SecretString='qCHotS9MdXdJAor',
    ForceOverwriteReplicaSecret=False
)

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
        'useractivitylog'
    ],
    manageAdminPassword=False,
    namespaceName='namespace',
    tags=[
        {
            'key': 'Type',
            'value': 'db'
        }
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
