import boto3
import os
import time
import random
from datetime import datetime
from pytz import timezone

now = datetime.now(timezone('US/Pacific'))
now_date = now.strftime("%Y%m%d")
now_time = now.strftime("%Y%m%d_%H%M%S")

# key for test
# os.environ['AWS_ACCESS_KEY_ID']="xxx"
# os.environ['AWS_SECRET_ACCESS_KEY']="xxx"

# login
def set_boto_session(accountId=None, roleName=None):
    creds = {}
    if accountId and roleName:
        role_arn = "arn:aws:iam::" + accountId + ":role/" + roleName
        sts_client = boto3.client('sts')
        stsResponse = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=roleName)
        creds['aws_access_key_id'] = stsResponse['Credentials']['AccessKeyId']
        creds['aws_secret_access_key'] = stsResponse['Credentials']['SecretAccessKey']
        creds['aws_session_token'] = stsResponse['Credentials']['SessionToken']
    else:
        print("invalid `accountId` or `roleName`!")
    botosession = boto3.session.Session(**creds)
    return botosession

# new password for admin user
def getAdminPassword():
    passChars = "abcdefghijklmnopqrstuvwxyz1234567890"
    pw_length = 16
    admin_pw = ""
    
    for i in range(pw_length):
        next_index = random.randrange(len(passChars))
        admin_pw += passChars[next_index]

    # replace 1 or 2 characters with a number for the front half:
    for i in range(random.randrange(1, 3)):
        replace_index = random.randrange(len(admin_pw) // 2)
        admin_pw = admin_pw[:replace_index] + \
               str(random.randrange(10)) + admin_pw[replace_index + 1:]

    # replace 1 or 2 characters with a uppercase letter for the back half:
    for i in range(random.randrange(1, 3)):
        replace_index = random.randrange(len(admin_pw) // 2, len(admin_pw))
        admin_pw = admin_pw[:replace_index] + \
               admin_pw[replace_index].upper() + admin_pw[replace_index + 1:]
        
    return admin_pw
        
# save to Secret Manager
def loadToSecretManager(adminUsername, adminUserPassword):
    secretsmanagerClient = boto3.client('secretsmanager', region_name="us-west-2")
    SecretString = '{"adminUser":"%s","Password":"%s"}' % (
       adminUsername, adminUserPassword)
    response = secretsmanagerClient.create_secret(Name=f'redshiftSecret-{now_time}',
                                                  SecretString=SecretString)

# create nemespace and workgroup for serverless
def createNamespaceWorkgroup(session, accountId, roleName):
    adminUserPW = getAdminPassword()
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    namespaceName = f'ecs-{now_date}'
    workgroupName = "prod-rssls-01"

    # create namespace
    namespaceResponse = rsServerlessClient.create_namespace(
        namespaceName=namespaceName,
        adminUsername='admin',
        adminUserPassword=adminUserPW,
        defaultIamRoleArn=f"arn:aws:iam::{accountId}:role/{roleName}",
        iamRoles=[
            f"arn:aws:iam::{accountId}:role/{roleName}"
        ],
        kmsKeyId='c3bc717c-287b-46b4-8721-e26b3c4f2431',
        logExports=['useractivitylog','userlog','connectionlog'],
        tags=[
            {
                'key': 'Type',
                'value': 'db'
            },
            {
                'key': 'Platform',
                'value': 'dataservices'
            },
            {
                'key': 'Environment',
                'value': 'prod'
            },
            {
                'key': 'Service',
                'value': 'redshiftserverless'
            },
            {
                'key': 'productLine',
                'value': 'd2c'
            },
            {
                'key': 'Name',
                'value': 'prod-us-db-redshiftserverless'
            },
            {
                'key': 'ServiceOwner',
                'value': 'dbteam'
            }
        ]
        
    )

    while True:
        response = rsServerlessClient.get_namespace(namespaceName=namespaceName)
        time.sleep(10)
        if response['namespace']['status'] == "AVAILABLE":   
            break
            
    # save admin user name and password to secret manager       
    adminUsername = response['namespace']['adminUsername']
    print("#" * 20, "loadToSecretManager", "#" * 20)
    loadToSecretManager(adminUsername, adminUserPW) 

    # create workgroup
    workgroupResponse = rsServerlessClient.create_workgroup(
        baseCapacity=8,
        maxCapacity=8,
        enhancedVpcRouting=True,
        publiclyAccessible=False,
        # securityGroupIds=["sg-e8e38296"],
        # subnetIds = ["subnet-82f298da","subnet-7df8c10b","subnet-6720c700"],
        securityGroupIds=["sg-011e043c0037cb8e7"],
        subnetIds=["subnet-0ccf709939a778510", "subnet-016aea78f570cdb45", "subnet-0e7705781de7962e4",
                   "subnet-079ee92704b5f0a17"],
        namespaceName=namespaceName,
        workgroupName=workgroupName,
        tags=[
            {
                'key': 'Type',
                'value': 'db'
            },
            {
                'key': 'Platform',
                'value': 'dataservices'
            },
            {
                'key': 'Environment',
                'value': 'prod'
            },
            {
                'key': 'Service',
                'value': 'redshiftserverless'
            },
            {
                'key': 'productLine',
                'value': 'd2c'
            },
            {
                'key': 'Name',
                'value': 'prod-us-db-redshiftserverless'
            },
            {
                'key': 'ServiceOwner',
                'value': 'dbteam'
            }
        ]
    )

    while True:
        response = rsServerlessClient.get_workgroup(workgroupName=workgroupName)
        time.sleep(10)
        if response['workgroup']['status'] == "AVAILABLE":
            break
    return namespaceID, namespaceName, workgroupName
    

if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "createNamespaceWorkgroup", "#" * 20)
    createNamespaceWorkgroup(session, "251338191197", "redshift_serverless_automation")
    
