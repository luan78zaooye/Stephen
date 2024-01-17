import boto3
import os
import time
from pytz import timezone
from datetime import datetime

now = datetime.now(timezone('US/Pacific'))
now_str = now.strftime("%Y%m%d")


# key for test
# os.environ['AWS_ACCESS_KEY_ID']="xxx"
# os.environ['AWS_SECRET_ACCESS_KEY']="xxx"

# login
def set_boto_session(accountId=None, roleName=None):
    creds = {}
    if accountId and roleName:
        role_arn = f"arn:aws:iam::{accountId}:role/{roleName}"
        sts_client = boto3.client('sts')
        stsResponse = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=roleName)
        creds['aws_access_key_id'] = stsResponse['Credentials']['AccessKeyId']
        creds['aws_secret_access_key'] = stsResponse['Credentials']['SecretAccessKey']
        creds['aws_session_token'] = stsResponse['Credentials']['SessionToken']
    else:
        print("invalid `accoundId` or `roleName`!")
    botosession = boto3.Session(**creds)
    return botosession, creds

# get info
def getNamespaceWorkgroup(session, namespaceName):
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    namespaceResponse = rsServerlessClient.get_namespace(namespaceName=namespaceName)
    namespaceID = namespaceResponse['namespace']['namespaceId']

# create datashare from physical cluster
def createDatashare(session, namespaceID):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    cluster_identifier = 'redshift-cluster'
    database = 'dev'
    db_user = 'awsuser'

    share_name = f"rawToServerless-{now_str}"
    sql_create_datashare = f"create datashare {share_name};"
    sql_create_datashare += f"alter datashare {share_name} add all tables in schema event;"
    sql_create_datashare += f"grant usage on datashare {share_name} to namespace '{namespaceID}';"

    physicalResponse = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=sql_create_datashare)

    # physicalResponseId = physicalResponse['Id']

    sql_query_datashare = "show datashares;"
    print("-" * 20, "Waiting for creating data share")

    while True:
        physicalResponse = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                                Database=database,
                                                                DbUser=db_user,
                                                                Sql=sql_query_datashare)
        physicalResponseId = physicalResponse['Id']
        time.sleep(10)
        response = redshiftDataClient.get_statement_result(Id=physicalResponseId)
        shareNames = [list(i[0].values())[0] for i in response['Records']]
        print("share_name", share_name)
        if len(response['Records']) != 0 and share_name in shareNames:
            break
    """From data share create DB for Serverless"""
    index = shareNames.index(share_name)
    print("-" * 20, f"data share `{share_name}` created")
    producer_account = list(response['Records'][index][-2].values())[0]
    producer_namespace = list(response['Records'][index][-1].values())[0]
    return producer_account, producer_namespace, share_name


# create DB for serverless, from physical cluster data share
def createDBforServerless(session, producer_account, producer_namespace, workgroupName, share_name):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    sql_createForServerless = f"create database share_db from datashare {share_name} of account '{producer_account}' namespace '{producer_namespace}';"

    sql_createForServerless += "grant usage on database share_db to admin;"
    sql_createForServerless += "grant usage on schema public to admin;"

    serverlessResponse = redshiftDataClient.execute_statement(Database="dev", WorkgroupName=workgroupName,
                                                              Sql=sql_createForServerless)

    # sql_test = "select * from share_db3.public.tableshare;"
    # queryFromServerless = redshiftDataClient.execute_statement(Database ="dev",WorkgroupName=workgroupName,Sql=sql_test)
    # time.sleep(3)
    # serverlessResponseId = queryFromServerless['Id']
    # response = redshiftDataClient.get_statement_result(Id=serverlessResponseId)


# query from serverless to test if the DB is created from data share successfully
def testQuery(session, workgroupName):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    sql_test = "select * from share_db.public.tableshare limit 2;"
    queryFromServerless = redshiftDataClient.execute_statement(Database="dev", WorkgroupName=workgroupName,
                                                               Sql=sql_test)
    time.sleep(3)
    serverlessResponseId = queryFromServerless['Id']
    response = redshiftDataClient.get_statement_result(Id=serverlessResponseId)
    print(response)


if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "createNamespaceWorkgroup", "#" * 20)
    workgroupName = "prod-rssls-01"
    namespaceID = getNamespaceID(session, "ecs-20240116")
    print("#" * 20, "createDatashare", "#" * 20)
    producer_account, producer_namespace, share_name = createDatashare(session, namespaceID)
    # time.sleep(60)
    print("#" * 20, "createDBforServerless", "#" * 20)
    createDBforServerless(session, producer_account, producer_namespace, workgroupName, share_name)
    print("#" * 20, "testQuery", "#" * 20)
    testQuery(session, workgroupName)