import boto3
import os
import time
from pytz import timezone
from datetime import datetime, timedelta
from create_redshift_serverless import set_boto_session
from create_redshift_serverless import namespaceName, workgroupName

now = datetime.now(timezone('US/Pacific'))
now_str = now.strftime("%Y%m%d")


def getNamespaceId(session, namespaceName):
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    namespaceResponse = rsServerlessClient.get_namespace(namespaceName=namespaceName)
    namespaceId = namespaceResponse['namespace']['namespaceId']
    return namespaceId

# create datashare from physical cluster
def createDatashare(session, namespaceId):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    cluster_identifier = 'prod-rsraw-01'
    database = 'dev'
    db_user = 'awsuser'
    consumer_namespace = namespaceId
    # sql scripts for producer
    share_name = f"raw_serverless_{now_str}"
    sql_create_datashare = f"CREATE DATASHARE {share_name};"
    sql_create_datashare += f"GRANT USAGE ON DATASHARE {share_name} TO NAMESPACE '{consumer_namespace}';"
    sql_create_datashare += f"ALTER DATASHARE {share_name} ADD SCHEMA event;"
    sql_create_datashare += f"ALTER DATASHARE {share_name} ADD ALL TABLES IN SCHEMA event;"
    sql_create_datashare += f"ALTER DATASHARE {share_name} SET INCLUDENEW = TRUE FOR SCHEMA event;"
    # add more schema to datashare
    ##########
    ##########
    physicalResponse = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=sql_create_datashare)

    # physicalResponseId = physicalResponse['Id']

    sql_query_datashare = "SHOW DATASHARES;"
    print("-" * 20, "Waiting for creating data share")

    start_time = datetime.now()
    while True:
        physicalResponse = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                                Database=database,
                                                                DbUser=db_user,
                                                                Sql=sql_query_datashare)
        physicalResponseId = physicalResponse['Id']
        time.sleep(10)
        response = redshiftDataClient.get_statement_result(Id=physicalResponseId)
        shareNames = [list(i[0].values())[0] for i in response['Records']]
        print(response['Records'][0])
        print("share_name", share_name)
        if len(response['Records']) != 0 and share_name in shareNames:
            break
        if datetime.now() - start_time > timedelta(seconds=100):
            break
    """From data share create DB for Serverless"""
    index = shareNames.index(share_name)
    print("-" * 20, f"data share `{share_name}` created")
    producer_namespace = list(response['Records'][index][-1].values())[0]
    return producer_account, producer_namespace, share_name


# create DB for serverless, from physical cluster data share
def createDBforServerless(session, producer_namespace, workgroupName, share_name):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    # sql scripts for consumer
    sql_createForServerless = f"CREATE DATABASE ecswarehouse FROM DATASHARE {share_name} OF NAMESPACE '{producer_namespace}';"
    sql_createForServerless += "CREATE EXTERNAL SCHEMA event FROM REDSHIFT DATABASE ecswarehouse SCHEMA event;"
  # sql_createForServerless += "GRANT USAGE ON SCHEMA event to group/user;"

    serverlessResponse = redshiftDataClient.execute_statement(Database="dev", WorkgroupName=workgroupName,
                                                              Sql=sql_createForServerless)


# query from serverless to test if the DB is created from data share successfully
def testQuery(session, workgroupName):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    sql_test = "select * from event.sales100 limit 2;"
    queryFromServerless = redshiftDataClient.execute_statement(Database="dev", WorkgroupName=workgroupName,
                                                               Sql=sql_test)
    time.sleep(3)
    serverlessResponseId = queryFromServerless['Id']
    response = redshiftDataClient.get_statement_result(Id=serverlessResponseId)
    print(response)


if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "getNamespaceId", "#" * 20)
    namespaceId = getNamespaceId(session, namespaceName)
    print("#" * 20, "createDatashare", "#" * 20)
    producer_account, producer_namespace, share_name = createDatashare(session, namespaceId)
    print("#" * 20, "createDBforServerless", "#" * 20)
    createDBforServerless(session, producer_namespace, workgroupName, share_name)
    print("#" * 20, "testQuery", "#" * 20)
    testQuery(session, workgroupName)
