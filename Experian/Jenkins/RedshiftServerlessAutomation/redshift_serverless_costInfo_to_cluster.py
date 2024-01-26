import boto3

from create_redshift_serverless import set_boto_session
from create_redshift_serverless import workgroupName

# cluster_identifier = 'prod-rsraw-01'

# serverless unload cost data weekly to S3
def serverlessUnloadToS3(session,  serverlessWorkgroupName):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")

    unload_cost_query = "UNLOAD($$SELECT TRUNC(CONVERT_TIMEZONE('US/Pacific', start_time)) as day, \
                                  (SUM(charged_seconds)/3600::double precision)*0.36 AS cost_incurred \
                               FROM sys_serverless_usage \
                               GROUP BY 1 \
                               ORDER BY 1 DESC$$) \
                        TO 's3://redshift-serverless-cost-info/cost/' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') \
                        CREDENTIALS 'aws_iam_role=arn:aws:iam::251338191197:role/redshift_role' \
                        ALLOWOVERWRITE \
                        PARALLEL OFF \
                        DELIMITER ',' \
                        EXTENSION 'csv';"



    serverlessResponse = redshiftDataClient.execute_statement(Database="dev", WorkgroupName=serverlessWorkgroupName,
                                                              Sql=unload_cost_query)
"""
# load data to physical cluster
def S3LoadToCluster(session, cluster_identifier):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    database = 'dev'
    db_user = 'awsuser'

    # sql scripts for producer
    load_query = " ;"

    physicalResponse = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=load_query)


# query from xx cluster to test if new info is loaded successfully

def testQuery(session, cluster_identifier):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    sql_test = "select * from xxx limit 7;"
    queryFromCluster = redshiftDataClient.execute_statement(Database="dev", WorkgroupName=cluster_identifier,
                                                               Sql=sql_test)
    time.sleep(10)
    clusterResponseId = queryFromCluster['Id']
    response = redshiftDataClient.get_statement_result(Id=clusterResponseId)
    print(response)
"""

if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "unload data to S3", "#" * 20)
    serverlessuUnloadToS3(session, workgroupName)
    """
    print("#" * 20, "load to xx cluster", "#" * 20)
    S3LoadToCluster(session, cluster_identifier)
    print("#" * 20, "testQuery", "#" * 20)
    testQuery(session, cluster_identifier)
    """
