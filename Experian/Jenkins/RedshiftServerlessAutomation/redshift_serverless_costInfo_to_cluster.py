import boto3
import time
from datetime import datetime, timedelta
from create_redshift_serverless import set_boto_session
from create_redshift_serverless import workgroupName

<<<<<<< Updated upstream
=======


>>>>>>> Stashed changes
now = datetime.now()
now_date = now - timedelta(days=1)
before_date = now - timedelta(days=7)
now_str = now_date.strftime("%Y%m%d")
now_str_hyphen = now_date.strftime("%Y-%m-%d")
before_str = before_date.strftime("%Y%m%d")

cluster_identifier = 'prod-rsraw-01'

# serverless unload cost data weekly to S3
def serverlessUnloadToS3(session,  serverlessWorkgroupName):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")

    unload_cost_query = f"UNLOAD($$SELECT TRUNC(start_time) as day, \
                                  (SUM(charged_seconds)/3600::double precision)*0.36 AS cost_incurred \
                               FROM sys_serverless_usage \
                               WHERE TRUNC(start_time) != TRUNC(getdate())\
                               GROUP BY 1 \
                               ORDER BY 1 DESC$$) \
                        TO 's3://redshift-serverless-cost-info/cost/{before_str}_to_{now_str}_' \
                        CREDENTIALS 'aws_iam_role=arn:aws:iam::251338191197:role/redshift_role' \
                        ALLOWOVERWRITE \
                        PARALLEL OFF \
                        DELIMITER ',' \
                        EXTENSION 'csv';"

    unload_user_cost_query = f"UNLOAD($$SELECT TRUNC(start_time) AS day, \
                                       TRIM(user_id) AS user_id, \
                                       SUM(elapsed_time) / 60000000::DOUBLE PRECISION AS query_time \
                               FROM sys_query_history \
                               WHERE TRUNC(start_time) != TRUNC(getdate()) \
                               GROUP BY 1,2 \
                               ORDER BY 1 DESC$$) \
                             TO 's3://redshift-serverless-cost-info/userCost/{before_str}_to_{now_str}_' \
                             CREDENTIALS 'aws_iam_role=arn:aws:iam::251338191197:role/redshift_role' \
                             ALLOWOVERWRITE \
                             PARALLEL OFF \
                             DELIMITER ',' \
                             EXTENSION 'csv';"
    
    print('-' * 20, 'start unloading' , '-' * 20)
    redshiftDataClient.execute_statement(Database="dev", WorkgroupName=serverlessWorkgroupName,
                                                              Sql=unload_cost_query)
    redshiftDataClient.execute_statement(Database="dev", WorkgroupName=serverlessWorkgroupName,
                                                              Sql=unload_user_cost_query)
    
    start_time = datetime.now()
    s3Client = session.client('s3', region_name="us-west-2")
    response1 = ''
    response2 = ''
    while True:
        time.sleep(10)
        try:
            response1 = s3Client.get_object(Bucket='redshift-serverless-cost-info',
                                       Key=f'cost/{before_str}_to_{now_str}_000.csv')
            response2 = s3Client.get_object(Bucket='redshift-serverless-cost-info',
                                       Key=f'userCost/{before_str}_to_{now_str}_000.csv')
        except Exception as e:
            print(f"UNLOAD not completed yet: {e}")
        if response1 != '' and response2 != '':
            print('-' * 20, 'UNLOAD completed' , '-' * 20)
            break
        if datetime.now() - start_time > timedelta(seconds=100):
            print("UNLOAD failed")
            break


# load data to physical cluster
def S3LoadToCluster(session, cluster_identifier):
    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    
    database = 'dev'
    db_user = 'awsuser'

    # sql scripts for producer
    load_cost_query = "TRUNCATE dbaworking.cost;"
    load_cost_query += "COPY dbaworking.cost \
                        FROM 's3://redshift-serverless-cost-info/cost/' \
                        CREDENTIALS 'aws_iam_role=arn:aws:iam::251338191197:role/redshift_role' \
                        TIMEFORMAT 'auto' EMPTYASNULL MAXERROR 0 COMPUPDATE OFF STATUPDATE OFF \
                        DELIMITER ',' TRUNCATECOLUMNS TRIMBLANKS CSV;"

    load_user_cost_query = "TRUNCATE dbaworking.user_cost;"
    load_user_cost_query += "COPY dbaworking.user_cost \
                             FROM 's3://redshift-serverless-cost-info/userCost/' \
                             CREDENTIALS 'aws_iam_role=arn:aws:iam::251338191197:role/redshift_role' \
                             TIMEFORMAT 'auto' EMPTYASNULL MAXERROR 0 COMPUPDATE OFF STATUPDATE OFF \
                             DELIMITER ',' TRUNCATECOLUMNS TRIMBLANKS CSV;"
    

    redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=load_cost_query)
    
    redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=load_user_cost_query)
    
    time.sleep(10)
    # query from USR cluster to test if new info is loaded successfully

    redshiftDataClient = session.client("redshift-data", region_name="us-west-2")
    sql_test1 = "select * from dbaworking.cost order by day desc limit 1;"
    sql_test2 = "select * from dbaworking.user_cost order by day desc limit 1;"
    queryFromCluster1 = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=sql_test1)

    queryFromCluster2 = redshiftDataClient.execute_statement(ClusterIdentifier=cluster_identifier,
                                                            Database=database,
                                                            DbUser=db_user,
                                                            Sql=sql_test2)
    time.sleep(10)
    clusterResponseId1 = queryFromCluster1['Id']
    response1 = redshiftDataClient.get_statement_result(Id=clusterResponseId1)
    clusterResponseId2 = queryFromCluster2['Id']
    response2 = redshiftDataClient.get_statement_result(Id=clusterResponseId2)

    if response1['Records'][0][0]['stringValue'] == now_str_hyphen and \
              response2['Records'][0][0]['stringValue'] == now_str_hyphen:
        print(response1['Records'])
        print(response2['Records'])          
        print('-' * 20 + "LOAD completed" + '-' * 20)
    else:
        print("LAOD failed")
           
        
    
    

if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "unload data to S3", "#" * 20)
    serverlessUnloadToS3(session, workgroupName)
    print("#" * 20, "load to USR cluster", "#" * 20)
    S3LoadToCluster(session, cluster_identifier)


