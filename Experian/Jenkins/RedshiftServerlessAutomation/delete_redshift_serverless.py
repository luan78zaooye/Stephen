import boto3
import time
import os
from create_redshift_serverless import set_boto_session

def deleteWorkgroupNamespace(session):
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    WorkgroupsResponse = rsServerlessClient.list_workgroups()
    workgroupName = WorkgroupsResponse['workgroups'][0]['workgroupName']
    NamespacesResponse = rsServerlessClient.list_namespaces()
    namespaceName = NamespacesResponse['namespaces'][0]['namespaceName']

    # delete workgroup
    print(f"ready to delete redshift serverless workgroup {workgroupName}")
    while True:
        response = rsServerlessClient.get_workgroup(workgroupName=workgroupName)
        time.sleep(10)
        if response['workgroup']['status'] == "AVAILABLE":
            break
    time.sleep(1)
    response = rsServerlessClient.delete_workgroup(workgroupName=workgroupName)
    if response['workgroup']['status'] == "DELETING":
        print(f"Deleting workgroup {workgroupName}")
    else:
        print(f"Failed to delete workgroup {workgroupName}")
        
    # delete namespace
    print(f"ready to delete redshift serverless namespace {namespaceName}")
    while True:
        response = rsServerlessClient.get_namespace(namespaceName=namespaceName)
        time.sleep(10)
        if response['namespace']['status'] == "AVAILABLE":
            break
    time.sleep(1)
    response = rsServerlessClient.delete_namespace(namespaceName=namespaceName)
    if response['namespace']['status'] == "DELETING":
        print(f"Deleting namespace {namespaceName}")
    else:
        print(f"Failed to delete namespace {namespaceName}")

    print("Deletion completed")


if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "deleteNamespaceWorkgroup", "#" * 20)
    deleteWorkgroupNamespace(session)
