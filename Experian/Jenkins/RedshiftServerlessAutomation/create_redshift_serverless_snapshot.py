import boto3
import time
from pytz import timezone
from datetime import datetime
from create_redshift_serverless import set_boto_session

now = datetime.now(timezone('US/Pacific'))
now_str = now.strftime("%Y%m%d-%H%M")
snapshotName = f"rssls-snapshot-{now_str}"
MaxSnapshotNum = 3

def getNamespaceName(session):
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    NamespacesResponse = rsServerlessClient.list_namespaces()
    namespaceName = NamespacesResponse['namespaces'][0]['namespaceName']
    return namespaceName

def createSnapshot(session, snapshotName, namespaceName):
    print("snapshotName:", snapshotName)
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    response = rsServerlessClient.create_snapshot(namespaceName=namespaceName,
                                                  snapshotName=snapshotName)
    if response['snapshot']['status'] == "CREATING":
        print(f"Start Creating {snapshotName}")
    else:
        raise Exception("create failed")

def deleteObsoleteSnapshot(session):
    rsServerlessClient = session.client("redshift-serverless", region_name="us-west-2")
    response = rsServerlessClient.list_snapshots()
    allSnapshots = [(i["snapshotName"], i["snapshotCreateTime"]) for i in response['snapshots']]
    allSnapshots.sort(reverse=True, key=lambda x: x[1])
    # print(allSnapshots)
    if len(allSnapshots) <= MaxSnapshotNum:
        print(f"Number of Snapshots does not reach limit {MaxSnapshotNum}, \
                   no delete process will occur")
    else:
        print(f"Number of Snapshots reaches limit {MaxSnapshotNum}")
        for i in range(MaxSnapshotNum, len(allSnapshots)):
            snapshotName = allSnapshots[i][0]
            print(f"Dealing snapshot {snapshotName}")
            response = rsServerlessClient.delete_snapshot(snapshotName=snapshotName)
            if response["snapshot"]["status"] == "DELETED":
                print(f"Deleting snapshot {snapshotName}")


if __name__ == "__main__":
    print("#" * 20, "set_boto_session", "#" * 20)
    session = set_boto_session("251338191197", "redshift_serverless_automation")
    print("#" * 20, "getNamespaceName", "#" * 20)
    namespaceName = getNamespaceName(session)
    print("#" * 20, "create_snapshot", "#" * 20)
    createSnapshot(session, snapshotName, namespaceName)
    print("#" * 20, "delete_snapshot", "#" * 20)
    deleteObsoleteSnapshot(session)




