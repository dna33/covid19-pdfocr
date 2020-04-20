import time
import boto3
import glob
from botocore.exceptions import ClientError

#PDF are processed asynchronously: files must be on S3


def checkIfFileIsOnS3(bucket, object_name):
    s3_client = boto3.client('s3')
    try:
        #Check if file was already uploaded
        response = s3_client.list_objects(Bucket='do-covid19')
        for content in response.get('Contents', []):
            if object_name == content.get('Key'):
                print(object_name + ' was already uploaded')
                return True
            else:
                return False
    except ClientError as e:
        print(e)
        return False


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        #Check if file was already uploaded
        response = s3_client.list_objects(Bucket='do-covid19')
        for content in response.get('Contents', []):
            if object_name == content.get('Key'):
                print(object_name + ' was already uploaded')
                return
        response = s3_client.upload_file(file_name, bucket, object_name)

    except ClientError as e:
        print(e)
        return False
    return True


def preparePathsForUpload(path):
    #informePath = '../raw/InformeEpidemiologico/*.pdf'
    #reportePath = '../raw/ReporteDiario/*.pdf'
    myList = []
    for file in glob.glob(path):
        filename = file.replace('\\', '/')
        s3path = filename.replace('../raw/', '')
        print('filename: ' + filename + ' and will be stored at ' + s3path)
        myList.append([filename, s3path])
    return myList


def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract', region_name='us-east-1')
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3BucketName,
                'Name': objectName
            }
        },
        FeatureTypes=[
            'TABLES'
        ],
    )
    return response["JobId"]


def isJobComplete(jobId):
    time.sleep(10)
    client = boto3.client('textract', region_name='us-east-1')
    #response = client.get_document_text_detection(JobId=jobId)
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while (status == "IN_PROGRESS"):
        time.sleep(10)
        #response = client.get_document_text_detection(JobId=jobId)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status


def getJobResults(jobId):
    pages = []

    time.sleep(5)

    client = boto3.client('textract', region_name='us-east-1')
    response = client.get_document_analysis(JobId=jobId)

    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None

    if ('NextToken' in response):
        nextToken = response['NextToken']

    while (nextToken):
        time.sleep(5)

        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if ('NextToken' in response):
            nextToken = response['NextToken']

        print(type(pages))
        for page in pages:
            print(type(page))
            print('page is :\n' + str(page))
            blocks = page['Blocks']
            print('blocks are :\n' + str(blocks))
            print(type(blocks))
            blocks_map = {}
            table_blocks = []
            for block in blocks:
                blocks_map[block['Id']] = block
                if block['BlockType'] == "TABLE":
                   print(block)



if __name__ == "__main__":
    # upload files to s3 from these paths
    myS3 = 'do-covid19'
    informePath = '../raw/InformeEpidemiologico/*.pdf'
    reportePath = '../raw/ReporteDiario/*.pdf'
    inf = preparePathsForUpload(informePath)
    for eachinf in inf:
        upload_file(eachinf[0], 'do-covid19', eachinf[1])
    rep = preparePathsForUpload(reportePath)
    for eachrep in rep:
        upload_file(eachrep[0], 'do-covid19', eachrep[1])


    # Document
    documentName = rep[0][1]
    print('Testing with ' + documentName)

    jobId = startJob(myS3, documentName)
    print("Started job with id: {}".format(jobId))
    if (isJobComplete(jobId)):
        response = getJobResults(jobId)
