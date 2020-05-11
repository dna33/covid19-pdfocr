import time
import boto3
import glob
from botocore.exceptions import ClientError
import os

#PDF are processed asynchronously: files must be on S3

## This implementation is suboptial as we query s3 for each file.
def checkForFileOnS3(bucketName, key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        return True
    else:
        return False


def getListOfFilesOnS3(bucketName):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)
    objs = list(bucket.objects.filter(Prefix=key))
    return objs

def checkForFileOnS3Locally(listOfFiles, file):
    if len(listOfFiles) > 0 and listOfFiles[0].key == file:
        return True
    else:
        return False

def upload_folder(path, bucketname):
    for root, dirs, files in os.walk(path):
        s3_client = boto3.client('s3')
        for file in files:
            s3path = os.path.join(root, file).replace('\\', '/').replace('../', '')
            #if checkIfFileIsOnS3(bucketname, s3path):
            if checkForFileOnS3(bucketname, s3path):
                print(file + ' was uploaded')
            else:
                print('uploading ' + file)
                filepath = os.path.join(root, file)
                s3_client.upload_file(filepath, bucketname, s3path)


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
        response = s3_client.list_objects(Bucket=bucket)
        for content in response.get('Contents', []):
            if object_name == content.get('Key'):
                print(object_name + ' was already uploaded')
                return
        myResponse = s3_client.upload_file(file_name, bucket, object_name)

    except ClientError as e:
        print(e)
        return False
    return True


def preparePathsForUpload(path):
    myList = []
    for file in glob.glob(path):
        filename = file.replace('\\', '/')
        s3path = filename.replace('../input/', '')
        print('filename: ' + filename + ' will be stored at s3://do-covid-19/' + s3path)
        myList.append([filename, s3path])
    return myList


def startJob(s3BucketName, objectName):
    print('Request processing for ' + objectName)
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

    return pages

def get_table_csv_results(pages):
    # Get the text blocks
    blocks_map = {}
    table_blocks = []
    blocks_list = []
    for page in pages:
        blocks_list.append(page['Blocks'])

    for blocks in blocks_list:

        for block in blocks:
            blocks_map[block['Id']] = block
            if (block['BlockType']) == "TABLE":
                table_blocks.append(block)

        if len(table_blocks) <= 0:
            return "<b> NO Table FOUND </b>"

    csv = ''
    for index, table in enumerate(table_blocks):
        csv += generate_table_csv(table, blocks_map, index +1)
        csv += '\n\n'

    return csv


def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)
    table_id = 'Table_' + str(table_index)
    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)
    for row_index, cols in rows.items():
        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'
    csv += '\n\n\n'
    return csv


def get_table_pd_results(pages):
    # Get the text blocks
    blocks_map = {}
    table_blocks = []
    blocks_list = []
    for page in pages:
        blocks_list.append(page['Blocks'])

    for blocks in blocks_list:

        for block in blocks:
            blocks_map[block['Id']] = block
            if (block['BlockType']) == "TABLE":
                table_blocks.append(block)

        if len(table_blocks) <= 0:
            return "<b> NO Table FOUND </b>"

    tables = []
    for index, table in enumerate(table_blocks):
        tables.append(generate_table_pd(table, blocks_map, index + 1))
    return tables


def generate_table_pd(table_result, blocks_map, table_index):
    all_rows = {}
    rows = get_rows_columns_map(table_result, blocks_map)
    table_id = 'Table_' + str(table_index)
    all_rows[table_id] = rows
    return all_rows


def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text += 'X '
    return text.replace(".", "").replace(",", ".")


if __name__ == "__main__":
    # upload files to s3 from these paths
    myS3 = 'do-covid-19'
    informePath = '../input/InformeEpidemiologico/*.pdf'
    reportePath = '../input/ReporteDiario/*.pdf'
    situacionPath = '../input/InformeSituacionCOVID19/*.pdf'
    inf = preparePathsForUpload(informePath)
    for eachinf in inf:
        upload_file(eachinf[0], myS3, eachinf[1])
    rep = preparePathsForUpload(reportePath)
    for eachrep in rep:
        upload_file(eachrep[0],myS3, eachrep[1])
    sit = preparePathsForUpload(situacionPath)
    for eachsit in sit:
        upload_file(eachsit[0], myS3, eachsit[1])

    # Document
    documentName = rep[0][1]
    print('Testing with ' + documentName)

    jobId = startJob(myS3, documentName)
    print("Started job with id: {}".format(jobId))
    if (isJobComplete(jobId)):
        response = getJobResults(jobId)
        #result = get_table_csv_results(response)
        result = get_table_pd_results(response)
        print(type(result))
        #print(result)
