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
        print('filename is: ' + filename)
        s3path = filename.replace('../raw/', '')
        print('and will be stored at ' + s3path)
        myList.append([filename, s3path])
    return myList



def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract', region_name='us-east-1')
    response = client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3BucketName,
                'Name': objectName
            }
        })

    return response["JobId"]


def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract', region_name='us-east-1')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while (status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status


def getJobResults(jobId):
    pages = []

    time.sleep(5)

    client = boto3.client('textract', region_name='us-east-1')
    response = client.get_document_text_detection(JobId=jobId)

    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if ('NextToken' in response):
        nextToken = response['NextToken']

    while (nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if ('NextToken' in response):
            nextToken = response['NextToken']

    return pages


# def get_rows_columns_map(table_result, blocks_map):
#     rows = {}
#     for relationship in table_result['Relationships']:
#         if relationship['Type'] == 'CHILD':
#             for child_id in relationship['Ids']:
#                 cell = blocks_map[child_id]
#                 if cell['BlockType'] == 'CELL':
#                     row_index = cell['RowIndex']
#                     col_index = cell['ColumnIndex']
#                     if row_index not in rows:
#                         # create new row
#                         rows[row_index] = {}
#
#                     # get the text value
#                     rows[row_index][col_index] = get_text(cell, blocks_map)
#     return rows
#
#
# def get_text(result, blocks_map):
#     text = ''
#     if 'Relationships' in result:
#         for relationship in result['Relationships']:
#             if relationship['Type'] == 'CHILD':
#                 for child_id in relationship['Ids']:
#                     word = blocks_map[child_id]
#                     if word['BlockType'] == 'WORD':
#                         text += word['Text'] + ' '
#                     if word['BlockType'] == 'SELECTION_ELEMENT':
#                         if word['SelectionStatus'] == 'SELECTED':
#                             text += 'X '
#     return text
#
#
# def get_table_csv_results(s3bucket, file_name):
#     with open(file_name, 'rb') as file:
#         file_test = file.read()
#         bytes_test = bytearray(file_test)
#         print('File loaded', file_name)
#
#     # process using image bytes
#     # get the results
#     client = boto3.client('textract',  region_name='us-east-1')
#
#     #response = client.analyze_document(Document={'Bytes': bytes_test}, FeatureTypes=['TABLES'])
#     response = response = client.start_document_analysis(
#     DocumentLocation={
#         'S3Object': {
#             'Bucket': s3bucket,
#             'Name': 'string',
#             'Version': 'string'
#         }
#     },
#     FeatureTypes=[
#         'TABLES'|'FORMS',
#     ],
#     ClientRequestToken='string',
#     JobTag='string',
#     NotificationChannel={
#         'SNSTopicArn': 'string',
#         'RoleArn': 'string'
#     }
# )
#
#     # Get the text blocks
#     blocks = response['Blocks']
#     pprint(blocks)
#
#     blocks_map = {}
#     table_blocks = []
#     for block in blocks:
#         blocks_map[block['Id']] = block
#         if block['BlockType'] == "TABLE":
#             table_blocks.append(block)
#
#     if len(table_blocks) <= 0:
#         return "<b> NO Table FOUND </b>"
#
#     csv = ''
#     for index, table in enumerate(table_blocks):
#         csv += generate_table_csv(table, blocks_map, index + 1)
#         csv += '\n\n'
#
#     return csv
#
#
# def generate_table_csv(table_result, blocks_map, table_index):
#     rows = get_rows_columns_map(table_result, blocks_map)
#
#     table_id = 'Table_' + str(table_index)
#
#     # get cells.
#     csv = 'Table: {0}\n\n'.format(table_id)
#
#     for row_index, cols in rows.items():
#
#         for col_index, text in cols.items():
#             csv += '{}'.format(text) + ","
#         csv += '\n'
#
#     csv += '\n\n\n'
#     return csv
#
#
# def process(file_name):
#     table_csv = get_table_csv_results(file_name)
#
#     output_file = '../output/' + file_name.replace('.pdf', '.csv')
#
#     # replace content
#     with open(output_file, "wt") as fout:
#         fout.write(table_csv)
#
#     # show the results
#     print('CSV OUTPUT FILE: ', output_file)


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
    s3BucketName = "ki-textract-demo-docs"
    documentName = rep[0][1]
    print(documentName)

    jobId = startJob(myS3, documentName)
    print("Started job with id: {}".format(jobId))
    if (isJobComplete(jobId)):
        response = getJobResults(jobId)

    # print(response)

    # Print detected text
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                print('\033[94m' + item["Text"] + '\033[0m')

