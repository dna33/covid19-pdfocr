from pdfDownloader import *
from awsProcessing import *
from postAwsProcessing import *
from os import listdir
from pprint import pprint

if __name__ == '__main__':
    test = False
    myS3 = 'do-covid-19'
    if test:

        # jobId = '3256e214371aea597f64a056b4094fef1e612d5accb2bffdd19f3157eb2c11f8'
        # print("Started job with id: {}".format(jobId))
        # if (isJobComplete(jobId)):
        #     response = getJobResults(jobId)
        #     # result = get_table_csv_results(response)
        #     result = get_table_pd_results(response)
        #     a = pandizer(result)
        #     print(a)

        # manyJobs = ['1613f7691076bd99cef29d85bdba25e388f69d9d61da3ff492bff8650f47b931',
        #             'd06663b849a8e20341c5926b9c10438de827f353384bccf3a3423a693e99f9c3',
        #             '7c51f381ed490a725a18b1627f338e1e24edaa92cc6f04d1208d83551833582d',
        #             '233941f06fa600cb4c1a8f806fe6a8aa2c6a079d7fe30d47636c0c1facdd15bf']
        # jobId = '3256e214371aea597f64a056b4094fef1e612d5accb2bffdd19f3157eb2c11f8'
        # print("Started job with id: {}".format(jobId))
        # for jobId in manyJobs:
        #     if (isJobComplete(jobId)):
        #         response = getJobResults(jobId)
        #         for i in response:
        #             pprint(i)
        #         # result = get_table_csv_results(response)
        #         result = get_table_pd_results(response)
        #         a = pandizer(result)
        #         print(a)



        reportePath = '../input/ReporteDiario/*.pdf'
        rep = preparePathsForUpload(reportePath)
        for eachrep in rep:
            if not checkIfFileIsOnS3(myS3, eachrep[1]):
                upload_file(eachrep[0], myS3, eachrep[1])

    else:
        # REPORTE DIARIO
        obtenerReporteDiario('https://www.gob.cl/coronavirus/cifrasoficiales/', '../input/ReporteDiario/')
        reportePath = '../input/ReporteDiario/*.pdf'
        rep = preparePathsForUpload(reportePath)
        # Como sabemos si un archivo ya se proceso??
        # Revisemos el output
        outputFiles = listdir('../output/raw/ReporteDiario')

        for eachrep in rep:
            # Check if the file was uploaded
            if not checkIfFileIsOnS3(myS3, eachrep[1]):
                upload_file(eachrep[0], myS3, eachrep[1])

            # Check if the file was processed
            sourceFile = eachrep[1].split('/')[1].replace('.pdf', '')
            if [x for x in outputFiles if sourceFile in x]:
                print(sourceFile + ' was already processed')

            else:
                print('processing ' + sourceFile)
                upload_file(eachrep[0], myS3, eachrep[1])
                jobId = startJob(myS3, eachrep[1])
                myfile = open("jobs.log", 'a+')
                myfile.write(eachrep[1] + ': ' + jobId + '\n')
                myfile.close()
                if (isJobComplete(jobId)):
                    response = getJobResults(jobId)
                    result = get_table_pd_results(response)
                    a = pandizer(result)
                    dumpDict2csv(a, sourceFile, '../output/raw/ReporteDiario/')

        # INFORME SITUACION
        obtenerSituacionCOVID19('http://epi.minsal.cl/informes-covid-19/', '../input/InformeSituacionCOVID19/')
        situacionPath = '../input/InformeSituacionCOVID19/*.pdf'
        sit = preparePathsForUpload(situacionPath)
        outputFiles = listdir('../output/raw/InformeSituacionCOVID19')
        for eachsit in sit:
            # Check if the file was uploaded
            if not checkIfFileIsOnS3(myS3, eachsit[1]):
                upload_file(eachsit[0], myS3, eachsit[1])

            # Check if the file was processed
            sourceFile = eachsit[1].split('/')[1].replace('.pdf', '')
            if [x for x in outputFiles if sourceFile in x]:
                print(sourceFile + ' was already processed')
            else:
                print('processing ' + sourceFile)
                upload_file(eachsit[0], myS3, eachsit[1])
                jobId = startJob(myS3, eachsit[1])
                myfile = open("jobs.log", 'a+')
                myfile.write(eachsit[1] + ': ' + jobId + '\n')
                myfile.close()
                if (isJobComplete(jobId)):
                    response = getJobResults(jobId)
                    result = get_table_pd_results(response)
                    a = pandizer(result)
                    dumpDict2csv(a, sourceFile, '../output/raw/InformeSituacionCOVID19/')

        # INFORME EPIDEMIOLOGICO
        obtenerInformeEpidemiologico('https://www.gob.cl/coronavirus/cifrasoficiales/',
                                     '../input/InformeEpidemiologico/')
        infPath = '../input/InformeEpidemiologico/*.pdf'
        inf = preparePathsForUpload(infPath)
        outputFiles = listdir('../output/raw/InformeEpidemiologico')
        for eachinf in inf:
            # Check if the file was uploaded
            if not checkIfFileIsOnS3(myS3, eachinf[1]):
                upload_file(eachinf[0], myS3, eachinf[1])

            # Check if the file was processed
            sourceFile = eachinf[1].split('/')[1].replace('.pdf', '')
            if [x for x in outputFiles if sourceFile in x]:
                print(sourceFile + ' was already processed')
            else:
                print('processing ' + sourceFile)
                upload_file(eachinf[0], myS3, eachinf[1])
                jobId = startJob(myS3, eachinf[1])
                myfile = open("jobs.log", 'a+')
                myfile.write(eachinf[1] + ': ' + jobId + '\n')
                myfile.close()
                if (isJobComplete(jobId)):
                    response = getJobResults(jobId)
                    result = get_table_pd_results(response)
                    a = pandizer(result)
                    dumpDict2csv(a, sourceFile, '../output/raw/InformeEpidemiologico/')

        # put the output to s3
        putOutputOnS3('../output', myS3)