from pdfDownloader import *
from awsProcessing import *
from postAwsProcessing import *

if __name__ == '__main__':
    test = True
    if test:
        # jobId = 'bb6fc8ce8e8a1269da1f77f3adc9967eff3ea878025bbff6b2f9cac6b8ecbb87'
        # print("Started job with id: {}".format(jobId))
        # if (isJobComplete(jobId)):
        #     response = getJobResults(jobId)
        #     # result = get_table_csv_results(response)
        #     result = get_table_pd_results(response)
        #     a = pandizer(result)
        #     #print(a)
        #     aux = tableIdentifier(a)
        #     print(aux)
        #     dump2csv(aux, 'lala', '../output/ReporteDiario/')

        obtenerReporteDiario('https://www.gob.cl/coronavirus/cifrasoficiales/', '../raw/ReporteDiario/')
        myS3 = 'do-covid19'
        reportePath = '../raw/ReporteDiario/*.pdf'

        rep = preparePathsForUpload(reportePath)
        for eachrep in rep:
            sourceFile = eachrep[1].split('/')[1].replace('pdf', '')
            print('processing ' + sourceFile)
            upload_file(eachrep[0], 'do-covid19', eachrep[1])
            jobId = startJob(myS3, eachrep[1])
            if (isJobComplete(jobId)):
                response = getJobResults(jobId)
                result = get_table_pd_results(response)
                a = pandizer(result)
                aux = tableIdentifier(a)
                dump2csv(aux, sourceFile, '../output/ReporteDiario/')
    else:

        # Bajamos pdfs
        obtenerInformeEpidemiologico('https://www.gob.cl/coronavirus/cifrasoficiales/', '../raw/InformeEpidemiologico/')
        obtenerReporteDiario('https://www.gob.cl/coronavirus/cifrasoficiales/', '../raw/ReporteDiario/')
        obtenerSituacionCOVID19('http://epi.minsal.cl/informes-covid-19/', '../raw/InformeSituacionCOVID19/')

        # subimos los pdfs a s3 desde
        myS3 = 'do-covid19'
        informePath = '../raw/InformeEpidemiologico/*.pdf'
        reportePath = '../raw/ReporteDiario/*.pdf'
        situacionPath = '../raw/InformeSituacionCOVID19/*.pdf'
        inf = preparePathsForUpload(informePath)
        for eachinf in inf:
            upload_file(eachinf[0], 'do-covid19', eachinf[1])
        rep = preparePathsForUpload(reportePath)
        for eachrep in rep:
            upload_file(eachrep[0], 'do-covid19', eachrep[1])
        sit = preparePathsForUpload(situacionPath)
        for eachsit in sit:
            upload_file(eachsit[0], 'do-covid19', eachsit[1])

        # Document
        documentName = rep[0][1]
        print('Testing with ' + documentName)

        #jobId = startJob(myS3, documentName)
        jobId = 'bb6fc8ce8e8a1269da1f77f3adc9967eff3ea878025bbff6b2f9cac6b8ecbb87'
        print("Started job with id: {}".format(jobId))
        if (isJobComplete(jobId)):
            response = getJobResults(jobId)
            # result = get_table_csv_results(response)
            result = get_table_pd_results(response)
            print(type(result))
