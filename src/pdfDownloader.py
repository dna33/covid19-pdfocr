import urllib3
from bs4 import BeautifulSoup
import shutil
import re
import os


def obtenerReporteDiario(reporte_url, path):
    req = urllib3.PoolManager()
    res = req.request('GET', reporte_url)
    soup = BeautifulSoup(res.data, features="html.parser")

    pdfs = []
    for link_soup in soup.find_all('a'):
        link = str(link_soup.get('href'))
        regex_pdf = re.compile(r"(reporte_covid19)[\w\-]*\.pdf", re.IGNORECASE)
        pdf_match = re.search(regex_pdf, link)
        if pdf_match:
            pdf_file = f'{path}{os.path.basename(link)}'
            if not os.path.isfile(pdf_file):
                with req.request('GET', link, preload_content=False) as res, open(pdf_file, 'wb') as pfopen:
                    shutil.copyfileobj(res, pfopen)
                    pdfs.append(os.path.basename(link))
            else:
                print(pdf_file + ' already downloaded ')

    return pdfs

def obtenerInformeEpidemiologico(reporte_url, path):
    req = urllib3.PoolManager()
    res = req.request('GET', reporte_url)
    soup = BeautifulSoup(res.data, features="html.parser")

    pdfs = []
    for link_soup in soup.find_all('a'):
        link = str(link_soup.get('href'))
        #regex_pdf = re.compile(r"(informe|reporte)[\w\-]*\.pdf", re.IGNORECASE)
        regex_pdf = re.compile(r"(epi|ep_)[\w\-]*\.pdf", re.IGNORECASE)

        pdf_match = re.search(regex_pdf, link)
        if pdf_match:
            pdf_file = f'{path}{os.path.basename(link)}'
            if not os.path.isfile(pdf_file):
                print('Downloading ' + pdf_file)
                with req.request('GET', link, preload_content=False) as res, open(pdf_file, 'wb') as pfopen:
                    shutil.copyfileobj(res, pfopen)
                    pdfs.append(os.path.basename(link))
            else:
                print(pdf_file + ' already downloaded ')
    return pdfs

def obtenerSituacionCOVID19(reporte_url, path):
    req = urllib3.PoolManager()
    res = req.request('GET', reporte_url)
    soup = BeautifulSoup(res.data, features="html.parser")

    pdfs = []
    for link_soup in soup.find_all('a'):
        link = str(link_soup.get('href'))
        regex_pdf = re.compile(r"(informe|reporte)[\w\-]*\.pdf", re.IGNORECASE)

        pdf_match = re.search(regex_pdf, link)
        if pdf_match:
            pdf_file = f'{path}{os.path.basename(link)}'
            if not os.path.isfile(pdf_file):
                print('Downloading ' + pdf_file)
                with req.request('GET', link, preload_content=False) as res, open(pdf_file, 'wb') as pfopen:
                    shutil.copyfileobj(res, pfopen)
                    pdfs.append(os.path.basename(link))
            else:
                print(pdf_file + ' already downloaded ')
    return pdfs

if __name__ == '__main__':
    obtenerInformeEpidemiologico('https://www.gob.cl/coronavirus/cifrasoficiales/', '../input/InformeEpidemiologico/')

    obtenerReporteDiario('https://www.gob.cl/coronavirus/cifrasoficiales/', '../input/ReporteDiario/')

    obtenerSituacionCOVID19('http://epi.minsal.cl/informes-covid-19/', '../input/InformeSituacionCOVID19/')
