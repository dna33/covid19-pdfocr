import fitz
import pandas as pd
import requests
import time
import os
import numpy as np

class padron:
    def __init__(self, comunas,comuna):
        self.comunas = comunas
        self.comuna = comuna
        self.file = ''
        self.url =self.comunas[comuna]


    def load_file(self):
        print('Request processing for ' + self.comuna)
        r = requests.get(self.url, stream=True)
        print(r.headers['content-length'])
        if os.path.exists(self.comuna+'_padron.pdf'):
            self.file = self.comuna+'_padron.pdf'
        else:
            with open(self.comuna+'_padron.pdf', 'wb') as fd:
                for chunk in r.iter_content(int(int(r.headers['content-length'])/1000)):
                    fd.write(chunk)
            self.file = self.comuna + '_padron.pdf'


    def get_info(self):
        if os.path.exists(self.comuna+'_padron.csv'):
            print('Tamos ready')
        else:
            doc = fitz.open(self.file)
            padron = []

            for page in doc:
                print("Parsing Page " + str(page.number) + "/" + str(len(doc)))
                dic = page.getText("dict")
                for block in dic['blocks'][156:]:
                    # dependiendo de cuantas lineas tiene el bloque (parrafo) es como se interpreta el orden de los campos
                    if len(block['lines']) == 5:
                        nombre = block['lines'][0]['spans'][0]['text']
                        ci = block['lines'][1]['spans'][0]['text']

                        genero_direccion = block['lines'][2]['spans'][0]['text']
                        gd_index = genero_direccion.find(' ')

                        genero = genero_direccion[:gd_index]

                        direccion = genero_direccion[gd_index + 1:]
                        circunscripcion = block['lines'][3]['spans'][0]['text']
                        mesa = block['lines'][4]['spans'][0]['text']


                    else:
                        nombre = block['lines'][0]['spans'][0]['text']
                        ci = block['lines'][1]['spans'][0]['text']

                        # ej: ' CONVENTO VIEJO 171 CALLE CONVENTO VIEJO CALLE CONVENTO VIEJO 171 CHIMBARONGO'
                        genero_direccion_circunscripcion = block['lines'][2]['spans'][0]['text']

                        genero_index = genero_direccion_circunscripcion.find(' ')
                        genero = genero_direccion_circunscripcion[:genero_index]

                        direccion_index = genero_direccion_circunscripcion.rfind(' ')
                        direccion = genero_direccion_circunscripcion[genero_index + 1:direccion_index]
                        circunscripcion = genero_direccion_circunscripcion[direccion_index + 1:]

                        mesa = block['lines'][3]['spans'][0]['text']

                    # print(nombre, ci, genero, direccion, circunscripcion, mesa, sep=',')

                    padron.append({
                        'Nombre': nombre,
                        'CI': ci,
                        'Genero': genero,
                        'Direccion': direccion,
                        'Circunscripcion': circunscripcion,
                        'Mesa': mesa
                        # 'Region': region,
                        # 'Provincia': provincia,
                        # 'Comuna': comuna
                    })

                # print('End page')

            padron_df = pd.DataFrame(padron)
            padron_df.to_csv(self.comuna+'_padron.csv', index=False)

            print('End')

class resultados:
    def __init__(self,eleccion,url,afinidad):
        self.eleccion = eleccion
        self.afinidad = afinidad
        self.url = url
        self.file = ''
        self.df_distrito = pd.DataFrame()
        self.df = pd.DataFrame()

    def load_xlsx(self):
        print('XLSX processing for ' + self.eleccion)
        r = requests.get(self.url, stream=True)
        print(r.headers['content-length'])
        self.file = self.eleccion + '_resultados.xlsx'
        if os.path.exists(self.file):
            self.file = self.eleccion + '_resultados_d10.csv'
            if os.path.exists(self.file):
                print('tamos ready')
                self.df = pd.read_csv(self.file)
            else:
                t0 = time.time()
                self.df = pd.read_excel(self.eleccion + '_resultados.xlsx')
                t1 = time.time()
                print('Time to process ' + self.file + ' ' + str((t1 - t0) / 60) + ' minutos')

        else:
            with open(self.file, 'wb') as fd:
                for chunk in r.iter_content(int(int(r.headers['content-length'])/1000)):
                    fd.write(chunk)
            t0 = time.time()
            self.df = pd.read_excel(self.file)
            t1 = time.time()
            print('Time to process ' + self.file + ' ' + str((t1 - t0) / 60) + ' minutos')


    def load_csv(self):
        if self.eleccion == 'Presidenciales 2017':
            self.df_distrito = self.df.loc[self.df['Circ.Senatorial'] == '7a Circunscripción']
            self.df_distrito = self.df_distrito.loc[self.df_distrito['Distrito'] == '10° Distrito']
            self.df_distrito.to_csv(self.eleccion + '_resultados_d10.csv', index=False)
            self.candidates = self.df_distrito[['Candidato']]
            self.candidates.drop_duplicates(inplace=True)
            self.candidates.reset_index(drop=True)
            self.candidates.to_csv(self.eleccion + '_candidates.csv', index=False)
        elif self.eleccion == 'Diputados 2017':
            self.df_distrito = self.df.loc[(self.df['Distrito'] == 10)]
            self.df_distrito.to_csv(self.eleccion + '_resultados_d10.csv', index=False)
            self.candidates = self.df_distrito[['Pacto','Partido', 'Candidato']]
            self.candidates.drop_duplicates(inplace=True)
            self.candidates.reset_index(drop=True)
            self.candidates.to_csv(self.eleccion + '_candidates.csv', index=False)
        else:
            self.df_distrito = self.df.loc[(self.df['Distrito'] == 10)]
            self.df_distrito.to_csv(self.eleccion + '_resultados_d10.csv', index=False)
            self.candidates = self.df_distrito[['Pacto', 'Sub Pacto','Partido', 'Candidato']]
            self.candidates.drop_duplicates(inplace=True)
            self.candidates.reset_index(drop=True)
            self.candidates.to_csv(self.eleccion + '_candidates.csv', index=False)


    def get_info(self):
        # crear probabilidad de voto
        self.df_distrito.rename(columns={
            'Votos TER':'Votos',
            'Votos TRICEL':'Votos'
        },inplace=True)
        self.df_distrito.rename(columns={
            'Votos TER': 'Votos',
            'Votos TRICEL': 'Votos'
        }, inplace=True)
        self.df_distrito['alcance'] = self.df_distrito['Votos']

        if self.eleccion == 'Concejales 2016 TER 1':
            columnas = ['Circunscripción', 'Mesa Nº', 'Tipo','alcance']
        elif self.eleccion == 'Concejales 2016 TER 2':
            columnas = ['Circunscripción', 'Mesa Nº', 'Tipo','alcance']
        elif self.eleccion == 'Diputados 2016':
            columnas = ['Circunscripción', 'Mesa Nº', 'Tipo','alcance']
        elif self.eleccion == 'Presidenciales 2017':
            columnas = ['Circunscripción', 'Mesa Nº', 'Tipo', 'alcance']

        self.df_alcance_mesa = self.df_distrito[columnas].groupby(['alcance']).sum

    def geolocalize(self):
        return
        #aplicar coordenadas
        #crear mapa

    def save(self):
        return

if __name__ == "__main__":
    # Procesar el padron actual, para cada eleccion servel cambia url :(
    padron_comunas = {
        'LA GRANJA': 'https://cdn.servel.cl/padron/A13111.pdf',
        'ÑUÑOA': 'https://cdn.servel.cl/padron/A13120.pdf',
        'MACUL': 'https://cdn.servel.cl/padron/A13118.pdf',
        'PROVIDENCIA': 'https://cdn.servel.cl/padron/A13123.pdf',
        'SANTIAGO': 'https://cdn.servel.cl/padron/A13101.pdf',
        'SAN JOAQUIN': 'https://cdn.servel.cl/padron/A13129.pdf'}
    t2 = time.time()
    for comuna in padron_comunas:
        t0 = time.time()
        my_padron=padron(padron_comunas,comuna)
        my_padron.load_file()
        my_padron.get_info()
        t1 = time.time()
        print('Time to process padron comuna '+comuna+' ' + str((t1 - t0)/60)+' minutos')
    t3 = time.time()
    print('Time to process padron del distrito 10: ' + str((t3 - t2)/60)+ ' minutos')

    # Definir probabilidad de voto por eleccion y afinidad de candidato, por eleccion y filiacion a partido, por eleccion y blanco-nulo
    eleccion ={
        'Concejales 2016 TER 1':'https://www.servel.cl/wp-content/uploads/2017/02/13_Resultados_Mesa_Concejales_TER_1.xlsx',
        'Concejales 2016 TER 2':'https://www.servel.cl/wp-content/uploads/2017/02/13_Resultados_Mesa_Concejales_TER_2.xlsx',
        'Diputados 2017':'https://www.servel.cl/wp-content/uploads/2018/03/13_Resultados_Mesa_DIPUTADOS_Tricel.xlsx',
        'Presidenciales 2017':'https://www.servel.cl/wp-content/uploads/2018/03/Resultados_Mesa_PRESIDENCIAL_Tricel_1v.xlsx'
    }
    afinidad_concejales = {
        '':'',
    }
    afinidad_diputados = {
        '':'',
    }
    afinidad_presidenciales = {
        '':'',
    }
    for ele in eleccion:
        mis_resultados=resultados(ele,eleccion[ele],afinidad_concejales)
        mis_resultados.load_xlsx()
        mis_resultados.load_csv()
        mis_resultados.get_info()
        #mi_mapa.geolocalize()
        #mi_mapa.save()