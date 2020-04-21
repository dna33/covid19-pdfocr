import pandas as pd

TABLAS_REPORTE_DIARIO = {1: 'CasosConfirmadosNivelNacional', 2: 'ExamenesRealizadosNivelNacional',
                         3: 'HospitalizacionUCIRegion', 4: 'HospitalizacionUciEtario'}



def regionName(df):
    if 'Region' in df.columns:
        print('Normalizando regiones')
        df['Region'] = df['Region'].str.strip()
        df["Region"] = df["Region"].replace({"Arica - Parinacota": "Arica y Parinacota", "Tarapaca": "Tarapacá",
                                            "Valparaiso": "Valparaíso", "Santiago": "Metropolitana",
                                            "Del Libertador General Bernardo O’Higgins": "O’Higgins",
                                            "Ohiggins": "O’Higgins", "Libertador Bernardo O'Higgins" : "O’Higgins",
                                            "Nuble": "Ñuble",
                                            "Biobio": "Biobío", "La Araucania": "Araucanía", "Araucania": "Araucanía",
                                            "Los Rios": "Los Ríos", "De los Rios": "Los Ríos",
                                            "De los Lagos": "Los Lagos",
                                            "Aysen": "Aysén", "Magallanes y la Antartica": "Magallanes",
                                            "": "Total"})

        codigoRegion = {'Tarapacá': '01', 'Antofagasta': '02', 'Atacama': '03', 'Coquimbo': '04', 'Valparaíso': '05',
                        'O’Higgins': '06', 'Maule': '07', 'Biobío': '08', 'Araucanía': '09', 'Los Lagos': '10',
                        'Aysén': '11', 'Magallanes': '12', 'Metropolitana': '13', 'Los Ríos': '14',
                        'Arica y Parinacota': '15', 'Ñuble': '16'}



        for region in df['Region']:
            if region in codigoRegion.keys():
                loc = df.loc[df['Region'] == region].index
                df.loc[loc, 'Codigoregion'] = codigoRegion[region]

            else:
                loc = df.loc[df['Region'] == region].index
                df.loc[loc, 'Codigoregion'] = ''
                print(region + ' no es region')

        # codigoregion quedo al final. Idealmente deberia estar junto a region
        codRegAux = df['Codigoregion']
        df.drop(labels=['Codigoregion'], axis=1, inplace=True)
        df.insert(1, 'Codigoregion', codRegAux)


def comunaName(df):
    if 'Comuna' in df.columns:
        print('Normalizando comunas')
        # Lee IDs de comunas desde página web oficial de SUBDERE
        df_dim_comunas = pd.read_excel("http://www.subdere.gov.cl/sites/default/files/documentos/cut_2018_v03.xls",
                                       encoding="utf-8")

        # Crea columna sin tildes, para hacer merge con datos publicados
        df_dim_comunas["Comuna"] = df_dim_comunas["Nombre Comuna"].str.normalize("NFKD").str.encode("ascii",
                                                        errors="ignore").str.decode("utf-8")
        print(df_dim_comunas)
        df = df.merge(df_dim_comunas, on="Comuna", how="outer")
        return(df)


def pandizer(tables):
    """
    Recibimos lista de tablas de diccionarios recursivos y las transformamos en pandas datraframes
    :param tables:lista de diccionarios: cada tabla es un diccionario compuesto de diccionarios para cada fila
    :return:
    """
    lenTables = len(tables)
    print(' Got ' + str(lenTables) + ' tables to pandize')
    df_list = []
    for i in range(lenTables):
        table_key = 'Table_' + str(i + 1)
        each_table = tables[i]
        pd_table = pd.DataFrame(each_table[table_key])
        #Remove empty spaces from headers
        aux = pd_table.transpose()
        aux.columns = aux.iloc[0]
        aux.drop(aux.index[0], inplace=True)
        aux.columns = aux.columns.str.replace(' ', '')
        df_list.append(aux)
    return df_list

def checkReporteDiario(tables):
    print(' Got ' + str(len(tables)) + ' tables to check for in reporte diario')
    reporteDiario = {}
    for table in tables:
        #drop empties:
        aux = [x for x in table.columns.tolist() if x]

        # REPORTE DIARIO nombres sin espacios por que no hay consistencia

        headerCasosConfirmadosNivelNacional = ['Casosnuevos', 'CasosTotales', '%Total', 'Fallecidos']
        headerExámenesRealizadosNivelNacional = ['#examenesrealizados', '%total', '#examenesinformadosultimas24hrs']
        headerHospotalizacionUCIRegion = ['Region', '#pacientes', '%total']
        headerHospotalizacionUCIEtario = ['Tramosdeedad', '#pacientes', '%total']
        if set(aux).issubset(set(headerCasosConfirmadosNivelNacional)):
            # primeros reportes no dicen region en el encabezado
            table.rename(columns={"": "Region"}, inplace=True)
            regionName(table)
            print('tabla es CasosConfirmadosNivelNacional')
            reporteDiario['CasosConfirmadosNivelNacional'] = table
        elif set(aux).issubset(set(headerExámenesRealizadosNivelNacional)):
            table.rename(columns={"": "Institucion"}, inplace=True)
            table["Institucion"] = table["Institucion"].replace({"" : "Total"})
            print('tabla es ExámenesRealizadosNivelNacional')
            reporteDiario['ExámenesRealizadosNivelNacional'] = table
        elif set(aux).issubset(set(headerHospotalizacionUCIRegion)):
            regionName(table)
            print('tabla es HospitalizacionUCIRegion')
            reporteDiario['HospitalizacionUCIRegion'] = table
        elif set(aux).issubset(set(headerHospotalizacionUCIEtario)):
            print('tabla es HospitalizacionUCIEtario')
            reporteDiario['HospitalizacionUCIEtario'] = table
        else:
            print('No podemos identificar la tabla:')
            raise Exception('No pudimos identificar la tabla')
    return reporteDiario

def tableIdentifier(tables):
    """
    tableIdentifier identifica las tablas que salen de textract en fucion de sus encabezados.

    :param tables: list of pandas dataframe
    :return:
    """
    # la unica forma confiable de identificar las tablas es en base a sus columnas.
    # asumo que textract funciona


    lenTables = len(tables)
    print(' Got ' + str(lenTables) + ' tables to identify')

    repDiario = checkReporteDiario(tables)
    print(repDiario)

    if repDiario:
        return repDiario

def dump2csv(dict, source, output):
    print(source + ' had ' + str(len(dict)) + ' tables')
    for k in dict.keys():
        print(k)
        print(dict[k])
        filename = output + source + '_' + str(k) + '.csv'
        dict[k].to_csv(filename, index=False)




