import pandas as pd

TABLAS_REPORTE_DIARIO = {1: 'CasosConfirmadosNivelNacional', 2: 'ExamenesRealizadosNivelNacional',
                         3: 'HospitalizacionUCIRegion', 4: 'HospitalizacionUciEtario'}



def regionName(df):
    if 'Region' in df.columns:
        print('Normalizando regiones')
        df["Region"] = df["Region"].replace({"Tarapaca": "Tarapacá", "Valparaiso": "Valparaíso",
                                         "Del Libertador General Bernardo O’Higgins": "O’Higgins",
                                         "Ohiggins": "O’Higgins", "Nuble": "Ñuble",
                                         "Biobio": "Biobío", "La Araucania": "Araucanía", "Los Rios": "Los Ríos",
                                         "Aysen": "Aysén", "Magallanes y la Antartica": "Magallanes"
                                         })

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


def tableIdentifier(tables):
    """
    tableIdentifier identifica las tablas que salen de textract en fucion de sus encabezados.

    :param tables: list of pandas dataframe
    :return:
    """
    # la unica forma confiable de identificar las tablas es en base a sus columnas.
    # asumo que textract funciona

    # REPORTE DIARIO
    headerCasosConfirmadosNivelNacional = ['Casosnuevos', 'CasosTotales', '%Total', 'Fallecidos']
    headerExámenesRealizadosNivelNacional = ['#examenesrealizados', '%total', '#examenesinformadosultimas24hrs']
    headerHospotalizacionUCIRegion = ['Region', '#pacientes', '%total']
    headerHospotalizacionUCIEtario = ['Tramosdeedad', '#pacientes', '%total']

    lenTables = len(tables)
    print(' Got ' + str(lenTables) + ' tables to identify')
    for table in tables:
        #drop empties:
        aux = [x for x in table.columns.tolist() if x]
        print(aux)
        # Normalizamos regiones
        regionName(table)
        # REPORTE DIARIO
        if set(aux).issubset(set(headerCasosConfirmadosNivelNacional)):
            print('tabla es CasosConfirmadosNivelNacional')
            print(table)
        elif set(aux).issubset(set(headerExámenesRealizadosNivelNacional)):
            print('tabla es ExámenesRealizadosNivelNacional')
            print(table)
        elif set(aux).issubset(set(headerHospotalizacionUCIRegion)):
            print('tabla es HospotalizacionUCIRegion')
            print(table)
        elif set(aux).issubset(set(headerHospotalizacionUCIEtario)):
            print('tabla es HospotalizacionUCIEtario')
            print(table)
        else:
            print('No podemos identificar la tabla:')
            print(table)
            raise Exception('No pudimos identificar la tabla')



