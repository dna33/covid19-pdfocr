import pandas as pd

TABLAS_REPORTE_DIARIO = {1: 'CasosConfirmadosNivelNacional', 2: 'ExamenesRealizadosNivelNacional',
                         3: 'HospitalizacionUCIRegion', 4: 'HospitalizacionUciEtario'}



def regionName(df):
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
        #print(each_table)
        pd_table = pd.DataFrame(each_table[table_key])
        #print(pd_table)
        #aux = (pd_table.columns.str.replace(" ", ""))
        df_list.append(pd_table.T)
    return df_list



def tableIdentifier(tables):
    """
    tableIdentifier identifica las tablas que salen de textract en fucion de sus encabezados.

    :param tables: pandas dataframe
    :return:
    """
    # la unica forma confiable de identificar las tablas es en base a sus columnas.
    # asumo que textract ide
    lenTables = len(tables)
    print(' Got ' + str(lenTables) + ' tables to identify')
    for i in range(lenTables):
        table_key = 'Table_' + str(i+1)
        each_table = tables[i]
        #print(each_table)
        #print(each_table[table_key])
        nombres_columnas = each_table[table_key][1]
        #print(nombres_columnas)
        print(nombres_columnas.values())

        if 'Casosnuevos' in nombres_columnas.values(): #and\
            #'Casos Totales 'in nombres_columnas.values() and\
            #'% Total 'in nombres_columnas.values() and\
            #'Fallecidos ' in nombres_columnas.values():
            print(each_table + ' es de tipo ' + TABLAS_REPORTE_DIARIO[1])
        elif '# examenes realizados 'in nombres_columnas.values() and\
            '% total 'in nombres_columnas.values() and\
             '# examenes informados ultimas 24 hrs' in nombres_columnas.values():
            print(each_table + ' es de tipo ' + TABLAS_REPORTE_DIARIO[2])


