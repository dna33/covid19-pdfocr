import pandas as pd
df2 = pd.DataFrame()
for comuna in ['maipu','nunoa','pudahuel','estacion_central','providencia','santiago','la_granja','san_joaquin','quilicura','macul']:
    geocode = comuna+'_geocode.csv'
    comuna1 = comuna.upper()+'_padron.csv'
    df1 = pd.read_csv(comuna1)
    df0 = pd.read_csv(geocode)
    df0['comuna'] = comuna.upper()
    aux = pd.concat([df0,df1[['Mesa']]],axis=1)
    df2 = df2.append(aux, ignore_index=True)
df2.comuna.replace({
    'NUNOA':'ÑUÑOA',
    'ESTACION_CENTRAL':'ESTACION CENTRAL',
    'LA_GRANJA':'LA GRANJA',
    'SAN_JOAQUIN':'SAN JOAQUIN'
},inplace=True)
df3 = pd.read_csv('Concejales 2016 TER 1_resultados_d10_d8.csv')
aux = pd.read_csv('Concejales 2016 TER 2_resultados_d10_d8.csv')
df3 = df3.append(aux, ignore_index=True)
df4 = pd.read_csv('concejales_d10_2017.csv',header=None)
df5 =  pd.merge(df3,df4[[4,5]], left_on='Candidato',right_on=4)
df5['Mesas Fusionadas'] = df5['Mesa Nº'].astype(int).astype(str)+df5['Tipo']
df5 = df5[['Comuna','Mesas Fusionadas','Candidato','Votos TER',5]]
df5['intencion_voto'] = df5['Votos TER'].astype(int)*df5[5].astype(int)
df5['key'] = df5['Comuna']+' '+ df5['Mesas Fusionadas']
df6 = df5.groupby(['key'])['intencion_voto'].agg('sum')
df2['key'] = df2['comuna']+' '+df2.Mesa.str.replace(' ', '')
df7 = pd.merge(df2, df6, on='key')
df7.dropna(inplace=True)
df7.to_csv('Concejales 2017 TER 1  TER 2-_Intencion de voto_Diego.csv')