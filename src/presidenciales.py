import pandas as pd
df2 = pd.DataFrame()df2.comuna.replace({
    'NUNOA':'ÑUÑOA',
    'ESTACION_CENTRAL':'ESTACION CENTRAL',
    'LA_GRANJA':'LA GRANJA',
    'SAN_JOAQUIN':'SAN JOAQUIN'
},inplace=True)
for comuna in ['maipu','nunoa','pudahuel','estacion_central','providencia','santiago','la_granja','san_joaquin','quilicura','macul']:
    geocode = comuna+'_geocode.csv'
    comuna1 = comuna.upper()+'_padron.csv'
    df1 = pd.read_csv(comuna1)
    df0 = pd.read_csv(geocode)
    df0['comuna'] = comuna.upper()
    aux = pd.concat([df0,df1[['Mesa']]],axis=1)
    df2 = df2.append(aux, ignore_index=True)

df3 = pd.read_csv('Presidenciales 2017_resultados_d10.csv')
df4 = pd.read_csv('Intención de Votos - Presidencial.csv',header=None)
df5 =  pd.merge(df3,df4, left_on='Candidato',right_on=3)
df5 = df5[['Comuna','Mesas Fusionadas','Candidato','Votos TRICEL',4]]
df5['intencion_voto'] = df5['Votos TRICEL'].astype(int)*df5[4].astype(int)
## cambiar a **Circ.Electoral** + mesa
df5['key'] = df5['Comuna']+' '+ df5['Mesas Fusionadas']
df6 = df5.groupby(['key'])['intencion_voto'].agg('sum')
df2['key'] = df2['comuna']+' '+df2.Mesa.str.replace(' ', '')
df7 = pd.merge(df2, df6, on='key')
df7.dropna(inplace=True)
df7.to_csv('Presidenciales Intencion de voto_Gabriel.csv')