import pandas as pd
df2 = pd.DataFrame()
for comuna in ['macul','nunoa','providencia','santiago','la_granja','san_joaquin']:
    geocode = comuna+'_geocode.csv'
    comuna = comuna+'.csv'
    df1 = pd.read_csv(comuna)
    df0 = pd.read_csv(geocode)
    aux = pd.concat([df0,df1[['Mesa']]],axis=1)
    df2 = df2.append(aux, ignore_index=True)
df2.comuna.replace({
    'LaGranja':'LA GRANJA',
    'SanJoaquin':'SAN JOAQUIN',
    'Santiago':'SANTIAGO',
    'Ñuñoa':'ÑUÑOA',
    'Macul':'MACUL',
    'Providencia':'PROVIDENCIA'
},inplace=True)
df3 = pd.read_csv('Diputados 2017_resultados_d10.csv')
df4 = pd.read_csv('Intención de Votos - Diputados.csv',header=None)
df5 =  pd.merge(df3,df4[[2,3]], left_on='Candidato',right_on=2)
df5 = df5[['Comuna','Mesas Fusionadas','Candidato','Votos TRICEL',3]]
df5['intencion_voto'] = df5['Votos TRICEL']*df5[3]
df5['key'] = df5['Comuna']+' '+ df5['Mesas Fusionadas']
df6 = df5.groupby(['key'])['intencion_voto'].agg('sum')
df2['key'] = df2['comuna']+' '+df2.Mesa.str.replace(' ', '')
df7 = pd.merge(df2, df6, on='key')
df7.dropna(inplace=True)
df7.to_json('Diputados 2017_Intencion de voto_Giovi.json')