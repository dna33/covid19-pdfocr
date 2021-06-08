import pandas as pd
df = pd.DataFrame()
for comuna in ['macul','nunoa','providencia','santiago','la_granja','san_joaquin']:
    geocode = comuna+'_geocode.csv'
    comuna = comuna+'.csv'
    df_1 = pd.read_csv(comuna)
    df_0 = pd.read_csv(geocode)
    aux = pd.concat([df_0,df_1[['Mesa']]],axis=1)
    df = df.append(aux, ignore_index=True)
df.comuna.replace({
    'LaGranja':'LA GRANJA',
    'SanJoaquin':'SAN JOAQUIN',
    'Santiago':'SANTIAGO',
    'Ñuñoa':'ÑUÑOA',
    'Macul':'MACUL',
    'Providencia':'PROVIDENCIA'
},inplace=True)
df1 = pd.read_csv('Presidenciales 2017_resultados_d10.csv')
df2 = pd.read_csv('Intención de Votos - Presidencial.csv',header=None)
df3 =  pd.merge(df1,df2, left_on='Candidato',right_on=0)
df3 = df3[['Comuna','Mesas Fusionadas','Candidato','Votos TRICEL',1]]
df3['intencion_voto'] = df3['Votos TRICEL']*df3[1]
df3['key'] = df3['Comuna']+' '+ df3['Mesas Fusionadas']
df4 = df3.groupby(['key'])['intencion_voto'].agg('sum')
df['key'] = df['comuna']+' '+df.Mesa.str.replace(' ', '')
df5 = pd.merge(df, df4, on='key')
df5.dropna(inplace=True)
df5.to_json('Presidenciales Intencion de voto_Giovi.json')