from flask import Flask
from flask import request
import os
import pandas as pd
import re

from cryptography.fernet import Fernet

def encrypt(message: bytes, key: bytes) -> bytes:
    return Fernet(key).encrypt(message)

def decrypt(token: bytes, key: bytes) -> bytes:
    return Fernet(key).decrypt(token)

token = b'gAAAAABk3Neip5QGf0g3rcawgzgT_8g2J3g2MQe-ztuSkEze-iqyvmOXYALotE1K3LTBENdSRsZ2sISu1leM5n3CbQQ2DZB5SvyYrcypShqW0xVlG6QQcWsTQM06wfACUWRA3WExs3dE'
key = b'5MSbG4mUnh-iBE6DqRqb7Uq5leynl-FmFUVKqLPj1r8='

from ibm_watson import DiscoveryV2
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

authenticator = IAMAuthenticator(decrypt(token, key).decode())
discovery = DiscoveryV2(
    version='2023-08-01',
    authenticator=authenticator
)

discovery.set_service_url('https://api.eu-de.discovery.watson.cloud.ibm.com/instances/2aa899bd-bc86-4a8e-9fe9-e6ea2d1203f6')

dic_tipo_contenedor = ['40HC','45GP','40ST','40 ST',
                       '40HC','40\' HIGH CUBE','40 HC','40hc','40 hc',
                       '42GP','42 GP','42gp','42 gp',
                       '45GP','45 GP','45gp','45 gp',
                       '45RT','45 RT','45rt','45 rt',
                       '20ST','20 ST','20st','20 st',
                       '20DV','20 DV','20\' DRY VAN','20dv','20 dv',
                       '40DV','40 DV','40dv','40 dv','40\' DRY VAN'
                       '22BU','22GP','22HR','22PF','8888','22PC','22RT','22TN',
                       '22UT','22UP','22VH','29PL','42GP','42HR','42PF','42PC',
                       '42RT','42TD','42UT','42UP','42VH','45GP','45RT','45UP',
                       '49PL','L5GP','L5R1','12TR'
                      ]

def entidades_a_df(entidades):
    i = 0
    df = pd.DataFrame(columns=['entidad','valor','certeza','inicio','fin'])
    for entidad in entidades:
        for mencion in entidad['mentions']:
            df.loc[i,'entidad'] = entidad['type']
            df.loc[i,'valor'] = mencion['text']
            df.loc[i,'certeza'] = mencion['confidence']
            df.loc[i,'inicio'] = mencion['location']['begin']
            df.loc[i,'fin'] = mencion['location']['end']
            i += 1
    df.sort_values('inicio',inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df

def unico_primero(df,campo):
    df = df.drop(df[(df['entidad']==campo)].iloc[1:].index)
    return df
#unico_primero(NER_df,'numero_guia')

def unico_maximo(df,campo):
    new_df = df[(df['entidad']==campo)].copy()
    new_df.valor = new_df.valor.apply(lambda x:float(x.replace(',','')))
    return df.drop(new_df[new_df.valor != new_df.valor.max()].index)
#unico_maximo(NER_df,'peso_bruto_total')

def quita_duplicados(df,campo):
    new_df = df[df.entidad==campo].copy()
    todos = new_df.index
    primeros = new_df.drop_duplicates('valor',keep='first').index
    duplicados = [x for x in todos if x not in primeros]
    return df.drop(duplicados)
#quita_duplicados(NER_df,'numero_contenedor')

def cuenta_unicos(df,campo):
    return df[df.entidad == campo].valor.nunique()
#cuenta_unicos(NER_df,'numero_contenedor')

def mantiene_primeros_n(df,campo,n):
    return df.drop(list(df[df.entidad == campo].index)[n:])
#mantiene_primeros_n(NER_df,'tipo_contenedor',cuenta_unicos(NER_df,'numero_contenedor'))

def QA_peso(peso):
    if type(peso) == str:
        if re.search('[a-zA-Z]', peso):
            alertas.append(peso+':'+'QA:peso mal formado')
            respuesta = peso
        else:
            peso = peso.replace(',','')
            entero = peso.split('.')[0]
            decimal = peso.split('.')[1][:-3]
            respuesta = float(entero + '.' + decimal)
    else:
        respuesta = peso
    return respuesta
#QA_peso('7118.080475040.000')

def QA_tipo_contenedor(dic,tipo):
    if tipo in dic:
        respuesta = tipo
    else:
        respuesta = tipo
        alertas.append(tipo + ':' + 'QA:tipo de contenedor desconocido')
    return respuesta
#QA_tipo_contenedor(dic_tipo_contenedor,'40HC')

def QA_peso(peso):
    if type(peso) == str:
        if re.search('[a-zA-Z]', peso):
            alertas.append(peso+':'+'QA:peso mal formado')
            respuesta = peso
        else:
            peso = peso.replace(',','')
            entero = peso.split('.')[0]
            decimal = peso.split('.')[1][:-3]
            respuesta = float(entero + '.' + decimal)
    else:
        respuesta = peso
    return respuesta
#QA_peso('7118.080475040.000')

def QA_numero_contenedor(numero):
    patron = re.compile("([a-zA-Z]{3})([UJZujz])(\s{0,2})(\d{6})(\d)")
    if patron.match(numero) == None:
        alertas.append(numero + ':' + 'QA:numero de contenedor no cuadra con ISO-6346')
        respuesta = numero
    else:
        respuesta = numero
    return respuesta
#QA_numero_contenedor('FANU 1705033')

def funcion(row):
    if row.entidad == 'peso_bruto' or row.entidad == 'peso_bruto_total':
        row.valor = QA_peso(row.valor)
    elif row.entidad == 'numero_contenedor':
        row.valor =  QA_numero_contenedor(row.valor)
    elif row.entidad == 'tipo_contenedor':
        row.valor =  QA_tipo_contenedor(dic_tipo_contenedor,row.valor)
    return row

alertas = []

lista_proyectos = discovery.list_projects().get_result()
proyecto = lista_proyectos['projects'][1]['project_id']
lista_colecciones = discovery.list_collections(proyecto).get_result()
coleccion = lista_colecciones['collections'][1]['collection_id']
lista_documentos = discovery.list_documents(proyecto,coleccion).get_result()

filtro = ''
consulta = ''
resultado = discovery.query(project_id=proyecto,collection_ids=[coleccion],filter=filtro,query=consulta,count=2000).get_result()
results_df = pd.DataFrame(resultado['results'])

for index,row in results_df.iterrows():
	entidades = row['enriched_text'][0]['entities']
	entidades_df = entidades_a_df(entidades)
	lista_numeros_guia = entidades_df[entidades_df.entidad == 'numero_guia']
	results_df.loc[index,'filename'] = results_df.loc[index,'extracted_metadata']['filename']
	if len(lista_numeros_guia) > 0:
		results_df.loc[index,'numero_guia'] = lista_numeros_guia.iloc[0]['valor']

results_df.drop(columns=['result_metadata','metadata','text','extracted_metadata','table_results_references','document_passages'],inplace=True)
lista_numeros_guia = [num_guia for num_guia in list(results_df.numero_guia) if str(num_guia) != 'nan']


app = Flask(__name__)

# set up root route
@app.route("/")
def hello_world():
	return "¡Hola Radar!"

@app.route("/getDocument", methods=['GET'])
def getDocument():
	numero_guia = request.args.get('numero_guia')
	entidades = results_df.loc[results_df.numero_guia == numero_guia,'enriched_text'].values[0][0]['entities']
	# funciones de ajuste
	NER_df = entidades_a_df(entidades)
	NER_df = unico_primero(NER_df,'numero_guia')
	NER_df = unico_primero(NER_df,'id_transportista')
	NER_df = unico_primero(NER_df,'fecha_entrada')
	NER_df = unico_maximo(NER_df,'peso_bruto_total')
	NER_df = quita_duplicados(NER_df,'numero_contenedor')
	NER_df = mantiene_primeros_n(NER_df,'tipo_contenedor',cuenta_unicos(NER_df,'numero_contenedor'))
	NER_df = mantiene_primeros_n(NER_df,'peso_bruto',cuenta_unicos(NER_df,'numero_contenedor'))
	return NER_df.apply(lambda row:funcion(row),axis = 1).to_json()

# Get the PORT from environment
port = os.getenv('PORT', '8080')
if __name__ == "__main__":
	app.run(host='0.0.0.0',port=int(port))

