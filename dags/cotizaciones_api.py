# Importación de librerías a utilizar:

import os
import datetime
import json
import pandas as pd
import requests as req
import psycopg2 as pg2
from psycopg2.extras import execute_values
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Seteo de variables presentes en el archivo 'variables.json':

current_dir = os.path.dirname(os.path.abspath(__file__))

json_file_path = os.path.join(current_dir, "variables.json")
# with open("variables.json") as json_file:
with open(json_file_path) as json_file:
    variables_json = json.load(json_file)

empresas_trabajo = variables_json["Nombres_Empresas"]

csv_file_path = os.path.join(current_dir, "empresas.csv")
# df_temporal = pd.read_csv("empresas.csv", sep = ';', index_col = 'Simbolo')
df_temporal = pd.read_csv(csv_file_path, sep = ';', index_col = 'Simbolo')
df_rubros = df_temporal.loc[empresas_trabajo]
df_rubros = df_rubros.reset_index()
df_rubros = df_rubros.rename(columns={'Simbolo': 'Codigo', 'Nombre': 'Empresa'})

nombres = df_rubros.set_index('Codigo')['Empresa'].to_dict()

columnas_df = variables_json["Columnas_DataFrame"]
dict_aws = variables_json["Columnas_Redshift"]
list_aws = list(variables_json["Columnas_Redshift"].keys())
credenciales = variables_json["Credenciales"]
limites = variables_json["Limites"]

CLAVE_API = credenciales["API_KEY"]
USUARIO_BD = credenciales["DW_USER"]
CONTRASENA_BD = credenciales["DW_PASSWORD"]
HOST_BD = credenciales["DW_HOST"]
PUERTO_BD = credenciales["DW_PORT"]
NOMBRE_BD = credenciales["DW_NAME"]
SENDGRID_API_KEY = credenciales["SENDGRID_API_KEY"]
EMAIL_DESDE = credenciales["EMAIL_FROM"]
EMAIL_PARA = credenciales["EMAIL_TO"]


# Extracción desde la API de los datos diarios de "Los 7 Magníficos" (Alphabet, Amazon, Apple, Meta, Microsoft, Nvidia y Tesla) correspondiente al NASDAQ tomando en cuenta el último año (sin contar Sabados y Domingos):

def get_stocks():
# def obtencion_cotizaciones(ti):

    bolsa = 'NASDAQ'
    intervalo = '1day'
    formato = 'JSON'
    tamaño_output = '260'

    empresas_api = ''
    contador_empresas_api = 1

    for i in empresas_trabajo:
        if contador_empresas_api != len(empresas_trabajo):
                empresas_api = empresas_api + i + ','
        else:
                empresas_api = empresas_api + i
        contador_empresas_api += 1

    try:
        api = 'https://api.twelvedata.com/time_series?symbol=' + empresas_api + '&exchange=' + bolsa + '&interval=' + intervalo + '&format=' + formato + '&outputsize=' + tamaño_output + '&apikey=' + CLAVE_API
        peticion = req.get(api)
        print('Conexión exitosa a la API y obtención de los datos.')
    except Exception as e:
        print('Conexión fallida a la API y obtención de los datos.')
        print(e)

    # Conversión de datos a formato JSON:

    datos_json = peticion.json()

    # Normalización de datos y agregado de columnas:

    datos_df = pd.DataFrame()
    claves = list(datos_json.keys())
    contador = 0

    for empresa in datos_json:
        tabla = pd.json_normalize(datos_json[empresa]['values'])
        tabla['Codigo'] = claves[contador]
        datos_df = pd.concat([datos_df, tabla], axis = 0)
        contador += 1

    datos_df.columns = columnas_df

    datos_df = datos_df.drop_duplicates()
    datos_df = datos_df.sort_values(by = ['Fecha_Generacion', 'Codigo'], ascending = [False, True],ignore_index = True)

    datos_df['Clave_Compuesta'] = datos_df.Codigo.str.cat(datos_df.Fecha_Generacion)
    datos_df['Fecha_Extraccion'] = str(datetime.datetime.now())

    datos_df = datos_df.merge(df_rubros, on = 'Codigo', how = 'left')
    dataframe_json = datos_df.to_json()

    # ti.xcom_push(key = 'cotizaciones', value = dataframe_json)
    return(dataframe_json)


# Conexión a la base de datos en Amazon Redshift y creación del cursor:

def create_tables():
    try:
        conexion = pg2.connect(host = HOST_BD, port = PUERTO_BD, dbname = NOMBRE_BD, user = USUARIO_BD, password = CONTRASENA_BD)
        print('Conexión exitosa a la base de datos para la creación de las tablas Staging y Destino.')
    except Exception as e:
        print('Conexión fallida a la base de datos para la creación de las tablas Staging y Destino.')
        print(e)

    # Creación de las tablas principal y staging en Amazon Redshift:

    columnas_query_create = ''
    contador_create = 1

    for i in dict_aws:
            if contador_create != len(dict_aws):
                    columnas_query_create = columnas_query_create + i + ' ' + dict_aws[i] + ','
            else:
                    columnas_query_create = columnas_query_create + i + ' ' + dict_aws[i]
            contador_create += 1

    query_create = 'CREATE SCHEMA IF NOT EXISTS b_arganaraz_londero_coderhouse;' + '''
    
    CREATE TABLE IF NOT EXISTS b_arganaraz_londero_coderhouse.cotizacion_magnificos(''' + columnas_query_create + ');' + '''

    CREATE TABLE IF NOT EXISTS b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging(''' + columnas_query_create + ');'

    try:
        with conexion.cursor() as cursor:
            cursor.execute(query_create)
        print('Creación correcta de las tablas Staging y Destino.')
    except Exception as e:
        print('Creación fallida de las tablas Staging y Destino.')
        print(e)
        
    conexion.commit()
    conexion.close


# Insercion de datos a la tabla staging creada en Amazon Redshift:

def insert_data(cotizaciones_json):
# def insercion_datos(ti):
    try:
        conexion = pg2.connect(host = HOST_BD, port = PUERTO_BD, dbname = NOMBRE_BD, user = USUARIO_BD, password = CONTRASENA_BD)
        print('Conexión exitosa a la base de datos para la inserción de los datos en la tabla.')
    except Exception as e:
        print('Conexión fallida a la base de datos para la inserción de los datos en la tabla.')
        print(e)

    columnas_query_insert = ''
    contador_insert = 1

    for x in dict_aws:
            if contador_insert != len(dict_aws):
                    columnas_query_insert = columnas_query_insert + x + ', '
            else:
                    columnas_query_insert = columnas_query_insert + x
            contador_insert += 1

    query_insert = 'INSERT INTO b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging(' + columnas_query_insert + ') VALUES %s;'

    # datos_dict = ti.xcom_pull(key = 'cotizaciones', task_ids = 'obtencion_cotizaciones')
    # datos_df = pd.DataFrame(eval(datos_dict))
    datos_dict = str(cotizaciones_json)
    print(datos_dict)
    print(type(datos_dict))
    datos_df = pd.DataFrame(eval(datos_dict))

    valores = [tuple(var) for var in datos_df[list_aws].to_numpy()]

    try:
        with conexion.cursor() as cursor:
            execute_values(cursor, query_insert, valores)
        print('Inserción correcta de los datos en la tabla Staging.')
    except Exception as e:
        print('Inserción fallida de los datos en la tabla Staging.')
        print(e)

    conexion.commit()

    # Actualización incremental de la tabla principal en base a los datos de la tabla staging en Amazon Redshift:

    query_incremental = '''
    DELETE FROM b_arganaraz_londero_coderhouse.cotizacion_magnificos 
    USING b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging 
    WHERE b_arganaraz_londero_coderhouse.cotizacion_magnificos.Clave_Compuesta = b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging.Clave_Compuesta;

    INSERT INTO b_arganaraz_londero_coderhouse.cotizacion_magnificos SELECT * FROM b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging;

    DROP TABLE b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging;
    '''

    try:
        with conexion.cursor() as cursor:
            cursor.execute(query_incremental)
        print('Inserción correcta de los datos en la tabla Destino a partir de la tabla Staging.')
    except Exception as e:
        print('Inserción fallida de los datos en la tabla Destino a partir de la tabla Staging.')
        print(e)
    
    conexion.commit()
    conexion.close()


# Envío de alerta vía email segun se pasen los limites:

def verify_threshold(cotizaciones_json):
# def verificacion_limites_alerta(ti):

    # df_temp_json = ti.xcom_pull(key = 'cotizaciones', task_ids = 'obtencion_cotizaciones')
    # dataframe_v = pd.DataFrame(eval(df_temp_json))
    datos_dict = str(cotizaciones_json)
    print(datos_dict)
    print(type(datos_dict))
    dataframe_v = pd.DataFrame(eval(datos_dict))

    for empresa_lim in limites:
        cotizacion_minima = limites[empresa_lim].get("minimo")
        cotizacion_maxima = limites[empresa_lim].get("maximo")

        ultima_fecha = max(dataframe_v['Fecha_Generacion'])
        valor_cierre = float(dataframe_v.loc[(dataframe_v.Codigo == empresa_lim) & (dataframe_v.Fecha_Generacion == ultima_fecha), 'Cierre'].values[0])
        
        if cotizacion_minima > valor_cierre or cotizacion_maxima < valor_cierre:

            if cotizacion_minima > valor_cierre:
                asunto = f"La acción {empresa_lim} esta por debajo del límite mínimo"
            else:
                asunto = f"La acción {empresa_lim} esta por encima del límite máximo"

            cuerpo = f"""
                El valor de cierre de la acción {empresa_lim} es {valor_cierre}.
                Los límites son: mínimo {cotizacion_minima} y máximo {cotizacion_maxima}
            """

            mensaje = Mail(
                from_email = EMAIL_DESDE,
                to_emails = EMAIL_PARA,
                subject = asunto,
                html_content = cuerpo)

            try:
                sg = SendGridAPIClient(SENDGRID_API_KEY)
                respuesta = sg.send(mensaje)
                print('Se detectó una alerta. Envío correcto del correspondiente correo. ' + respuesta.status_code)
            except Exception as e:
                print('Se detectó una alerta. Envío incorrecto del correspondiente correo.')
                print(e)

    
# Verificación del entorno en el que se encuentra ejecutando el archivo:
    
if os.environ.get("ENTORNO") == 'Docker':
    cotizaciones = get_stocks()
    create_tables()
    insert_data(cotizaciones)
    verify_threshold(cotizaciones)