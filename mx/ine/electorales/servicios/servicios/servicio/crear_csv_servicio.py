import json
import pandas as pd
from io import BytesIO
from gzip import GzipFile
import re
import math
import csv
import os
from datetime import datetime
import multiprocessing
from numbers import Number
from ...persistencia.repositorios.repositorio.repositorio_queries import RepoQueries
from .lee_parametros import define_parametros
import decimal
import numpy as np
import boto3

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, np.generic):
        return np.asscalar(obj)
    raise TypeError


def guardar(info, **params):
    key = params['ruta'] + params['archivo']
    data = json.dumps(info, default=decimal_default, separators=(',', ':'))
    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 9, gz_body)
    gz.write(data.encode('utf-8'))
    gz.close()
    data = gz_body.getvalue()
    s3 = params['boto'].resource('s3', **params.get('botocredenciales', {}))
    s3.Bucket(params['bucket']).put_object(Key=key, Body=data, ContentEncoding='gzip')
    print('guardado en ', key)
    del data

def compile_asociacion(x):
    return re.compile(r'\basociacion\B').search(x)


def flt4(x):
    return '{:.4f}'.format(math.floor(x * 10**4) / 10**4)


def sincoma(x):
    return int(str(x)) if str(x).isnumeric() else x


columnas_renombrar = {
    'ID_ESTADO': 'ID_ENTIDAD',
    'ESTADO': 'ENTIDAD',
    'ID_DISTRITO': 'ID_DISTRITO_FEDERAL',
    'BOLETAS_SOBRANTES': 'TOTAL_BOLETAS_SOBRANTES',
    'PERSONAS_VOTARON': 'TOTAL_PERSONAS_VOTARON',
    'REPRESENTANTES_P_VOTARON': 'TOTAL_REP_PARTIDO_CI_VOTARON',
    'VOTOS_SACADOS': 'TOTAL_VOTOS_SACADOS',
    'NO_REGISTRADOS': 'NO_REGISTRADAS',
    'VOTOS_ASENTADOS': 'TOTAL_VOTOS_ASENTADO',
    'CONTABILIZA': 'CONTABILIZADA',
    'SHA': 'CODIGO_INTEGRIDAD',
    'TOTAL_VOTOS_CALCULADOS': 'TOTAL_VOTOS_CALCULADO',
    'DIGITALIZAION': 'DIGITALIZACIÓN'

}

parser = ['ID_ESTADO_C', 'SECCION_C', 'TIPO_CASILLA', 'ID_CASILLA_C', 'EXT_CONTIGUA_C', 'TIPO_ACTA_C']

datos_no_empleados = ['LISTA_NOMINAL', 'CIRCUNSCRIPCION', 'ID_MUNICIPIO', 'llave_acta', 'RUTA']


def ubicacion(x):
    return f'/tmp/{x}'
    

class ActualizadorAnticipado:
    def actualizar_seccion(self, seccion, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return seccion
            
    def actualizar_distrito(self, distrito, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return distrito  
            
    def actualizar_id_distrito(self, id_distrito, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return id_distrito 
            
    def actualizar_tipoacta(self, tipoacta, tipo_casilla):
        if tipo_casilla == 'A':
            return '2PVA'
        elif tipo_casilla == 'P':
            return '2PVPP'
        else:
            return tipoacta
            
    def actualizar_idcasilla(self, idcasilla, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return idcasilla
    
    def actualizar_boletas(self, boletas, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return boletas
            
    def actualizar_personas(self, personas, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return personas
    
    def actualizar_rep_ppyci(self, ppyci, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return ppyci
            
    def actualizar_votos_sacados(self, votos, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return votos
            
    def actualizar_representantes(self, representantes, tipo_casilla):
        if tipo_casilla in ['A', 'P', 'M']:
            return 'N/A'
        else:
            return representantes

            

class CrearCSV:

    def __init__(self, **params):
        self.params = define_parametros(**params)
        self.repositorio = params.get('repositorio', RepoQueries(**self.params))
        asociaciones = self.repositorio.obtener_asociaciones()
        self.c_asociaciones = {str(asociacion[0]): asociacion[2] for asociacion in asociaciones}
        self.c_estados = {str(estado[0]): estado[1] for estado in self.repositorio.obtener_nombre_estado()}
        self.c_cabeceras_distrital = {
            f'{distrito[1]}{distrito[0]}': distrito[2]
            for distrito in self.repositorio.obtener_cabecera_distrital()
        }
        self.asociacion_nombre = lambda x: self.c_asociaciones[f'{x[-2:]}'] if len(x) > 12 else self.c_asociaciones[
            f'{x[-1:]}']
        self.pares_estado_distrito = self.repositorio.obtener_estados_distritos()
        self.header = {
            'ACTAS_ESPERADAS': 0,
            'ACTAS_REGISTRADAS': 0,
            'ACTAS_FUERA_CATALOGO': 0,
            'ACTAS_CAPTURADAS': 0,
            'PORCENTAJE_ACTAS_CAPTURADAS': 0,
            'ACTAS_CONTABILIZADAS': 0,
            'PORCENTAJE_ACTAS_CONTABILIZADAS': 0,
            'PORCENTAJE_ACTAS_INCONSISTENCIAS': 0,
            'ACTAS_NO_CONTABILIZADAS': 0,
            'LISTA_NOMINAL_ACTAS_CONTABILIZADAS': 0,
            'TOTAL_VOTOS_C_CS': 0,
            'TOTAL_VOTOS_S_CS': 0,
            'PORCENTAJE_PARTICIPACION_CIUDADANA': 0,
            'suma_observaciones': 0
        }
        self.asociaciones = [f'asociacion_{x[0]}' for x in asociaciones if float(x[1]) < 10]

        self.columnas_csv_init = [
            'ID_ACTA', 'CLAVE_CASILLA', 'CLAVE_ACTA', 'ID_ESTADO', 'ESTADO', 'ID_DISTRITO', 'DISTRITO_FEDERAL',
            'SECCION', 'ID_CASILLA', 'TIPO_CASILLA', 'EXT_CONTIGUA', 'UBICACION_CASILLA', 'TIPO_ACTA',
            'BOLETAS_SOBRANTES', 'PERSONAS_VOTARON', 'REPRESENTANTES_P_VOTARON', 'VOTOS_SACADOS', *self.asociaciones,
            'VOTOS_ASENTADOS', 'TOTAL_VOTOS_CALCULADOS', 'LISTA_NOMINAL', 'REPRESENTANTES_PP_CI', 'OBSERVACIONES',
            'CONTABILIZA', 'MECANISMOS_TRASLADO', 'SHA', 'FECHA_HORA_ACOPIO', 'FECHA_HORA_CAPTURA',
            'FECHA_HORA_VERIFICACION', 'ORIGEN', 'DIGITALIZACION', 'TIPO_DOCUMENTO', 'COTEJADA'
        ]

        self.actas_esperadas = self.repositorio.obtener_actas_esperadas()[0][0]
        self.info_actas_fuera_catalogo = self.repositorio.obtener_info_actas_fuera_catalogo()
        self.actas_fuera_catalogo = len(self.info_actas_fuera_catalogo[list(self.info_actas_fuera_catalogo.keys())[0]])
        #agregamos la información del acta voto en el extranjero 
        self.info_acta_voto_extranjero = self.repositorio.obtener_info_voto_extranjero()
        #sacamos su total para sumarlo al total de actas esperadas
        self.acta_voto_extranjero = len(self.info_acta_voto_extranjero[list(self.info_acta_voto_extranjero.keys())[0]])
        self.manager = multiprocessing.Manager()

        
    def leer(self, ruta, nombre):
        key = ruta + nombre
        if 'Contents' in self.params['s3'].list_objects_v2(Bucket=self.params['bucket'], Prefix=key):
            obj = self.params['s3'].get_object(Bucket=self.params['bucket'], Key=key)
            contenido = obj['Body'].read()
            gz_body = BytesIO(contenido)
            contenido = GzipFile(None, 'rb', fileobj=gz_body).read()
            contenido = json.loads(contenido.decode('utf-8'))
            return contenido
        else:
            print(f'no existe informacion  en {key}')
            return False

    def subir_archivo(self, archivo_leer, ruta, archivo):
        key = ruta + archivo
        self.params['s3'].upload_file(
            archivo_leer, Bucket=self.params['bucket'], Key=key, ExtraArgs={'ContentType': 'text/csv'}
        )

    def obtiene_votos(self, x):
        if x != '-':
            return sincoma(x['VOTOS'])
        return x

    def parsear(self, df):
        df = df.astype('int64', errors='ignore')
        columnas = list(df.columns)
        actasnp = df.values
        idx_asociaciones = [columnas.index(a) for a in self.asociaciones]
        actasnp[:,
                idx_asociaciones] = [[self.obtiene_votos(col) for col in row] for row in actasnp[:, idx_asociaciones]]
        return pd.DataFrame(actasnp, columns=columnas)

    def formato_fecha(self, fecha):
        if fecha != '-':
            fecha = datetime.strptime(fecha[:-7], '%Y-%m-%d %H:%M:%S')
            return fecha.strftime('%d/%m/%Y %H:%M:%S')
        else:
            return fecha

    def define_ubicacion_casilla(self, ubicacion):
        u = {1: '1', 2: '2'}
        return u.get(ubicacion, ubicacion)

    def define_origen(self, tipo):
        o = {'E': 'CATD', '-': '-'}
        return o.get(tipo, 'CASILLA')

    def define_digitalizacion(self, tipo):
        d = {'M': 'Móvil', 'E': 'Escáner', 'U': 'Urna Electrónica'}
        return d.get(tipo, tipo)

    def define_tipo_acta(self, tipo):
        ta = {
            int(self.repositorio.actas_mr): '2',
            int(self.repositorio.actas_esp): '2E',
            int(self.repositorio.actas_ve): '2PVE'
        }
        return ta.get(tipo, '')
    
    def define_distrito(self, distrito):
        dis = f'{int(distrito):03}' if distrito != 0 else 'N/A'
        dis_str = str(dis)
        return dis_str
    
    def define_nombre(self, nombre):
        nom = nombre if nombre != 'Votos en el Extranjero' else 'N/A'
        return nom
        
    def define_seccion(self, seccion):
        sec = f'{int(seccion):04}' if seccion != 0 else str('N/A')
        return sec
    
    def define_idcasilla(self, casilla):
        cas = f'{int(casilla):02}' if casilla != 0 else str('N/A')
        return cas
        
    def define_extcontigua(self, extcontigua):
        extc = f'{int(extcontigua):02}'
        return extc
        
    def define_traslado(self, traslado):
        doc = traslado if traslado != 'N' else ''
        return doc
    
    
    def tranforma_extranjero(self, df):
        columnas = list(df.columns)
        dfnp = df.values
        
        df['ID_DISTRITO_FEDERAL'] = [self.define_distrito(x) for x in dfnp[:, columnas.index('ID_DISTRITO_FEDERAL')]]
        df['DISTRITO_FEDERAL'] = [self.define_nombre(x) for x in dfnp[:, columnas.index('DISTRITO_FEDERAL')]]
        df['SECCION'] = [self.define_seccion(x) for x in dfnp[:, columnas.index('SECCION')]]
        df['ID_CASILLA'] = [self.define_idcasilla(x) for x in dfnp[:, columnas.index('ID_CASILLA')]]
        df['EXT_CONTIGUA'] = [self.define_extcontigua(x) for x in dfnp[:, columnas.index('EXT_CONTIGUA')]]
        df['MECANISMOS_TRASLADO'] = [self.define_traslado(x) for x in dfnp[:, columnas.index('MECANISMOS_TRASLADO')]]
        
        return df
        

    def transformar_fuera_catalogo(self, df):
        columnas = list(df.columns)
        dfnp = df.values
        df['ORIGEN'] = [self.define_origen(x) for x in dfnp[:, columnas.index('TIPO_DIGITALIZACION')]]
        df['DIGITALIZACION'] = [self.define_digitalizacion(x) for x in dfnp[:, columnas.index('TIPO_DIGITALIZACION')]]
        df['TIPO_ACTA'] = [self.define_tipo_acta(x) for x in dfnp[:, columnas.index('ID_TIPO_CANDIDATURA')]]
        #df['DISTRITO_FEDERAL'] = [
        #    self.c_cabeceras_distrital[str(x)[1:]] for x in dfnp[:, columnas.index('estado_distrito')]
        #]
        df['ESTADO'] = [self.c_estados[str(x)] for x in dfnp[:, columnas.index('ID_ESTADO')]]
        df['LISTA_NOMINAL'] = df['LISTA_NOMINAL_CASILLA']

        df.rename(columns={columna: self.asociacion_nombre(columna) for columna in self.asociaciones}, inplace=True)

        df.rename(columns=columnas_renombrar, inplace=True)
        return df
    
    def transformar_voto_extranjero(self, df):
        columnas = list(df.columns)
        dfnp = df.values

        df['ORIGEN'] = [self.define_origen(x) for x in dfnp[:, columnas.index('TIPO_DIGITALIZACION')]]
        df['DIGITALIZACION'] = [self.define_digitalizacion(x) for x in dfnp[:, columnas.index('TIPO_DIGITALIZACION')]]
        df['TIPO_ACTA'] = [self.define_tipo_acta(x) for x in dfnp[:, columnas.index('ID_TIPO_CANDIDATURA')]]
        df['DISTRITO_FEDERAL'] = 'Votos en el Extranjero'
        df['ESTADO'] = [self.c_estados[str(x)] for x in dfnp[:, columnas.index('ID_ESTADO')]]
        df['LISTA_NOMINAL'] = df['LISTA_NOMINAL_CASILLA']

        df.rename(columns={columna: self.asociacion_nombre(columna) for columna in self.asociaciones}, inplace=True)

        df.rename(columns=columnas_renombrar, inplace=True)
        return df
    
    
    def transformar_anticipado(self, df):
        actualizador = ActualizadorAnticipado()
        # Aplica la función usando una lista por comprensión
        df['SECCION'] = [actualizador.actualizar_seccion(seccion, tipo_casilla) 
                         for seccion, tipo_casilla in zip(df['SECCION'], df['TIPO_CASILLA'])]

        df['DISTRITO_FEDERAL'] = [actualizador.actualizar_distrito(distrito, tipo_casilla) 
                         for distrito, tipo_casilla in zip(df['DISTRITO_FEDERAL'], df['TIPO_CASILLA'])]
                         
        df['ID_DISTRITO_FEDERAL'] = [actualizador.actualizar_id_distrito(id_distrito, tipo_casilla) 
                         for id_distrito, tipo_casilla in zip(df['ID_DISTRITO_FEDERAL'], df['TIPO_CASILLA'])]
                         
        df['TIPO_ACTA'] = [actualizador.actualizar_tipoacta(tipoacta, tipo_casilla) 
                         for tipoacta, tipo_casilla in zip(df['TIPO_ACTA'], df['TIPO_CASILLA'])]
                         
        df['ID_CASILLA'] = [actualizador.actualizar_idcasilla(idcasilla, tipo_casilla) 
                         for idcasilla, tipo_casilla in zip(df['ID_CASILLA'], df['TIPO_CASILLA'])]
                         
        df['TOTAL_BOLETAS_SOBRANTES'] = [actualizador.actualizar_boletas(boletas, tipo_casilla) 
                         for boletas, tipo_casilla in zip(df['TOTAL_BOLETAS_SOBRANTES'], df['TIPO_CASILLA'])]
                         
        df['TOTAL_PERSONAS_VOTARON'] = [actualizador.actualizar_personas(personas, tipo_casilla) 
                         for personas, tipo_casilla in zip(df['TOTAL_PERSONAS_VOTARON'], df['TIPO_CASILLA'])]
                         
        df['TOTAL_REP_PARTIDO_CI_VOTARON'] = [actualizador.actualizar_rep_ppyci(ppyci, tipo_casilla) 
                         for ppyci, tipo_casilla in zip(df['TOTAL_REP_PARTIDO_CI_VOTARON'], df['TIPO_CASILLA'])]
                         
        df['TOTAL_VOTOS_SACADOS'] = [actualizador.actualizar_votos_sacados(votos, tipo_casilla) 
                         for votos, tipo_casilla in zip(df['TOTAL_VOTOS_SACADOS'], df['TIPO_CASILLA'])]
                         
        df['REPRESENTANTES_PP_CI'] = [actualizador.actualizar_representantes(representantes, tipo_casilla) 
                         for representantes, tipo_casilla in zip(df['REPRESENTANTES_PP_CI'], df['TIPO_CASILLA'])]
        return df

    
    def transformar(self, df):
        # NOTE COLUMNAS PARA CLAVE ACTA Y CLAVE CASILLA.
        df['ID_ESTADO_C'] = [f'{int(x):02}' for x in df.ID_ESTADO]
        df['SECCION_C'] = [f'{int(x):04}' for x in df.SECCION]
        df['ID_CASILLA_C'] = [f'{int(x):02}' for x in df.ID_CASILLA]
        df['EXT_CONTIGUA_C'] = [f'{int(x):02}' for x in df.EXT_CONTIGUA]
        df['TIPO_ACTA_C'] = df.ID_TIPO_CANDIDATURA

        columnas = list(df.columns)
        dfnp = df.values
        idx_clave = [columnas.index(p) for p in parser]

        dfnp = df.values

        df['CLAVE_CASILLA'] = [f"'{''.join(map(str,x))}'" for x in dfnp[:, idx_clave[:-1]]]
        df['CLAVE_ACTA'] = [f"'{''.join(map(str,x))}'" for x in dfnp[:, idx_clave]]

        df['UBICACION_CASILLA'] = [self.define_ubicacion_casilla(int(x)) for x in dfnp[:, columnas.index('UBICACION')]]
        df['ORIGEN'] = [self.define_origen(x) for x in dfnp[:, columnas.index('TIPO_DIGITALIZACION')]]
        df['DIGITALIZACION'] = [self.define_digitalizacion(x) for x in dfnp[:, columnas.index('TIPO_DIGITALIZACION')]]
        df['TIPO_ACTA'] = [self.define_tipo_acta(x) for x in dfnp[:, columnas.index('ID_TIPO_CANDIDATURA')]]
        

        df['DISTRITO_FEDERAL'] = [
            self.c_cabeceras_distrital[str(x)[1:]] for x in dfnp[:, columnas.index('estado_distrito')]
        ]
        df['ESTADO'] = [self.c_estados[str(x)] for x in dfnp[:, columnas.index('ID_ESTADO')]]

        df['LISTA_NOMINAL'] = df['LISTA_NOMINAL_CASILLA']

        df.loc[dfnp[:, columnas.index('PUBLICACION')] != '-',
               'PUBLICACION'] = dfnp[dfnp[:, columnas.index('PUBLICACION')] != '-', [columnas.index('CONTABILIZA')]]
        df['CONTABILIZA'] = df.PUBLICACION

        df.rename(columns={columna: self.asociacion_nombre(columna) for columna in self.asociaciones}, inplace=True)

        for fecha in ['FECHA_HORA_ACOPIO', 'FECHA_HORA_CAPTURA', 'FECHA_HORA_VERIFICACION']:
            df[fecha] = df[fecha].apply(self.formato_fecha)

        df.rename(columns=columnas_renombrar, inplace=True)
        return df

    def suma_columna(self, df_columna_valores):
        return sum(v for v in df_columna_valores if v != '-')

    def obtener_parte_superior(self, df, header):
        df['TOTAL_VOTOS_CALCULADOS'] = [
            sum(v for v in row if isinstance(v, Number)) for row in df[self.asociaciones].values
        ]

        columnas = list(df.columns)
        dfnp = df.values

        idx_pub = columnas.index('PUBLICACION')
        idx_cnt = columnas.index('CONTABILIZA')

        header['ACTAS_NO_CONTABILIZADAS'] += len(dfnp[(dfnp[:, idx_pub] != '-') & (dfnp[:, idx_cnt] == 0)])

        header['LISTA_NOMINAL_ACTAS_CONTABILIZADAS'] += dfnp[
            (dfnp[:, idx_pub] != '-') & (dfnp[:, idx_cnt] == 1)][:, columnas.index('LISTA_NOMINAL_CASILLA')].sum()

        header['TOTAL_VOTOS_C_CS'] += dfnp[(dfnp[:, idx_pub] != '-') &
                                           (dfnp[:, idx_cnt] == 1)][:, columnas.index('TOTAL_VOTOS_CALCULADOS')].sum()

        header['TOTAL_VOTOS_S_CS'] += dfnp[
            ((dfnp[:, idx_pub] != '-') & (dfnp[:, idx_cnt] == 1)) &
            (dfnp[:, columnas.index('TIPO_CASILLA')] != 'S')][:, columnas.index('TOTAL_VOTOS_CALCULADOS')].sum()

        header['PORCENTAJE_PARTICIPACION_CIUDADANA'] = (
            flt4(header['TOTAL_VOTOS_S_CS'] * 100 / header['LISTA_NOMINAL_ACTAS_CONTABILIZADAS']) if
            (header['TOTAL_VOTOS_S_CS'] and
             header['LISTA_NOMINAL_ACTAS_CONTABILIZADAS']) else flt4(0)
        ) if not self.params['ultimo_corte'] else (
            flt4(header['TOTAL_VOTOS_C_CS'] * 100 / header['LISTA_NOMINAL_ACTAS_CONTABILIZADAS']) if
            (header['TOTAL_VOTOS_C_CS'] and
             header['LISTA_NOMINAL_ACTAS_CONTABILIZADAS']) else flt4(0)
        )

        header['ACTAS_ESPERADAS'] = self.actas_esperadas

        actas_fuera_de_catalogo_anterior = header['ACTAS_FUERA_CATALOGO']

        header['ACTAS_FUERA_CATALOGO'] = self.actas_fuera_catalogo

        actas_capturadas = sum(1 for v in dfnp[:, idx_pub] if v != '-')

        header['ACTAS_REGISTRADAS'] += actas_capturadas + \
            header['ACTAS_FUERA_CATALOGO'] - actas_fuera_de_catalogo_anterior

        header['ACTAS_CAPTURADAS'] += actas_capturadas

        header['PORCENTAJE_ACTAS_CAPTURADAS'] = flt4(
            float(header['ACTAS_CAPTURADAS']) * 100 / header['ACTAS_ESPERADAS']
        )

        header['ACTAS_CONTABILIZADAS'] += sum(v for v in dfnp[:, idx_cnt] if v != '-')

        header['PORCENTAJE_ACTAS_CONTABILIZADAS'] = flt4(
            header['ACTAS_CONTABILIZADAS'] * 100 / header['ACTAS_ESPERADAS']
        )

        header['suma_observaciones'] += sum(1 for v in dfnp[:, columnas.index('OBSERVACIONES')] if isinstance(v, list))

        header['PORCENTAJE_ACTAS_INCONSISTENCIAS'] = flt4(
            header['suma_observaciones'] * 100 / header['ACTAS_ESPERADAS']
        )

    def define_columnas(self, df, columnas_csv_init):
        # NOTE  renombrar columnas
        df.rename(columns=columnas_renombrar, inplace=True)

        columnas_csv_init = [
            self.asociacion_nombre(x) if x in columnas_csv_init and compile_asociacion(x) else x
            for x in columnas_csv_init
        ]

        columnas_csv_init = [columnas_renombrar[x] if x in columnas_renombrar.keys() else x for x in columnas_csv_init]

        # NOTE  ordenar columnas
        df = df[columnas_csv_init[1:]]

        # NOTE  ordena los elementos con respecto al valor de las columnas
        df = df.sort_values(
            by=['ID_ENTIDAD', 'ID_DISTRITO_FEDERAL', 'SECCION', 'TIPO_CASILLA', 'ID_CASILLA', 'EXT_CONTIGUA']
        )
        return df

    def csv_base(self):
        with open(ubicacion(self.params['tmp_csv_base']), 'w', encoding='utf_8_sig') as base:
            csv.writer(base, lineterminator='\r')

    def llenar_csv(self, header):
        with open(ubicacion(self.params['tmp_info_candidaturas']), mode='w', encoding='utf_8_sig') as base:
            base_write = csv.writer(base, lineterminator='\r')
            if not self.params['corte_nayarit']:
                base_write.writerow(["PRESIDENCIA DE LA REPÚBLICA"])
            else:
                base_write.writerow(["Senadurias Federales"])
            base_write.writerow([f"{self.params['fecha_header']} (UTC-6)"])
            base_write.writerow([])
            temp = pd.DataFrame([header])
            base_write.writerow(list(temp))
            base_write.writerows(temp.values.tolist())
            base_write.writerow([])
            with open(ubicacion(self.params['tmp_csv_base']), mode='r', encoding='utf_8_sig', newline='') as info:
                for line in csv.reader(info, lineterminator='\r'):
                    base_write.writerow(line)

    def juntar_informacion(self, df, columnas):
        with open(ubicacion(self.params['tmp_csv_base']), mode='a+', encoding='utf_8_sig') as base:
            base_write = csv.writer(base, lineterminator='\r')
            if columnas:
                base_write.writerow(list(df))
            base_write.writerows(df.values.tolist())

    def archivo_diputaciones(self, datos, descripcion):
        with open(ubicacion(self.params['tmp_archivo_candidatura']), 'w', newline='', encoding='utf_8_sig') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([i[0] for i in descripcion])
            [writer.writerow(i) for i in datos]

    def unir_dataframe(self, estados_distritos):
        actas = self.leer(
            ruta=self.params['ruta_info_corte'],
            nombre=self.params['nombre_archivo_corte'] % (estados_distritos['ID_ESTADO'], estados_distritos['ID_DISTRITO'])
        )

        '''
        actas_rp = self.leer(
            ruta=self.params['ruta_info_corte'],
            nombre=self.params['nombre_archivo_corte_rp'] % (estados_distritos[0], estados_distritos[1])
        )
        '''
        actas_rp = False
        
        actaspd = pd.DataFrame()
        if not isinstance(actas, bool):
            actaspd = pd.DataFrame(actas).set_index('ID_ACTA').drop(columns=datos_no_empleados, errors='ignore')
            actaspd = pd.concat([pd.DataFrame(columns=self.asociaciones), actaspd])
        if not isinstance(actas_rp, bool):
            actaspd_rp = pd.DataFrame(actas_rp).set_index('ID_ACTA').drop(columns=datos_no_empleados, errors='ignore')
            actaspd = pd.concat([actaspd, actaspd_rp])
    
        return actaspd.fillna('-')

        

    def crea_csv_diputaciones(self):
        datos, descripcion = self.repositorio.obtiene_candidaturas()
        self.archivo_diputaciones(datos, descripcion)
        
        
    def obtiene_informacion(self, estados_distritos, acumulador):
        df = self.unir_dataframe(estados_distritos)
        df = self.parsear(df)
        acumulador.append(df)

    def agrega_circuns_csv(self):
        pares_restantes = self.pares_estado_distrito.copy()
        nuevas_columnas = True
        header = self.header.copy()
        while pares_restantes:
            df = pd.DataFrame()
            acumulador = self.manager.list()
            hilos = []
            for estados_distritos in pares_restantes[:60]:
                h = multiprocessing.Process(target=self.obtiene_informacion, args=(estados_distritos, acumulador))
                h.start()
                hilos.append(h)
            for h in hilos:
                h.join()
            df = pd.concat([df, *list(acumulador)])
            pares_restantes = pares_restantes[60:]
            self.obtener_parte_superior(df, header)
            df = self.transformar(df)
            df = self.define_columnas(df, self.columnas_csv_init)
            df = self.tranforma_extranjero(df)
            df = self.transformar_anticipado(df)
            self.juntar_informacion(df, nuevas_columnas)
            nuevas_columnas = False
        del header['suma_observaciones']
        return header

    def agrega_actas_fuera_catalogo(self):
        all_columnas = [*self.columnas_csv_init, *list(self.info_actas_fuera_catalogo.keys())]
        
        unique_columns = list(set(all_columnas)) 

        df = pd.DataFrame(self.info_actas_fuera_catalogo, columns=unique_columns).fillna('-')
        '''df = pd.DataFrame(
            self.info_actas_fuera_catalogo,
            columns=set([*self.columnas_csv_init, *list(self.info_actas_fuera_catalogo.keys())])
        ).fillna('-')'''
        df = self.transformar_fuera_catalogo(df)
        df = self.define_columnas(df, self.columnas_csv_init)
        self.juntar_informacion(df, columnas=False)

    def agrega_actas_voto_extranjero(self):
        df = pd.DataFrame(
            self.info_acta_voto_extranjero,
            columns= set([*self.columnas_csv_init, *list(self.info_acta_voto_extranjero.keys())])
        ).fillna('-')
        df = self.transformar_voto_extranjero(df)
        df= self.define_columnas(df, self.columnas_csv_init)
        self.juntar_informacion(df, columnas=False)
        

    def crea_csv_votos(self):
        self.csv_base()
        header = self.agrega_circuns_csv()
        #self.agrega_actas_voto_extranjero()
        self.agrega_actas_fuera_catalogo()
        self.llenar_csv(header)

    def borra_tmp(self):
        # os.remove(ubicacion(self.params['tmp_archivo_candidatura']))
        os.remove(ubicacion(self.params['tmp_info_candidaturas']))
        os.remove(ubicacion(self.params['tmp_csv_base']))

    def crear_csv(self):

        # self.crea_csv_diputaciones()

        self.crea_csv_votos()

        self.subir_archivo(
            ubicacion(self.params['tmp_info_candidaturas']), self.params['ruta_bitacora_csv'],
            self.params['archivo_bitacora_info_candidatura']
        )

        '''
        self.subir_archivo(
            ubicacion(self.params['tmp_archivo_candidatura']), self.params['ruta_bitacora_csv'],
            self.params['archivo_bitacora_candidaturas']
        )
        '''

        self.borra_tmp()
