import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime
from models.driver import Ubication, StatusVehicle, StatusScanner, Planning, ReturnInvoice, AuthorizationOrder, InfoScanner
from datetime import datetime, timedelta
from models.driver import User, Access, Company, Polygon,PositionPolygon
import pytz
import psycopg2
from psycopg2 import Error

class BdCodeBarConnect:

    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        #conexion nevada con datos de prueba en local
        host = "vmnevada"
        port = 5434  # Puerto por defecto de MySQL
        database = 'bd_codebar'
        username = 'admin'
        password = "14nc4r1n4*"
        
        #conexion bd de proveedor
        #host = "192.168.1.118"
        #port = 3306  # Puerto por defecto de MySQL
        #database = 'control_acceso'
        #username = 'pi'
        #password = "raspberry"

        try:
            # Establece la conexión MYSQL
            #self.connection = mysql.connector.connect(
            #    host=host,
            #    port=port,
            #    database=database,
            #    user=username,
            #    password=password
            #)

            # Establece la conexión PostgreSQL
            self.connection = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=username,
                password=password
            )
            #Verificacion de Conexion Mysql
            #if self.connection.is_connected():
            #Verificacion de Conexion Postgresql
            if self.connection:
                self.cursor = self.connection.cursor()
                print("Conexión exitosa a la BD")
            
        except Error as e:
            print("Error al conectar a la base de datos:", e)

    async def getScanners(self):
        id_status = 21
        try:
            consulta = f'''select ac.id_acceso, ac.codigo, ac.fecha_hora, ac.ticket_entrada,
            ac.tipo, ac.turno, ac.id_equipo, ac.foto, ac.id_estatus, ac.id_tracking, eq.nombre, eq.compania, co.detalles
            from acceso as ac
            join equipos as eq ON eq.id_equipo = ac.id_equipo 
            join codigo as co on co.codigo = ac.codigo
            where ac.id_estatus={id_status} order by ac.fecha_hora'''
            print(consulta)
            self.cursor.execute(consulta)
            
            result = self.cursor.fetchall()  # Aquí consumes todos los resultados

            response = []
            if result is not None:
                for resultado in result:
                    ubication = InfoScanner(
                        id_acceso=str(resultado[0]),
                        codigo=resultado[1],
                        fecha_hora=str(resultado[2]),
                        entrada_scanner=resultado[3],
                        tipo=resultado[4],
                        turno=str(resultado[5]), 
                        id_equipo=str(resultado[6]),
                        foto=resultado[7],
                        id_estatus=str(resultado[8]),
                        id_tracking=str(resultado[9]),
                        nombre_scanner=resultado[10],
                        compania=resultado[11],
                        detalle_guia=resultado[12],
                    )
                    
                    response.append(ubication)
                    print(response)

                return response
            else:
                return None
        except Error as e:
            print("Error al ejecutar la consulta:", e)
            return e

    
    def syncCodeBar(self, code: str):
        print('SQL CODEBAR')
        try:       
            consulta = f"UPDATE acceso SET id_estatus=1 where acceso.ticket_entrada ='{code}'"
            self.cursor.execute(consulta,)
            self.connection.commit()
            print(consulta)
        except Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def closeConnection(self):
        # Cierra el cursor y la conexión
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

