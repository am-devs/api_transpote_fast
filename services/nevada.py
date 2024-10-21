import psycopg2
import json
from typing import Optional
from datetime import datetime
from models.driver import Ubication, StatusVehicle, StatusScanner, Planning, ReturnInvoice, AuthorizationOrder
from datetime import datetime, timedelta
from models.driver import User, Access, Company, Polygon,PositionPolygon
from models.driver import InfoScanner
import pytz

class NevadaConnect:

    def __init__(self):
        self.connection = None
        self.cursor = None
        

    def connect(self):
        host = "vmnevada"
        port = 5434
        database ='transporte'
        username ='admin'
        password = "14nc4r1n4*"

        conn_string = f"dbname={database} user={username} password={password} host={host} port={port}"

        try:
            # Establece la conexión
            self.connection = psycopg2.connect(conn_string)

            # Crea un cursor
            self.cursor = self.connection.cursor()

        except psycopg2.Error as e:
            print("Error al conectar a PostgreSQL:", e)

    def post_ubi(self, ubi: Ubication):
        try:
            # Ejecuta la consulta de prueba
            fecha = datetime.now()
            formato_fecha = fecha.strftime("%d-%m-%Y, %H:%M:%S")
            consulta = "INSERT INTO ubication (cod_socio, nombre_socio, placa, latitud, longitud, fecha) VALUES (%s, %s, %s, %s, %s, %s)"
            self.cursor.execute(consulta, (ubi.cod_socio, ubi.nombre_socio, ubi.placa, ubi.latitud, ubi.longitud, formato_fecha))
            self.connection.commit()
            consulta = "SELECT * FROM last_ubication lb WHERE lb.placa=%s;"
            self.cursor.execute(consulta, (ubi.placa,))
            self.connection.commit()
            result = self.cursor.fetchone()
            if result is not None:
                consulta = "UPDATE last_ubication SET cod_socio= %s ,nombre_socio= %s, latitud = %s, longitud = %s, fecha=%s WHERE last_ubication.placa = %s"
                self.cursor.execute(consulta, (ubi.cod_socio, ubi.nombre_socio ,ubi.latitud, ubi.longitud, formato_fecha, ubi.placa,))
                self.connection.commit()
                return 1
            else:
                consulta = "INSERT INTO last_ubication (cod_socio, nombre_socio, placa, latitud, longitud, fecha) VALUES (%s, %s, %s, %s, %s, %s)"
                self.cursor.execute(consulta, (ubi.cod_socio, ubi.nombre_socio, ubi.placa, ubi.latitud, ubi.longitud, formato_fecha))
                self.connection.commit()
                return 2    
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def getLocations(self):
        try:
            consulta = "select cod_socio, placa, latitud, longitud from ubication"
            self.cursor.execute(consulta)
            self.connection.commit()
            result = self.cursor.fetchall()
            response = []
            if result is not None:
                for resultado in result:
                    ubication = Ubication(
                        cod_socio=resultado[0],
                        placa=resultado[1],
                        latitud=str(resultado[2]),
                        longitud=str(resultado[3])
                    )
                    response.append(ubication)

                return response
            else:
                return None
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def getLastLocations(self):
        try:
            consulta = "select cod_socio, nombre_socio, placa, latitud, longitud from last_ubication"
            consulta = "SELECT la.cod_socio, la.nombre_socio, la.placa, la.latitud, la.longitud FROM last_ubication AS la WHERE TO_TIMESTAMP(la.fecha, 'DD-MM-YYYY') = CURRENT_DATE"
            self.cursor.execute(consulta,)
            self.connection.commit()
            result = self.cursor.fetchall()   
            response = []  
            if result is not None:       
                for resultado in result:
                    ubication = Ubication(
                        cod_socio=resultado[0],
                        nombre_socio=resultado[1],
                        placa=resultado[2],
                        latitud=str(resultado[3]),
                        longitud=str(resultado[4])
                    )
                    response.append(ubication)

                return response
            else:
                return None
            
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e


    def getLocationSpecific(self, placa:str):
        try:
            consulta = "select cod_socio, placa, latitud, longitud from last_ubication where last_ubication.placa = %s"
            self.cursor.execute(consulta, (placa,))
            self.connection.commit()
            result = self.cursor.fetchone()     
            if result is not None:       
                for resultado in result:
                    ubication = Ubication(
                        cod_socio=resultado[0],
                        placa=resultado[1],
                        latitud=str(resultado[2]),
                        longitud=str(resultado[3])
                    )
                    return ubication
            else:
                return None
            
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def getAllTickets(self):
         try:
            consulta = "select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, estatusvehicle, status_parada, date_scanner from ticket_stop"
            self.cursor.execute(consulta,)
            result = self.cursor.fetchall() 
            response = []  
            if result is not None:
                for res in result:
                  status_vehicle = StatusVehicle(
                    placa=res[0],
                    parada=res[1],
                    ticket_entrada=res[2],
                    fecha_ticket=res[3],
                    orden_carga=res[4],
                    tipo_viaje=res[5],
                    estatus_vehicle=res[6],
                    status_parada=res[7],
                    dateScanner=res[8]
                    )
                  response.append(status_vehicle)
                
                return response
            else:
                return None

         except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def getAllTicketsAdmin(self, company_id:str, permission_user:int):
         consulta =''
         if permission_user == 1:
             consulta = f'''select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, 
                            estatusvehicle, status_parada, date_scanner, tp_point_control_id, guia_insai, turno, detalle_guia, nombre_productor, is_raw_material
                            from ticket_stop 
                            where company_id={company_id}'''
         
         if permission_user == 2:
             consulta = f'''select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, 
                            estatusvehicle, status_parada, date_scanner, tp_point_control_id, guia_insai, turno, detalle_guia, nombre_productor, is_raw_material
                            from ticket_stop 
                            where company_id={company_id} and is_raw_material=TRUE'''
         
         if permission_user == 3:
             consulta = f'''select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, 
                            estatusvehicle, status_parada, date_scanner, tp_point_control_id, guia_insai, turno, detalle_guia, nombre_productor, is_raw_material
                            from ticket_stop 
                            where company_id={company_id} and is_raw_material=FALSE'''
         
         try:
            self.cursor.execute(consulta,)
            result = self.cursor.fetchall() 
            response = []  
            if result is not None:
                for res in result:
                  status_vehicle = StatusVehicle(
                    placa=res[0],
                    parada=res[1],
                    ticket_entrada=res[2],
                    fecha_ticket=res[3],
                    orden_carga=res[4],
                    tipo_viaje=res[5],
                    estatus_vehicle=res[6],
                    status_parada=res[7],
                    dateScanner=res[8],
                    tp_point_control_id=str(res[9]),
                    guia_insai=res[10],
                    turno=res[11],
                    detalle_guia=res[12],
                    nombre_productor=res[13],
                    es_materia_prima=res[14]
                    )
                  response.append(status_vehicle)
                
                return response
            else:
                return None

         except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e

    def getTicketsTransit(self):
         try:
            consulta = "select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, estatusvehicle, status_parada, date_scanner from ticket_stop where status_parada='transito' "
            self.cursor.execute(consulta,)
            result = self.cursor.fetchall() 
            response = []  
            if result is not None:
                for res in result:
                  status_vehicle = StatusVehicle(
                    placa=res[0],
                    parada=res[1],
                    ticket_entrada=res[2],
                    fecha_ticket=res[3],
                    orden_carga=res[4],
                    tipo_viaje=res[5],
                    estatus_vehicle=res[6],
                    status_parada=res[7],
                    dateScanner=res[8]
                    )
                  response.append(status_vehicle)
                
                return response
            else:
                return None

         except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def getTicketInsai(self, insai_code:str):
        try:
            consulta = "select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, estatusvehicle, status_parada from ticket_stop where ticket_stop.guia_insai = %s order by date_scanner ASC"
            self.cursor.execute(consulta, (insai_code,))
            result = self.cursor.fetchone() 
            print('TESTTT CONSULTAAA')
            print(result)
            if result is not None:
                return StatusVehicle(
                    placa=result[0],
                    parada=result[1],
                    ticket_entrada=result[2],
                    fecha_ticket=result[3],
                    orden_carga=result[4],
                    tipo_viaje=result[5],
                    estatus_vehicle=result[6],
                    status_parada=result[7]
                    )
            else:
                return None

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e

    

    def getTicket(self, ticket:str):
        try:
            consulta = "select placa, parada, ticket_entrada,  fecha_ticket, orde_carga, tipo_de_viaje, estatusvehicle, status_parada from ticket_stop where ticket_stop.ticket_entrada = %s order by date_scanner ASC"
            self.cursor.execute(consulta, (ticket,))
            result = self.cursor.fetchone() 
            print('TESTTT CONSULTAAA')
            print(result)
            if result is not None:
                return StatusVehicle(
                    placa=result[0],
                    parada=result[1],
                    ticket_entrada=result[2],
                    fecha_ticket=result[3],
                    orden_carga=result[4],
                    tipo_viaje=result[5],
                    estatus_vehicle=result[6],
                    status_parada=result[7]
                    )
            else:
                return None

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        

    def getTimeTracking(self, ticket: str):
        try:
            consulta = "select status_parada, date_created from tracking_stop where tracking_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (ticket,))
            result = self.cursor.fetchall() 
            count = len(result)
            print(count)
            total_time = timedelta()  # Inicializar el tiempo total como cero
            previous_time = None  # Almacenar el tiempo anterior
            
            if result:
                for res in result:
                    date_str = res[1]  # Obtener la cadena de fecha y hora
                    # Convertir la cadena a objeto datetime
                    date_created = datetime.strptime(date_str, '%d-%m-%Y, %H:%M:%S')

                    if previous_time:
                        # Calcular la diferencia de tiempo entre este registro y el anterior
                        time_difference = date_created - previous_time
                        total_time += time_difference

                    previous_time = date_created  # Actualizar el tiempo anterior

                # Extraer los días, horas, minutos y segundos de la suma total
                total_days = total_time.days
                total_seconds = total_time.seconds
                total_hours = total_days * 24 + total_seconds // 3600
                total_minutes = (total_seconds % 3600) // 60
                total_seconds %= 60

                # Formatear los valores en una cadena 'HH:MM:SS'
                total_time_str = '{:02d}:{:02d}:{:02d}'.format(total_hours, total_minutes, total_seconds)

                return total_time_str
            else:
                return None

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return None


    
    def getTracking(self, ticket:str):
        try:
            consulta = "select status_parada, date_created from tracking_stop where tracking_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (ticket,))
            result = self.cursor.fetchall() 
            count = len(result)
            print(count)
            response = []
            fecha = datetime.now()
            date_format = datetime.now()
            if result is not None:
                if count == 1:
                    for res in result:
                        date_compare=res[1]
                        date_format = datetime.strptime(date_compare, "%d-%m-%Y, %H:%M:%S")
                        time_spent = str(fecha - date_format)
                        status_scanner = StatusScanner(
                            status_scanner=res[0],
                            date_created=res[1],
                            time_spent=time_spent
                            )
                        response.append(status_scanner)
                    return response
                if count > 1:
                    fecha = datetime.now()
                    contfor = 0
                    for res in result:
                        contfor=contfor+1
                        if contfor==1:   
                            date_compare=res[1]
                            date_compare_format = datetime.strptime(date_compare, "%d-%m-%Y, %H:%M:%S")
                            status_scanner = StatusScanner(
                                status_scanner=res[0],
                                date_created=res[1],
                                time_spent=res[1],
                                )
                            response.append(status_scanner)

                        if contfor > 1:
                            contfor=contfor+1
                            res_time = res[1]
                            time_create = datetime.strptime(res_time, "%d-%m-%Y, %H:%M:%S")
                            time_spent = str(time_create - date_compare_format)
                            date_compare_format = time_create
                            status_scanner = StatusScanner(
                                status_scanner=res[0],
                                date_created=res[1],
                                time_spent=time_spent,
                                )
                            response.append(status_scanner)
                    return response
            else:
                return None

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
          
    def firstScanner(self, status_vehicle:StatusVehicle, parada: str):
        fecha = datetime.now()
        formato_fecha = fecha.strftime("%d-%m-%Y, %H:%M:%S")
        try:
            consulta = "INSERT INTO ticket_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga, tipo_de_viaje, estatusvehicle, status_parada, primer_scanner, date_scanner) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(consulta, (status_vehicle.placa, status_vehicle.parada,
                                           status_vehicle.ticket_entrada, status_vehicle.fecha_ticket, status_vehicle.orden_carga,
                                           status_vehicle.tipo_viaje, status_vehicle.estatus_vehicle, parada, parada, formato_fecha))
            self.connection.commit()
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e

    def firstScannerDynamic(self, status_vehicle: Optional[StatusVehicle], info_code: InfoScanner, es_materia_prima: bool):
        fecha = datetime.now()
        formato_fecha = fecha.strftime("%d-%m-%Y, %H:%M:%S")
        if status_vehicle is None:
            try:
                consulta = f'''INSERT INTO ticket_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga,
                tipo_de_viaje, estatusvehicle, status_parada, date_scanner, company_id, guia_insai, is_raw_material,
                tp_point_control_id, turno, detalle_guia, nombre_productor)
                VALUES ('no posee', 'no posee', 'no posee', 'no posee','no posee', 'no posee', 'no posee', 
                '{info_code.nombre_scanner}', '{formato_fecha}', '{info_code.compania}', '{info_code.entrada_scanner}',
                {es_materia_prima}, {int(info_code.id_equipo)}, '{info_code.turno}', '{info_code.detalle_guia}', '{info_code.nombre_productor}')'''
                self.cursor.execute(consulta,)
                self.connection.commit()
                return
            except psycopg2.Error as e:
                print("Error al ejecutar la consulta:", e)
                return e

        try:
            consulta= f'''INSERT INTO ticket_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga,
                tipo_de_viaje, estatusvehicle, status_parada, date_scanner, guia_insai, is_raw_material, tp_point_control_id, company_id, turno, detalle_guia)
                VALUES ('{status_vehicle.placa}', '{status_vehicle.parada}', '{status_vehicle.ticket_entrada}',
                '{status_vehicle.fecha_ticket}','{status_vehicle.orden_carga}', '{status_vehicle.tipo_viaje}',
                '{status_vehicle.estatus_vehicle}', '{info_code.nombre_scanner}', '{formato_fecha}', 'no posee',
                {es_materia_prima}, {int(info_code.id_equipo)}, {info_code.compania}, '{info_code.turno}', '{info_code.detalle_guia}')'''
            self.cursor.execute(consulta,)
            self.connection.commit()
            
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e

    def insertTrakingDynamic(self, status_vehicle: Optional[StatusVehicle], info_code: InfoScanner, ticket_insai: Optional[StatusVehicle]):
        fecha = datetime.now()
        formato_fecha = fecha.strftime("%d-%m-%Y, %H:%M:%S")
        
        if status_vehicle is None and ticket_insai is None:    
            try:
                print('SQL INSERT TRACKING1')
                consulta = f'''INSERT INTO tracking_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga, tipo_viaje,
                estatusvehicle, status_parada, registro_peso, date_created, turno, foto, tipo, id_acceso, guia_insai) 
                VALUES ('no posee', 'no posee', 'no posee', 'no posee','no posee', 'no posee', 'no posee',
                 '{info_code.nombre_scanner}', 'no posee', '{formato_fecha}', '{info_code.turno}', '{info_code.foto}',
                 '{info_code.tipo}', {int(info_code.id_acceso)}, '{info_code.entrada_scanner}') '''
                print(consulta)
                self.cursor.execute(consulta,)
                self.connection.commit()
                
            except psycopg2.Error as e:
                print("Error al ejecutar la consulta:", e)
                return e
            
        if ticket_insai is not None:
            try:
                print('SQL INSERT TRACKING2')
                consulta = f'''
                INSERT INTO tracking_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga, tipo_viaje, estatusvehicle, status_parada, 
                registro_peso, date_created, turno, foto, tipo, id_acceso, guia_insai) VALUES (
                '{ticket_insai.placa}','{ticket_insai.parada}','{ticket_insai.ticket_entrada}','{ticket_insai.fecha_ticket}',
                '{ticket_insai.orden_carga}','{ticket_insai.tipo_viaje}','{ticket_insai.estatus_vehicle}','{info_code.nombre_scanner}',
                '{ticket_insai.registro_peso}','{str(formato_fecha)}','{info_code.turno}','{info_code.foto}','{info_code.tipo}',
                {int(info_code.id_acceso)}, '{info_code.entrada_scanner}') '''
                print(consulta)
                self.cursor.execute(consulta,)
                self.connection.commit()
            except psycopg2.Error as e:
                print("Error al ejecutar la consulta:", e)
                return e
            
        
        if status_vehicle is not None:
            try:
                print('SQL INSERT TRACKING3')
                consulta = f'''
                INSERT INTO tracking_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga, tipo_viaje, estatusvehicle,
                status_parada, registro_peso, date_created, turno, foto, tipo, id_acceso, guia_insai)
                VALUES ('{status_vehicle.placa}', '{status_vehicle.parada}', '{status_vehicle.ticket_entrada}','{status_vehicle.fecha_ticket}',
                '{status_vehicle.orden_carga}', '{status_vehicle.tipo_viaje}', '{status_vehicle.estatus_vehicle}', '{info_code.nombre_scanner}', 
                '{status_vehicle.registro_peso}', '{str(formato_fecha)}', '{info_code.turno}', '{info_code.foto}', '{info_code.tipo}', 
                {int(info_code.id_acceso)}, 'no posee')
                ''' 
                self.cursor.execute(consulta,)
                self.connection.commit()
            except psycopg2.Error as e:
                print("Error al ejecutar la consulta:", e)
                return e

    def addTicketToInsai(self, status_vehicle:StatusVehicle, info_code: InfoScanner):
        try:
            print('SQL ADD TICKET INSAI')
            consulta = f'''UPDATE ticket_stop SET placa='{status_vehicle.placa}', parada='{status_vehicle.placa}',
            ticket_entrada='{status_vehicle.ticket_entrada}', fecha_ticket='{status_vehicle.fecha_ticket}',
            orde_carga='{status_vehicle.orden_carga}', tipo_de_viaje='{status_vehicle.tipo_viaje}', estatusvehicle='{status_vehicle.estatus_vehicle}',
            status_parada='{info_code.nombre_scanner}', tp_point_control_id={info_code.id_equipo}
            where ticket_stop.guia_insai='{info_code.entrada_scanner}'
            '''
            print(consulta)
            self.cursor.execute(consulta,)
            self.connection.commit()
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e


    def insertTraking(self, status_vehicle:StatusVehicle, parada: str):
        fecha = datetime.now()
        formato_fecha = fecha.strftime("%d-%m-%Y %H:%M:%S")

        try:
            consulta = "INSERT INTO tracking_stop (placa, parada, ticket_entrada, fecha_ticket, orde_carga, tipo_viaje, estatusvehicle, status_parada, registro_peso, date_created) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(consulta, (status_vehicle.placa, status_vehicle.parada,
                                           status_vehicle.ticket_entrada, status_vehicle.fecha_ticket, status_vehicle.orden_carga,
                                           status_vehicle.tipo_viaje, status_vehicle.estatus_vehicle, parada, status_vehicle.registro_peso, formato_fecha))
            self.connection.commit()
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def finishDocument(self, code:str):
        try:
            print('SQL FINISH DOCUMENT')
            consulta = f"DELETE FROM ticket_stop where ticket_stop.ticket_entrada ='{code}' or ticket_stop.guia_insai='{code}'"
            self.cursor.execute(consulta,)
            self.connection.commit()
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e


    def deleteTicket(self, status_vehicle:StatusVehicle):
        try:
            consulta = "DELETE FROM ticket_stop WHERE ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()
        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e
    
    def completeTicket(self, status_vehicle:StatusVehicle):
        try:        
            consulta = "UPDATE ticket_stop SET status_parada='completado' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def stopvigilance(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='vigilancia' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def stopvigilance2(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='vigilancia2' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def stopromana(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='romana' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def updateTicket(self, code: str, status_parada:str, tp_point_control:int):
        try:
            print('SQL UPDATE')      
            consulta = f"UPDATE ticket_stop SET status_parada='{status_parada}', tp_point_control_id={tp_point_control} where ticket_stop.ticket_entrada ='{code}' or ticket_stop.guia_insai='{code}'"
            self.cursor.execute(consulta,)
            self.connection.commit()
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e

    def stoptransport(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='taller' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def stopyard(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='patio' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def stop1(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='p1' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def stop2(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='p2' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def stop3(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='p3' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def stop4(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='p4' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
        
    def stop5(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='p5' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def completeRecordWeight(self, status_vehicle: StatusVehicle):
        try:       
            print(status_vehicle.ticket_entrada)
            consulta = "UPDATE ticket_stop SET status_parada='transito' where ticket_stop.ticket_entrada = %s"
            self.cursor.execute(consulta, (status_vehicle.ticket_entrada,))
            self.connection.commit()

        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    def createReturn(self, return_invoice: ReturnInvoice):
        fecha = datetime.now()
        formato_fecha = fecha.strftime("%d-%m-%Y, %H:%M:%S")
        try:       
            consulta = (f"INSERT INTO return_invoice (invoice_id, business_partner_id, product_id, reason, quantity, date_creation) VALUES ('{return_invoice.invoiceID}','{return_invoice.businessPartnerId}','{return_invoice.product_id}','{return_invoice.reason}','{return_invoice.quantity}','{formato_fecha}')")
            self.cursor.execute(consulta)
            self.connection.commit()
            return 1
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e
    
    async def getAuthorizations(self, chofer_id:str):
        try:
            #te falto el nombre del producto, tienes que traertelo en todos lados.
            consulta = f"select order_id, documentno, name_item, business_partner, product_id, quantity, reason, status, date_create from authorization_order where chofer_id={chofer_id}"
            self.cursor.execute(consulta)
            self.connection.commit()
            result = self.cursor.fetchall()
            response = []
            print(result)
            if result is not None:
                for resultado in result:
                    aut_order = AuthorizationOrder(
                        order_id=str(resultado[0]),
                        documentoNo=resultado[1],
                        name_item=resultado[2],
                        businessPartner=str(resultado[3]),   
                        product_id=str(resultado[4]), 
                        quantity=resultado[5],
                        reason=resultado[6],
                        status=resultado[7],
                        date_create=resultado[8],
                        
                    )
                    response.append(aut_order)
                return response
            else:
                return None
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return e

    async def insertAuthorization(self, request:AuthorizationOrder):
        venezuela_tz = pytz.timezone('America/Caracas')
        # Obtener la fecha y hora actual en la zona horaria de Venezuela
        fecha = datetime.now(venezuela_tz)
        # Formatear la fecha y hora
        formato_fecha = fecha.strftime("%d-%m-%Y, %H:%M:%S")
        status ='en espera'
        try:
            consulta = "INSERT INTO authorization_order (order_id, documentno, business_partner, product_id, quantity, reason, status, date_create, name_item, chofer_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(consulta, (request.order_id, request.documentoNo,
                                           request.businessPartner, request.product_id, request.quantity,
                                           request.reason, status, formato_fecha, request.name_item, request.chofer_id))
            self.connection.commit()
            return 'hecho'
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None
    
    async def updateAuthorization(self, order_id:str, response:str, product_id:str):
        try:       
            consulta = f"UPDATE authorization_order SET status='{response}' where authorization_order.order_id = {order_id} and authorization_order.product_id={product_id}"
            self.cursor.execute(consulta,)
            self.connection.commit()
            return 'hecho'
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None
    
    async def loginAdmin(self, username:str, password:str):
        try:
            consulta = f"SELECT tp_user_id ,username, password from tp_user where username='{username}' and password='{password}'"
            self.cursor.execute(consulta)
            self.connection.commit()
            result = self.cursor.fetchall()
            response = []
            if result is not None:
                for resultado in result:
                    user = User(
                        username=resultado[1],
                        id=str(resultado[0]),
                        password=resultado[2],
                    )
                    response.append(user)
                
                if len(response) == 0:
                    return None
                
                return response
           
            else:
                return None
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None    
    
    async def getCompanies(self, user_id:str):
        try:
            consulta = f'''SELECT tc.company_id, tc.name, tc.centerlat, tc.centerlong FROM tp_company tc
            JOIN tp_company_user tcu ON tcu.tp_company_id = tc.company_id
            JOIN tp_user tu ON tu.tp_user_id = tcu.tp_user_id
            WHERE tu.tp_user_id ={user_id}'''
            self.cursor.execute(consulta)
            self.connection.commit()
            result = self.cursor.fetchall()
            response = []
            if result is not None:
                for resultado in result:
                    user = Company(
                        company_id=str(resultado[0]),
                        name=resultado[1],
                        latitud=str(resultado[2]),
                        longitud=str(resultado[3]),
                    )
                    response.append(user)
                
                if len(response) == 0:
                    return None
                
                return response
           
            else:
                return None
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None


    async def accessAdmin(self, user_id:str, company_id:str):
        try:
            consulta = f'''select tp.name, tp.is_raw_material, tp.is_terminate from tp_permission as tp 
            JOIN tp_access tpa ON tpa.tp_permission_id = tp.tp_permission_id 
            JOIN tp_user tpu ON tpu.tp_user_id = tpa.tp_user_id WHERE tpu.tp_user_id={user_id} and tp.company_id={company_id}'''
            self.cursor.execute(consulta)
            self.connection.commit()
            result = self.cursor.fetchone()
            if result is None:
                return None
        
            if len(result) == 0:
                return None

            if result is not None:
                user = Access(
                    name=result[0],
                    is_raw_material=str(result[1]),
                    is_terminate=str(result[2]),
                )
               
                if user.is_raw_material == 'True' and user.is_terminate == 'True':
                    return 1
                if user.is_raw_material == 'True' and user.is_terminate == 'False':
                    return 2
                if user.is_raw_material == 'False' and user.is_terminate == 'True':
                    return 3
                    
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None
    
    async def getPolygons(self, company_id:str):
        try:
            #consulta = f'''
            #select name, latitud1, longitud1, latitud2, longitud2, latitud3, longitud3, latitud4, longitud4,
            #latitud5, longitud5, tp_point_control_id
            #from tp_point_control
            #where company_id={company_id}'''
            consulta = f'''
            SELECT 
            tc.name, 
            tc.latitud1, 
            tc.longitud1, 
            tc.latitud2, 
            tc.longitud2, 
            tc.latitud3, 
            tc.longitud3, 
            tc.latitud4, 
            tc.longitud4, 
            tc.latitud5, 
            tc.longitud5, 
            tc.tp_point_control_id, 
            COUNT(ts.tp_point_control_id) AS ticket_stop_count
            FROM tp_point_control AS tc
            LEFT JOIN ticket_stop ts ON ts.tp_point_control_id = tc.tp_point_control_id
            where tc.company_id={company_id}
            GROUP BY 
            tc.name, 
            tc.latitud1, 
            tc.longitud1, 
            tc.latitud2, 
            tc.longitud2, 
            tc.latitud3, 
            tc.longitud3, 
            tc.latitud4, 
            tc.longitud4, 
            tc.latitud5, 
            tc.longitud5, 
            tc.tp_point_control_id
            '''
            self.cursor.execute(consulta)
            self.connection.commit()
            resultado = self.cursor.fetchall()
            print(resultado)
            polygons = []
            if resultado is None:
                return None
                        
            if len(resultado) == 0:
                return None

            if resultado is not None:
                for result in resultado:
                    polygon = Polygon(
                        name=result[0],
                        latitud1=str(result[1]),
                        longitud1=str(result[2]),
                        latitud2=str(result[3]),
                        longitud2=str(result[4]),
                        latitud3=str(result[5]),
                        longitud3=str(result[6]),
                        latitud4=str(result[7]),
                        longitud4=str(result[8]),
                        latitud5=str(result[9]),
                        longitud5=str(result[10]),
                        tp_point_control_id=str(result[11]),
                        total_vehicles=str(result[12])
                    )
                    polygons.append(polygon)
                return polygons
                    
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None 

    async def getPositions(self, polygon_id:str, length:int):
        try:
            consulta = f'''
            SELECT DISTINCT tp_position_point_id, tp_point_control_id, latitud, longitud
	        FROM public.tp_position_point 
            WHERE tp_point_control_id = {polygon_id}
            ORDER BY tp_position_point_id ASC
            LIMIT {length}'''
            self.cursor.execute(consulta)
            self.connection.commit()
            resultado = self.cursor.fetchall()
            print(resultado)
            positions_polygons = []
            if resultado is None:
                return None
        
            if len(resultado) == 0:
                return None

            if resultado is not None:
                for result in resultado:
                    position_polygon = PositionPolygon(
                        tp_position_point_control_id=str(result[0]),
                        tp_point_control_id=str(result[1]),
                        latitud=str(result[2]),
                        longitud=str(result[3]),
                    )
                    positions_polygons.append(position_polygon)
                return positions_polygons
                    
        except psycopg2.Error as e:
           print("Error al ejecutar la consulta:", e)
           return None 

    def closeConnection(self):
        # Cierra el cursor y la conexión
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

