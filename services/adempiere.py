import datetime
from fastapi import HTTPException, status
import psycopg2
from models.driver import User, Vehicle, StatusVehicle, Planning, Ticket, Invoice,Product, OrderReturn, CreateReturn
from typing import List
import xmltodict
import requests
import re

class AdempiereConnect:
    def __init__(self):CreateReturn
        
    def connect(self):
        #para ver lo de la Prioridad en la pantalla debes usar la base de datos de FC 
        host = "vmtestbd"
        port = 5432
        database = "adempiere_fc"
        #host = "losroques"
        #port = 5434
        #database = "adempiere"
        username = "adempiere"
        password = "adempiere" 
        #password = "ad3mp13r3sf1d4.*"
        conn_string = f"dbname={database} user={username} password={password} host={host} port={port}"

        try:
            # Establece la conexión
            self.connection = psycopg2.connect(conn_string)

            # Crea un cursor
            self.cursor = self.connection.cursor()

        except psycopg2.Error as e:
            print("Error al conectar a PostgreSQL:", e)

    def login(self, ci:str):
        try:
            # Ejecuta la consulta de prueba
            consulta = "SELECT cb.value as codigo_socio, cb.name as nombre_socio FROM C_BPartner cb  where cb.value=%s;"
            self.cursor.execute(consulta, (ci,))
            # Obtiene el resultado
            resultados = self.cursor.fetchall()
            print("Resultados de la consulta:", resultados)
            if not resultados:
                return None
            for resultado in resultados:
                return User(username=resultado[1], code=resultado[0])

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e
    
    def loginDriver(self, ci:str):
        try:
            # Ejecuta la consulta de prueba
            consulta = "select fd.fta_driver_id, fd.name as chofer, fd.value as ci, ad.Name from fta_driver as fd JOIN AD_Client as ad on ad.AD_Client_ID =  fd.AD_Client_ID where fd.value =%s;"
            self.cursor.execute(consulta, (ci,))
            # Obtiene el resultadoresultados
            resultados = self.cursor.fetchall()
            
            if not resultados:
                return None
            for resultado in resultados:
                return User(username=resultado[1], code=resultado[2], id=str(resultado[0],), company=resultado[3])

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e
    
    #Funcion que obtiene ID del ticket por codigo INSAI y obtiene datos del ticket
    def getTicketxInsai(self, code_insai: str):
        try:
            consulta = (f"""SELECT FTA_EntryTicket_ID FROM FTA_EntryTicketGuide WHERE Ext_Guide= '{code_insai}' """)
            self.cursor.execute(consulta)
            resultados = self.cursor.fetchone()
            print(f'RESULTADO TICKET INSAI:{resultados}')
            if resultados is None:
                return None
            
            ticket_id =int(resultados[0])
            print(f'ticket_ID: {ticket_id}')
            ticket = self.getTicketxID(ticket_id)
            print(f'TICKET ADEMPIERE: {ticket}')
            return ticket
            
        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e

    #Funcion que obtiene los datos de un Ticket
    def driverTicket(self, ticket:str):
        try:
            # Ejecuta la consulta de prueba
            consulta =(f"""
                SELECT
                FD.FTA_DRIVER_ID,
                FD.NAME AS CHOFER,
                FD.VALUE AS CI,
                AD.NAME
                FROM
                    FTA_DRIVER AS FD
                    JOIN AD_CLIENT AS AD ON AD.AD_CLIENT_ID = FD.AD_CLIENT_ID
                    JOIN FTA_EntryTicket fe ON fe.FTA_Driver_ID = fd.FTA_Driver_ID
                WHERE
                    fe.DocumentNo ='{ticket}' """)
            self.cursor.execute(consulta)
            # Obtiene el resultadoresultados
            resultados = self.cursor.fetchall()
            if not resultados:
                return None
            for resultado in resultados:
                return User(username=resultado[1], code=resultado[2], id=str(resultado[0],), company=resultado[3])


        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e

    #Funcion que obtiene la placa de un vehiculo
    def queryVehicle(self, plate:str,):
        
        try:
            # Ejecuta la consulta de prueba
            consulta = "SELECT name from FTA_VEHICLE where FTA_VEHICLE.VehiclePlate=%s;"
            self.cursor.execute(consulta, (plate,))
            result = self.cursor.fetchall()
            print("Resultados de la consulta:", result)
            for resultado in result:
                return Vehicle(plate=plate)
        
        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e
    
    #Funcion que obtiene las facturas de un chofer especifico
    def getInvoicesDriver(self, driver_id: str):

        try:
            # Primera consulta para obtener los tickets
            consulta = """
                SELECT FTA_EntryTicket_ID, documentno 
                FROM FTA_EntryTicket as et 
                WHERE et.FTA_Driver_ID = %s 
                AND et.datedoc >= DATE_TRUNC('year', CURRENT_DATE);
            """
            self.cursor.execute(consulta, (driver_id,))
            result = self.cursor.fetchall()
            response = []
            tickets_str = ''
            count = 0

            if result:
                print("Resultados de la consulta de tickets:", result)
                for resultado in result:
                    ticket = Ticket(
                        id=str(resultado[0]),
                        code=resultado[1],
                    )
                    response.append(ticket)

                for item in response:
                    count += 1
                    if count == 1:
                        tickets_str = f"'{item.id}'"
                    else:
                        tickets_str = tickets_str + ', ' + f"'{item.id}'"

                print(f"Cadena de tickets construida: {tickets_str}")

                try:
                    # Segunda consulta para obtener las facturas
                    consulta = f'''
                        SELECT DISTINCT ci.DocumentNo, ci.DateAcct, ci.C_Invoice_ID, ado.name, cb.name, cb.C_BPartner_ID, et.DocumentNo
                        FROM FTA_EntryTicket as et
                        JOIN FTA_LoadOrder as lo 
                        ON et.FTA_Driver_ID = lo.FTA_Driver_ID 
                        AND lo.FTA_EntryTicket_ID = et.FTA_EntryTicket_ID
                        JOIN FTA_LoadOrderLine as loline 
                        ON lo.FTA_LoadOrder_ID = loline.FTA_LoadOrder_ID 
                        JOIN C_OrderLine as coline 
                        ON loline.C_OrderLine_ID = coline.C_OrderLine_ID
                        JOIN C_Order as co ON coline.C_Order_ID = co.C_Order_ID
                        JOIN C_Invoice as ci ON co.C_Order_ID = ci.C_Order_ID
                        JOIN AD_Org ado ON ci.AD_Org_ID = ado.AD_Org_ID
	                    JOIN C_BPartner cb ON ci.C_BPartner_ID = cb.C_BPartner_ID
                        JOIN C_DocType as ctype on ci.C_DocType_ID = ctype.C_DocType_ID
	                    JOIN C_DocBaseType as baset on ctype.C_DocBaseType_ID = baset.C_DocBaseType_ID
                        WHERE et.FTA_Driver_ID = {driver_id} 
                        AND lo.FTA_EntryTicket_ID in ({tickets_str}) 
                        AND ci.docstatus = 'CO'
                        AND (ci.is_confirm !='Y' OR ci.is_confirm is NULL)
                        AND baset.DocBaseType ='ARI'
                    '''
                    print(f"Segunda consulta SQL: {consulta}")

                    self.cursor.execute(consulta)
                    result = self.cursor.fetchall()
                    response = []

                    if result:
                        print("Resultados de las facturas:", result)
                        for resultado in result:
                            date_only = resultado[1].date() 
                            invoice = Invoice(
                                documentoNo=resultado[0],
                                date=str(date_only),
                                invoiceID=str(resultado[2]),
                                org=resultado[3],
                                businessPartner=resultado[4],
                                businessPartnerId=str(resultado[5]),
                                ticket=str(resultado[6]),
                            )
                            response.append(invoice)
                        return response
                    else:
                        print("No se encontraron facturas.")
                        return None

                except psycopg2.Error as e:
                    print("Error al ejecutar la consulta de facturas:", e)
                    return str(e)

            else:
                print("No se encontraron tickets.")
                return None

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de tickets:", e)
            return str(e)
        
    #Funcion que obtiene el detalle de la factura
    def getDetailInvoice(self, invoiceId: str):
        try:
            #Primera consulta para obtener los tickets
            consulta = f"""
            select mp.name, mp.M_Product_ID, cil.qtyinvoiced from c_invoice as ci
            JOIN C_InvoiceLine cil ON ci.C_Invoice_ID = cil.C_Invoice_ID
            JOIN M_Product mp ON cil.M_Product_ID = mp.M_Product_ID
            WHERE ci.C_Invoice_ID={invoiceId}
            """
            self.cursor.execute(consulta)
            result = self.cursor.fetchall()
            response = []
            tickets_str = ''
            if result:
                print("Resultados de la consulta:", result)
                for resultado in result:
                    product = Product(
                        name=resultado[0],
                        product_id=str(resultado[1]),
                        quantity=resultado[2],
                    )
                    response.append(product)
                return response
            else:
                print("No se encontraron Lineas de la Factura.")
                return None

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de tickets:", e)
            return str(e)
    
    #Funcion que obtiene el estatus del vehiculo
    def statusVehicle(self, plate: str):

        credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
        )
        
        consulta =(
            "SELECT DISTINCT ON (vh.vehicleplate) "
                    "vh.vehicleplate AS placa, "
                    "CASE "
                    "WHEN pard.datestop::date = now()::date THEN 'Reparacion'::text "
                    "ELSE 'activo'::text "
                    "END AS parada, "
                    "CASE "
                        "WHEN regp.datedoc < CURRENT_DATE OR repd.datedoc < CURRENT_DATE AND tk.docstatus::text = 'CO'::text THEN 'Sin Ticket'::character varying "
                        "WHEN tk.docstatus::text = 'CO'::text THEN tk.documentno "
                        "WHEN tk.docstatus::text <> 'CO'::text THEN 'Sin Ticket'::character varying "
                        "ELSE 'Sin Ticket'::character varying "
                    "END AS ticket_entrada, "
                    "CASE "
						"WHEN tk.created IS NULL THEN 'Sin Ticket'::text "
						"ELSE to_char(tk.created, 'YYYY-MM-DD HH24:MI:SS') "
					"END AS fecha_ticket, "
                    "CASE "
                        "WHEN tk.fta_entryticket_id = car.fta_entryticket_id AND car.docstatus::text = 'CO'::text AND regp.datedoc < CURRENT_DATE THEN 'Sin Orden Carga'::character varying "
                        "WHEN tk.fta_entryticket_id = car.fta_entryticket_id AND car.docstatus::text = 'CO'::text THEN car.documentno "
                        "ELSE 'Sin Orden Carga'::character varying "
                    "END AS orde_carga, "
                    "CASE "
							"WHEN regp.datedoc < CURRENT_DATE AND car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'sin viaje'::text "
							"WHEN repd.documentno IS NOT NULL AND car.documentno IS NULL AND tk.operationtype = 'MOM'::bpchar AND repd.datedoc = CURRENT_DATE THEN 'acarreo'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype = 'MOM'::bpchar AND car.docstatus::text = 'CO'::text AND regp.datedoc <> CURRENT_DATE THEN 'sin viaje'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype = 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'despacho a sucursal'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text AND regp.datedoc <> CURRENT_DATE THEN 'despacho a cliente'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'despacho a cliente'::text "
							"ELSE 'sin viaje'::text "
						"END AS tipo_de_viaje, "
                    "CASE "
                        "WHEN mov2.m_movementparent_id = mov.m_movement_id AND regp.docstatus::text = 'CO'::text OR repd.docstatus::text = 'CO'::text AND car.docstatus::text = 'CO'::text THEN 'liberado'::text "
                        "WHEN regp.datedoc = CURRENT_DATE AND regp.docstatus::text = 'CO'::text THEN 'transito'::text "
                        "WHEN repd.datedoc = CURRENT_DATE AND repd.docstatus::text = 'CO'::text THEN 'transito'::text "
                        "WHEN req.documentno IS NOT NULL OR car.documentno IS NOT NULL AND car.docstatus::text = 'CO'::text AND regp.documentno IS NULL THEN 'no disponible'::text "
                        "ELSE 'disponible'::text "
                    "END AS estatusvehicle "
            "FROM fta_vehicle vh "
                "LEFT JOIN fta_vehicle_stopline par ON par.fta_vehicle_id = vh.fta_vehicle_id AND par.created::date = now()::date "
                "LEFT JOIN fta_vehicle_stop pard ON pard.fta_vehicle_stop_id = par.fta_vehicle_stop_id "
                "LEFT JOIN m_requisition req ON req.m_requisition_id = pard.m_requisition_id "
                "LEFT JOIN fta_entryticket tk ON tk.fta_vehicle_id = vh.fta_vehicle_id AND tk.datedoc >= CURRENT_DATE AND tk.docstatus::text = 'CO'::text "
                "LEFT JOIN fta_loadorder car ON car.fta_vehicle_id = vh.fta_vehicle_id AND tk.fta_entryticket_id = car.fta_entryticket_id AND car.datedoc >= CURRENT_DATE AND car.docstatus::text = 'CO'::text "
                "LEFT JOIN fta_recordweight regp ON tk.fta_entryticket_id = regp.fta_entryticket_id AND regp.datedoc >= CURRENT_DATE  AND regp.docstatus::text = 'CO'::text "
                "LEFT JOIN dd_recordweight repd ON repd.fta_entryticket_id = tk.fta_entryticket_id AND repd.datedoc >= CURRENT_DATE  AND repd.docstatus::text = 'CO'::text "
                "LEFT JOIN ad_org orgs ON orgs.ad_org_id = repd.ad_org_id OR orgs.ad_org_id = regp.ad_org_id "
                "LEFT JOIN dd_orderline od ON od.dd_orderline_id = repd.dd_orderline_id "
                "LEFT JOIN m_locator loc ON loc.m_warehouse_id = regp.m_warehouse_id OR loc.m_locator_id = od.m_locator_id "
                "LEFT JOIN m_locator loc2 ON loc2.m_locator_id = od.m_locatorto_id "
                "LEFT JOIN m_movement mov ON mov.fta_recordweight_id = regp.fta_recordweight_id OR mov.dd_recordweight_id = repd.dd_recordweight_id AND mov.movementdate = CURRENT_DATE AND mov.docstatus = 'CO'::bpchar "
                "LEFT JOIN m_movement mov2 ON mov2.m_movementparent_id = mov.m_movement_id "
                "LEFT JOIN m_movementline movl ON movl.m_movement_id = mov2.m_movement_id "
                "LEFT JOIN m_locator mloc ON mloc.m_locator_id = movl.m_locator_id "
                "LEFT JOIN ad_org orgd ON orgd.ad_org_id = mov2.ad_org_id "
                "LEFT JOIN c_doctype doct ON doct.c_doctype_id = mov.c_doctype_id "
            "WHERE vh.isowner = 'Y'::bpchar and vh.vehicleplate=%s "
            "ORDER BY vh.vehicleplate, fecha_ticket DESC;")
        try:
            self.cursor.execute(consulta, (plate,))
            result = self.cursor.fetchone()
            print("Resultados de la consulta:", result)
            if result is None:
                print('el resultado de la consulta es None')
                return None
            if result is not None:
                
                print('El resultado de la consulta fueeeeeeee: '+ result[1])
                return StatusVehicle(
                            placa=result[0], 
                            parada=result[1], 
                            ticket_entrada=result[2],
                            fecha_ticket=result[3],
                            orden_carga=result[4],
                            tipo_viaje=result[5],
                            estatus_vehicle=result[6],
                            )
            else:
                return None
        except psycopg2.Error as e:
                    print("Error al ejecutar la consulta:", e)
                    return e

    #Funcion que obtiene datos del ticket a partir de su ID:
    def getTicketxID(self, ticket_id: int):        
        consulta=(
            f'''
            SELECT DISTINCT ON (vh.vehicleplate)
                   vh.vehicleplate AS placa,
                   CASE
                   WHEN pard.datestop::date = now()::date THEN 'Reparacion'::text
                   ELSE 'activo'::text
                   END AS parada,
                   CASE
                       WHEN tk.docstatus::text = 'CO'::text THEN tk.documentno
                       WHEN tk.docstatus::text <> 'CO'::text THEN 'Sin Ticket'::character varying
                       ELSE 'Sin Ticket'::character varying
                   END AS ticket_entrada,
                   CASE
						WHEN tk.created IS NULL THEN 'Sin Ticket'::text
						ELSE to_char(tk.created, 'YYYY-MM-DD HH24:MI:SS')
					END AS fecha_ticket,
                   CASE
                       WHEN tk.fta_entryticket_id = car.fta_entryticket_id AND car.docstatus::text = 'CO'::text THEN car.documentno
                       ELSE 'Sin Orden Carga'::character varying
                   END AS orde_carga,
                   CASE
							WHEN regp.datedoc < CURRENT_DATE AND car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'sin viaje'::text
							WHEN repd.documentno IS NOT NULL AND car.documentno IS NULL AND tk.operationtype = 'MOM'::bpchar AND repd.datedoc = CURRENT_DATE THEN 'acarreo'::text
							WHEN car.documentno IS NOT NULL AND tk.operationtype = 'MOM'::bpchar AND car.docstatus::text = 'CO'::text AND regp.datedoc <> CURRENT_DATE THEN 'sin viaje'::text
							WHEN car.documentno IS NOT NULL AND tk.operationtype = 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'despacho a sucursal'::text
							WHEN car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text AND regp.datedoc <> CURRENT_DATE THEN 'despacho a cliente'::text
							WHEN car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'despacho a cliente'::text
							ELSE 'sin viaje'::text
						END AS tipo_de_viaje,
                   CASE
                       WHEN mov2.m_movementparent_id = mov.m_movement_id AND regp.docstatus::text = 'CO'::text OR repd.docstatus::text = 'CO'::text AND car.docstatus::text = 'CO'::text THEN 'liberado'::text
                       WHEN regp.datedoc = CURRENT_DATE AND regp.docstatus::text = 'CO'::text THEN 'transito'::text
                       WHEN repd.datedoc = CURRENT_DATE AND repd.docstatus::text = 'CO'::text THEN 'transito'::text
                       WHEN req.documentno IS NOT NULL OR car.documentno IS NOT NULL AND car.docstatus::text = 'CO'::text AND regp.documentno IS NULL THEN 'no disponible'::text
                       ELSE 'disponible'::text
                   END AS estatusvehicle,
                   CASE
					WHEN regp.docstatus::text = 'CO'::text AND regp.datedoc::date = now()::date THEN regp.documentno
					WHEN repd.docstatus::text = 'CO'::text AND repd.datedoc::date = now()::date THEN repd.documentno
					ELSE 'Sin Peso'::character varying
					END AS registro_peso,
                   vh.isowner as esPropio
            FROM fta_vehicle vh
               LEFT JOIN fta_vehicle_stopline par ON par.fta_vehicle_id = vh.fta_vehicle_id AND par.created::date = now()::date
               LEFT JOIN fta_vehicle_stop pard ON pard.fta_vehicle_stop_id = par.fta_vehicle_stop_id
               LEFT JOIN m_requisition req ON req.m_requisition_id = pard.m_requisition_id
               LEFT JOIN fta_entryticket tk ON tk.fta_vehicle_id = vh.fta_vehicle_id AND tk.docstatus::text = 'CO'::text
               LEFT JOIN fta_loadorder car ON car.fta_vehicle_id = vh.fta_vehicle_id AND tk.fta_entryticket_id = car.fta_entryticket_id  AND car.docstatus::text = 'CO'::text
               LEFT JOIN fta_recordweight regp ON tk.fta_entryticket_id = regp.fta_entryticket_id AND regp.docstatus::text = 'CO'::text
               LEFT JOIN dd_recordweight repd ON repd.fta_entryticket_id = tk.fta_entryticket_id  AND repd.docstatus::text = 'CO'::text
               LEFT JOIN ad_org orgs ON orgs.ad_org_id = repd.ad_org_id OR orgs.ad_org_id = regp.ad_org_id
               LEFT JOIN dd_orderline od ON od.dd_orderline_id = repd.dd_orderline_id
               LEFT JOIN m_locator loc ON loc.m_warehouse_id = regp.m_warehouse_id OR loc.m_locator_id = od.m_locator_id
               LEFT JOIN m_locator loc2 ON loc2.m_locator_id = od.m_locatorto_id
               LEFT JOIN m_movement mov ON mov.fta_recordweight_id = regp.fta_recordweight_id OR mov.dd_recordweight_id = repd.dd_recordweight_id AND mov.movementdate = CURRENT_DATE AND mov.docstatus = 'CO'::bpchar
               LEFT JOIN m_movement mov2 ON mov2.m_movementparent_id = mov.m_movement_id
               LEFT JOIN m_movementline movl ON movl.m_movement_id = mov2.m_movement_id
               LEFT JOIN m_locator mloc ON mloc.m_locator_id = movl.m_locator_id
               LEFT JOIN ad_org orgd ON orgd.ad_org_id = mov2.ad_org_id
               LEFT JOIN c_doctype doct ON doct.c_doctype_id = mov.c_doctype_id
               WHERE tk.FTA_EntryTicket_ID={ticket_id}
               ORDER BY vh.vehicleplate, fecha_ticket DESC; ''')
        try:   
            # Ejecuta la consulta de prueba
            self.cursor.execute(consulta,)
            print(f'CONSULTA TICKET INSAI: {consulta}')
            result = self.cursor.fetchone()
            print("Resultados de la consulta:", result)
            if result is None:
                print('el resultado de la consulta es None')
                return None
            if result is not None:
                print('El resultado de la consulta fueeeeeeee: '+ result[1])
                return StatusVehicle(
                            placa=result[0], 
                            parada=result[1], 
                            ticket_entrada=result[2],
                            fecha_ticket=result[3],
                            orden_carga=result[4],
                            tipo_viaje=result[5],
                            estatus_vehicle=result[6],
                            registro_peso=result[7],
                            esPropio=result[8],
                            )
            else:
                return None
                
        except psycopg2.Error as e:        
            print("Error al ejecutar la consulta:", e)
            return e 



    #Funcion que obtiene datos a partir del Ticket
    def getTicket(self, ticket: str):
         consulta=(
            "SELECT DISTINCT ON (vh.vehicleplate) "
                    "vh.vehicleplate AS placa, "
                    "CASE "
                    "WHEN pard.datestop::date = now()::date THEN 'Reparacion'::text "
                    "ELSE 'activo'::text "
                    "END AS parada, "
                    "CASE "
                        #"WHEN regp.datedoc < CURRENT_DATE OR repd.datedoc < CURRENT_DATE AND tk.docstatus::text = 'CO'::text THEN 'Sin Ticket'::character varying "
                        "WHEN tk.docstatus::text = 'CO'::text THEN tk.documentno "
                        "WHEN tk.docstatus::text <> 'CO'::text THEN 'Sin Ticket'::character varying "
                        "ELSE 'Sin Ticket'::character varying "
                    "END AS ticket_entrada, "
                    "CASE "
						"WHEN tk.created IS NULL THEN 'Sin Ticket'::text "
						"ELSE to_char(tk.created, 'YYYY-MM-DD HH24:MI:SS') "
					"END AS fecha_ticket, "
                    "CASE "
                    #    "WHEN tk.fta_entryticket_id = car.fta_entryticket_id AND car.docstatus::text = 'CO'::text AND regp.datedoc < CURRENT_DATE THEN 'Sin Orden Carga'::character varying "
                        "WHEN tk.fta_entryticket_id = car.fta_entryticket_id AND car.docstatus::text = 'CO'::text THEN car.documentno "
                        "ELSE 'Sin Orden Carga'::character varying "
                    "END AS orde_carga, "
                    "CASE "
							"WHEN regp.datedoc < CURRENT_DATE AND car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'sin viaje'::text "
							"WHEN repd.documentno IS NOT NULL AND car.documentno IS NULL AND tk.operationtype = 'MOM'::bpchar AND repd.datedoc = CURRENT_DATE THEN 'acarreo'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype = 'MOM'::bpchar AND car.docstatus::text = 'CO'::text AND regp.datedoc <> CURRENT_DATE THEN 'sin viaje'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype = 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'despacho a sucursal'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text AND regp.datedoc <> CURRENT_DATE THEN 'despacho a cliente'::text "
							"WHEN car.documentno IS NOT NULL AND tk.operationtype <> 'MOM'::bpchar AND car.docstatus::text = 'CO'::text THEN 'despacho a cliente'::text "
							"ELSE 'sin viaje'::text "
						"END AS tipo_de_viaje, "
                    "CASE "
                        "WHEN mov2.m_movementparent_id = mov.m_movement_id AND regp.docstatus::text = 'CO'::text OR repd.docstatus::text = 'CO'::text AND car.docstatus::text = 'CO'::text THEN 'liberado'::text "
                        "WHEN regp.datedoc = CURRENT_DATE AND regp.docstatus::text = 'CO'::text THEN 'transito'::text "
                        "WHEN repd.datedoc = CURRENT_DATE AND repd.docstatus::text = 'CO'::text THEN 'transito'::text "
                        "WHEN req.documentno IS NOT NULL OR car.documentno IS NOT NULL AND car.docstatus::text = 'CO'::text AND regp.documentno IS NULL THEN 'no disponible'::text "
                        "ELSE 'disponible'::text "
                    "END AS estatusvehicle,"
                    "CASE "
					"WHEN regp.docstatus::text = 'CO'::text AND regp.datedoc::date = now()::date THEN regp.documentno "
					"WHEN repd.docstatus::text = 'CO'::text AND repd.datedoc::date = now()::date THEN repd.documentno "
					"ELSE 'Sin Peso'::character varying "
					"END AS registro_peso,"
                    "vh.isowner as esPropio "
            "FROM fta_vehicle vh "
                "LEFT JOIN fta_vehicle_stopline par ON par.fta_vehicle_id = vh.fta_vehicle_id AND par.created::date = now()::date "
                "LEFT JOIN fta_vehicle_stop pard ON pard.fta_vehicle_stop_id = par.fta_vehicle_stop_id "
                "LEFT JOIN m_requisition req ON req.m_requisition_id = pard.m_requisition_id "
                #"LEFT JOIN fta_entryticket tk ON tk.fta_vehicle_id = vh.fta_vehicle_id AND tk.datedoc >= (CURRENT_DATE - '4 day'::interval) AND tk.docstatus::text = 'CO'::text "
                #"LEFT JOIN fta_loadorder car ON car.fta_vehicle_id = vh.fta_vehicle_id AND tk.fta_entryticket_id = car.fta_entryticket_id AND car.datedoc >= (CURRENT_DATE - '4 day'::interval) AND car.docstatus::text = 'CO'::text "
                #"LEFT JOIN fta_recordweight regp ON tk.fta_entryticket_id = regp.fta_entryticket_id AND regp.datedoc >= (CURRENT_DATE - '4 day'::interval)  AND regp.docstatus::text = 'CO'::text "
                #"LEFT JOIN dd_recordweight repd ON repd.fta_entryticket_id = tk.fta_entryticket_id AND repd.datedoc >= (CURRENT_DATE - '4 day'::interval)  AND repd.docstatus::text = 'CO'::text "
                "LEFT JOIN fta_entryticket tk ON tk.fta_vehicle_id = vh.fta_vehicle_id AND tk.docstatus::text = 'CO'::text "
                "LEFT JOIN fta_loadorder car ON car.fta_vehicle_id = vh.fta_vehicle_id AND tk.fta_entryticket_id = car.fta_entryticket_id  AND car.docstatus::text = 'CO'::text "
                "LEFT JOIN fta_recordweight regp ON tk.fta_entryticket_id = regp.fta_entryticket_id AND regp.docstatus::text = 'CO'::text "
                "LEFT JOIN dd_recordweight repd ON repd.fta_entryticket_id = tk.fta_entryticket_id  AND repd.docstatus::text = 'CO'::text "
                "LEFT JOIN ad_org orgs ON orgs.ad_org_id = repd.ad_org_id OR orgs.ad_org_id = regp.ad_org_id "
                "LEFT JOIN dd_orderline od ON od.dd_orderline_id = repd.dd_orderline_id "
                "LEFT JOIN m_locator loc ON loc.m_warehouse_id = regp.m_warehouse_id OR loc.m_locator_id = od.m_locator_id "
                "LEFT JOIN m_locator loc2 ON loc2.m_locator_id = od.m_locatorto_id "
                "LEFT JOIN m_movement mov ON mov.fta_recordweight_id = regp.fta_recordweight_id OR mov.dd_recordweight_id = repd.dd_recordweight_id AND mov.movementdate = CURRENT_DATE AND mov.docstatus = 'CO'::bpchar "
                "LEFT JOIN m_movement mov2 ON mov2.m_movementparent_id = mov.m_movement_id "
                "LEFT JOIN m_movementline movl ON movl.m_movement_id = mov2.m_movement_id "
                "LEFT JOIN m_locator mloc ON mloc.m_locator_id = movl.m_locator_id "
                "LEFT JOIN ad_org orgd ON orgd.ad_org_id = mov2.ad_org_id "
                "LEFT JOIN c_doctype doct ON doct.c_doctype_id = mov.c_doctype_id "
                "WHERE tk.documentno=%s "
                "ORDER BY vh.vehicleplate, fecha_ticket DESC;")
         try:
                # Ejecuta la consulta de prueba
                self.cursor.execute(consulta, (ticket,))
                result = self.cursor.fetchone()
                print("Resultados de la consulta del ticket:", result)
                if result is None:
                    print('el resultado de la consulta es None')
                    return None
                if result is not None:
                    return StatusVehicle(
                                placa=result[0], 
                                parada=result[1], 
                                ticket_entrada=result[2],
                                fecha_ticket=result[3],
                                orden_carga=result[4],
                                tipo_viaje=result[5],
                                estatus_vehicle=result[6],
                                registro_peso=result[7],
                                esPropio=result[8],
                                )
                else:
                    return None
         except psycopg2.Error as e:
                    print("Error al ejecutar la consulta:", e)
                    return e

    def getNameBPartner(self, documentNo: str):
        try:
            match = re.search(r"CI/RIF:\s*([A-Z]\d+)", documentNo)
            if match:
                documentNo = match.group(1)
                print(f'DOCUMENTNO: {documentNo}')
            else:
                return None
            consulta = f"""
            select Name from C_BPartner
            where Value like '%{documentNo}%'
            limit 1
            """
            print(f'consulta socio: {consulta}')
            self.cursor.execute(consulta)
            result = self.cursor.fetchall()
            name_producer = ''
            if result:
                for resultado in result:
                    name_producer =resultado[0]
                return name_producer
            else:
                print("No se encontro el socio.")
                return None
        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de Obtener Entregas:", e)
            return str(e)

    #Funcion que obtiene las entregas
    def getDelivery(self, invoiceId: str):
        try:
            consulta = f"""
            select mi.M_InOut_ID from c_invoice as ci
            join c_order co ON co.C_Order_ID = ci.C_Order_ID
            join M_InOut mi ON mi.C_Order_ID = co.C_Order_ID
            where ci.C_Invoice_ID={invoiceId} 
            and (mi.is_confirm is NULL OR mi.is_confirm='N')
            and mi.docstatus ='CO'
            """
            self.cursor.execute(consulta)
            result = self.cursor.fetchall()
            deliveryID = ''
            response = []
            if result:
                for resultado in result:
                    deliveryID =resultado[0]
                    response.append(deliveryID)
                return response
            else:
                print("No se encontraron Entregas.")
                return None

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de Obtener Entregas:", e)
            return str(e)
    
    #Funcion obtiene las ordenes de devolucion
    def getReturns(self):
        try:
            #Consulta para obtener las devoluciones
            consulta = f"""
            select co.c_order_id, co.documentno, ado.name as name_org, cb.name as name_bp, co.DateOrdered FROM C_Order as co
            JOIN AD_Org ado ON co.AD_Org_ID = ado.AD_Org_ID
            JOIN C_BPartner cb ON co.C_BPartner_ID = cb.C_BPartner_ID
            WHERE co.C_DocTypeTarget_ID=1000790 
            AND co.DocStatus='CO'
            AND co.DateOrdered >= DATE_TRUNC('year', CURRENT_DATE); 
            """
            self.cursor.execute(consulta)
            result = self.cursor.fetchall()
            response = []
            if result:
                for resultado in result:
                    date_only = resultado[4].date()
                    orderReturn = OrderReturn(
                        orderReturnID=str(resultado[0]),
                        documentNo=resultado[1],
                        name_org=resultado[2],
                        name_bp=resultado[3],
                        dateordered=str(date_only),
                    ),
                    response.append(orderReturn)
                return response
            else:
                print("No se encontraron Entregas.")
                return None
            
        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de Obtener las Devoluciones:", e)
            return str(e)

    #Funcion obtiene el detalle de la orden de devolucions
    def getDetailReturn(self, orderId: str):
        try:
            #Primera consulta para obtener los tickets
            consulta = f"""
            select mp.name, mp.M_Product_ID, col.qtyentered from c_order as co
            JOIN C_OrderLine col ON co.C_Order_ID = col.C_Order_ID
            JOIN M_Product mp ON col.M_Product_ID = mp.M_Product_ID
            WHERE co.C_Order_ID={orderId}
            """
            self.cursor.execute(consulta)
            result = self.cursor.fetchall()
            response = []
            if result:
                print("Resultados de la consulta:", result)
                for resultado in result:
                    product = Product(
                        name=resultado[0],
                        product_id=str(resultado[1]),
                        quantity=resultado[2],
                    )
                    response.append(product)
                return response
            else:
                print("No se encontraron Lineas de la Factura.")
                return None

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de tickets:", e)
            return str(e)

    #Funcion debo BORRAR por que se actualizaran las facturas por XML
    def confirmInvoice(self, invoiceId: str):
        try:
            #Primera consulta para obtener los tickets
            consulta = f"""
            UPDATE c_invoice SET is_confirm='Y' WHERE c_invoice_id={invoiceId}
            """
            self.cursor.execute(consulta)
            self.connection.commit()
            
            return {"Respuesta":"Factura Confirmada."} 

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de Obtener Entregas:", e)
            return str(e) 
        
    #Funcion debo BORRAR por que se actualizaran las entregas por XML
    def confimDelivery(self, listDeliveryIds: list):
        try:
            for deliveryId in listDeliveryIds:
                consulta = f"""
                UPDATE M_InOut SET is_confirm='Y' WHERE M_InOut_ID={deliveryId}
                """
                self.cursor.execute(consulta)
                self.connection.commit()

            return {"Respuesta":"Entrega actualizada."} 

        except psycopg2.Error as e:
            print("Error al ejecutar la consulta de Obtener Entregas:", e)
            return str(e) 

     #Funcion que obtiene la planificacion de una orden de carga
    def getPlanning(self, orden_carga: str):
        
            consulta = (f'''
            SELECT 
            CASE
            WHEN mw.name like '%P1%' then 'p1'
            WHEN mw.name like '%P2%' then 'p2'
            WHEN mw.name like '%P3%' then 'p3'
            WHEN mw.name like '%P4%' then 'p4'
            WHEN mw.name like '%P5%' then 'p5'
            end as name_warehouse
            from fta_loadplanner fl 
            JOIN m_warehouse mw ON fl.m_warehouse_id = mw.m_warehouse_id
            JOIN fta_loadorder fo ON fl.fta_loadorder_id = fo.fta_loadorder_id 
            WHERE fo.documentno ='{orden_carga}' ORDER BY name_warehouse
            ''')
            try:
                print("Consulta SQL:", consulta)
                print("Parámetros:", (orden_carga,))
                self.cursor.execute(consulta, )
                result = self.cursor.fetchall()
                print(result)
                response = []
                if result:
                    for resultado in result:      
                        planning = Planning(
                            
                            punto_escaner=resultado[0],
                        )
                        response.append(planning)
                    return response
                else:
                    return None
            except psycopg2.Error as e:
                print("Error al ejecutar la consulta:", e)
                return e
    
    #Funcion que confirma si el tilde de registro de peso en la orden de carga esta tildado
    def confirmRecordWeight(self, ticket_id:str):
        consulta=(f'''
            SELECT  
            CASE  
                WHEN tk.docstatus::text = 'CO'::text THEN tk.documentno 
                WHEN tk.docstatus::text <> 'CO'::text THEN 'Sin Ticket'::character varying 
                ELSE 'Sin Ticket'::character varying 
            END AS ticket_entrada, 
            CASE 
                WHEN tk.fta_entryticket_id = car.fta_entryticket_id AND car.docstatus::text = 'CO'::text THEN car.documentno 
                ELSE 'Sin Orden Carga'::character varying 
            END AS orde_carga,
            CASE
                WHEN rewl.FTA_LoadOrderLine_ID = carline.FTA_LoadOrderLine_ID  AND rewl.FTA_RecordWeight_ID = rw.FTA_RecordWeight_ID THEN rw.DocumentNo
                ELSE 'sin registro de peso'::character varying 
            END AS registro_peso
        FROM fta_entryticket tk
        LEFT JOIN fta_loadorder car ON car.fta_entryticket_id = tk.fta_entryticket_id AND car.docstatus::text = 'CO'::text 
        LEFT JOIN FTA_LoadOrderLine carline ON carline.FTA_LoadOrder_ID = car.FTA_LoadOrder_ID 
        LEFT JOIN FTA_RecordWeightLoadOrder rewl ON rewl.FTA_LoadOrderLine_ID = carline.FTA_LoadOrderLine_ID
        LEFT JOIN FTA_RecordWeight rw ON rw.FTA_RecordWeight_ID = rewl.FTA_RecordWeight_ID
        WHERE tk.documentno='{ticket_id}'
            ''')
        try:
                self.cursor.execute(consulta, )
                result = self.cursor.fetchall()
                print(result)
                checkout = False
                if result:
                    for resultado in result:      
                        recordweight = resultado[2]
                        if recordweight != 'sin registro de peso':
                            checkout = True
                        else:
                            checkout = False
                    return checkout
                else:
                    return checkout
                
        except psycopg2.Error as e:
            print("Error al ejecutar la consulta:", e)
            return e

    def closeConnection(self):
        # Cierra el cursor y la conexión
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    #Funcion para crear la cabecera de la orden de Devolucion
    async def createReturnOrder(self, request:CreateReturn):
        url = 'http://adempiere-fc-engine.iancarina.com.ve/ADInterface/services/ModelADService'
        description = f'Orden creada por el chofer: {request.name_driver} {request.driver_id}'
        xml = f'''
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:adin="http://3e.pl/ADInterface">
        <soapenv:Header/>
        <soapenv:Body>
            <adin:createData>
                <adin:ModelCRUDRequest>
                    <adin:ModelCRUD>
                        <adin:serviceType>CreateOrderPedidos</adin:serviceType>
                        <adin:TableName>C_Order</adin:TableName>
                        <adin:RecordID>0</adin:RecordID>
                        <adin:Action>Create</adin:Action>
                        <adin:PageNo>0</adin:PageNo>
                        <adin:DataRow>
                            <adin:field column="AD_Client_ID">
                                <adin:val>1000000</adin:val>
                            </adin:field>
                            <adin:field column="AD_Org_ID">
                                <adin:val>1000009</adin:val>
                            </adin:field>
                            <adin:field column="C_DocTypeTarget_ID">
                                <adin:val>1000790</adin:val>
                            </adin:field>
                            <adin:field column="Description">
                                <adin:val>{description}</adin:val>
                            </adin:field>
                            <adin:field column="C_BPartner_ID">
                                <adin:val>{request.c_bpartner_id}</adin:val>
                            </adin:field>
                            <adin:field column="M_Warehouse_ID">
                                <adin:val>1000058</adin:val>
                            </adin:field>
                        </adin:DataRow>
                    </adin:ModelCRUD>
                    <adin:ADLoginRequest>
                        <adin:user>dGarcia</adin:user>
                        <adin:pass>dGarcia</adin:pass>
                        <adin:lang>es_VE</adin:lang>
                        <adin:ClientID>1000000</adin:ClientID>
                        <adin:RoleID>1000000</adin:RoleID>
                        <adin:OrgID>0</adin:OrgID>
                        <adin:WarehouseID>0</adin:WarehouseID>
                        <adin:stage>0</adin:stage>
                    </adin:ADLoginRequest>
                </adin:ModelCRUDRequest>
            </adin:createData>
        </soapenv:Body>
        </soapenv:Envelope>
        '''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
        }

        try:
            response = requests.post(url, headers=headers, data=xml)

            if response.status_code == 200:
                response_dict = xmltodict.parse(response.content)
                print(response.content)
                recordID = response_dict.get('soap:Envelope', {}).get('soap:Body', {}).get('ns1:createDataResponse', {}).get('StandardResponse', {}).get('@RecordID', '')
                if recordID:
                    print(f'Se creo la devolucion, RecordID: {recordID}')
                    return recordID
                
                print('NO SE CREO LA DEVOLUCION PADRE')
                return None

            else:
                raise HTTPException(status_code=response.status_code, detail="Error de conexion creando la devolucion")
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error de conexion: {e}")


    #Funcion para crear las lineas de la orden de Devolucion
    async def createReturnOrderLine(self, order_id:str, product_id:str, quantity:str):
        url = 'http://adempiere-fc-engine.iancarina.com.ve/ADInterface/services/ModelADService'
        xml = f'''
      <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:adin="http://3e.pl/ADInterface">
      <soapenv:Header/>
      <soapenv:Body>
          <adin:createData>
              <adin:ModelCRUDRequest>
                  <adin:ModelCRUD>
                      <adin:serviceType>CreateOrderLinePedidos</adin:serviceType>
                      <adin:TableName>C_OrderLine</adin:TableName>
                      <adin:RecordID>0</adin:RecordID>
                      <adin:Action>Create</adin:Action>
                      <adin:PageNo>0</adin:PageNo>
                      <adin:DataRow>
                          <adin:field column="C_Order_ID">
                              <adin:val>{order_id}</adin:val>
                          </adin:field>
                          <adin:field column="AD_Client_ID">
                            <adin:val>1000000</adin:val>
                          </adin:field>
                          <adin:field column="AD_Org_ID">
                            <adin:val>1000009</adin:val>
                          </adin:field>
                          <adin:field column="M_Product_ID">
                            <adin:val>{product_id}</adin:val>
                          </adin:field>
                          <adin:field column="QtyEntered">
                            <adin:val>{quantity}</adin:val>
                          </adin:field>
                      </adin:DataRow>
                  </adin:ModelCRUD>
                  <adin:ADLoginRequest>
                      <adin:user>dGarcia</adin:user>
                      <adin:pass>dGarcia</adin:pass>
                      <adin:lang>es_VE</adin:lang>
                      <adin:ClientID>1000000</adin:ClientID>
                      <adin:RoleID>1000000</adin:RoleID>
                      <adin:OrgID>0</adin:OrgID>
                      <adin:WarehouseID>0</adin:WarehouseID>
                      <adin:stage>0</adin:stage>
                  </adin:ADLoginRequest>
              </adin:ModelCRUDRequest>
          </adin:createData>
      </soapenv:Body>
      </soapenv:Envelope>
        '''
    
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
        }

        try:
            response = requests.post(url, headers=headers, data=xml)

            if response.status_code == 200:
                response_dict = xmltodict.parse(response.content)
                print(response.content)
                recordID = response_dict.get('soap:Envelope', {}).get('soap:Body', {}).get('ns1:createDataResponse', {}).get('StandardResponse', {}).get('@RecordID', '')
                if recordID:
                    print(f'Se creo la devolucion, RecordID: {recordID}')
                    return recordID
                
                print('NO SE CREO LA DEVOLUCION PADRE')
                return None

            else:
                raise HTTPException(status_code=response.status_code, detail="Error de conexion creando la devolucion")
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error de conexion: {e}")


    #Funcion que actualiza el campo is_confirm en la factura
    async def UpdateInvoiceConfirm(self, c_invoice_id:str):
        url = 'http://adempiere-fc-engine.iancarina.com.ve/ADInterface/services/ModelADService'
        xml = f'''
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:adin="http://3e.pl/ADInterface">
                <soapenv:Header/>
                <soapenv:Body>
                    <adin:updateData>
                        <adin:ModelCRUDRequest>
                            <adin:ModelCRUD>
                            <adin:serviceType>UpdateIsConfirmInvoice</adin:serviceType>
                            <adin:RecordID>{c_invoice_id}</adin:RecordID>
                            <!--Optional:-->
                            <adin:DataRow>
                                <adin:field column="is_confirm">
                                    <adin:val>Y</adin:val>
                                </adin:field>
                            </adin:DataRow>
                            </adin:ModelCRUD>
                            <adin:ADLoginRequest>
                            <adin:user>dGarcia</adin:user>
                            <adin:pass>dGarcia</adin:pass>
                            <adin:lang>es_VE</adin:lang>
                            <adin:ClientID>1000000</adin:ClientID>
                            <adin:RoleID>1000000</adin:RoleID>
                            <adin:OrgID>0</adin:OrgID>
                            <adin:WarehouseID>0</adin:WarehouseID>
                            <adin:stage>0</adin:stage>
                            </adin:ADLoginRequest>
                        </adin:ModelCRUDRequest>
                        </adin:queryData>
                    </soapenv:Body>
                    </soapenv:Envelope>
        '''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
        }

        try:
            response = requests.post(url, headers=headers, data=xml)

            if response.status_code == 200:
                response_dict = xmltodict.parse(response.content)
                print(response.content)
                recordID = response_dict.get('soap:Envelope', {}).get('soap:Body', {}).get('ns1:updateDataResponse', {}).get('StandardResponse', {}).get('@RecordID', '')
                if recordID:
                    print(f'Se creo la devolucion, RecordID: {recordID}')
                    return recordID
                
                print('NO SE CREO LA DEVOLUCION PADRE')
                return None

            else:
                raise HTTPException(status_code=response.status_code, detail="Error de conexion creando la devolucion")
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error de conexion: {e}")

    #Funcion que actualiza el campo is_confirm en la entrega
    async def UpdateDeliveryConfirm(self, m_inout_id:str):
        url = 'http://adempiere-fc-engine.iancarina.com.ve/ADInterface/services/ModelADService'
        xml = f'''
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:adin="http://3e.pl/ADInterface">
                <soapenv:Header/>
                <soapenv:Body>
                    <adin:updateData>
                        <adin:ModelCRUDRequest>
                            <adin:ModelCRUD>
                            <adin:serviceType>UpdateIsConfirmInOut</adin:serviceType>
                            <adin:RecordID>{m_inout_id}</adin:RecordID>
                            <!--Optional:-->
                            <adin:DataRow>
                                <adin:field column="is_confirm">
                                    <adin:val>Y</adin:val>
                                </adin:field>
                            </adin:DataRow>
                            </adin:ModelCRUD>
                            <adin:ADLoginRequest>
                            <adin:user>dGarcia</adin:user>
                            <adin:pass>dGarcia</adin:pass>
                            <adin:lang>es_VE</adin:lang>
                            <adin:ClientID>1000000</adin:ClientID>
                            <adin:RoleID>1000000</adin:RoleID>
                            <adin:OrgID>0</adin:OrgID>
                            <adin:WarehouseID>0</adin:WarehouseID>
                            <adin:stage>0</adin:stage>
                            </adin:ADLoginRequest>
                        </adin:ModelCRUDRequest>
                        </adin:queryData>
                    </soapenv:Body>
                    </soapenv:Envelope>
        '''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
        }

        try:
            response = requests.post(url, headers=headers, data=xml)

            if response.status_code == 200:
                response_dict = xmltodict.parse(response.content)
                print(response.content)
                recordID = response_dict.get('soap:Envelope', {}).get('soap:Body', {}).get('ns1:updateDataResponse', {}).get('StandardResponse', {}).get('@RecordID', '')
                if recordID:
                    print(f'Se creo la devolucion, RecordID: {recordID}')
                    return recordID
                
                print('NO SE CREO LA DEVOLUCION PADRE')
                return None

            else:
                raise HTTPException(status_code=response.status_code, detail="Error de conexion creando la devolucion")
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error de conexion: {e}")   
        
            

