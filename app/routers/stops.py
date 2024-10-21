from typing import Annotated
from models.tokenModels import User
from services.nevada import NevadaConnect
from models.driver import InfoScanner
from services.bd_barcode import BdCodeBarConnect
from services.adempiere import AdempiereConnect
from fastapi import APIRouter, Depends, HTTPException
from .logins import get_current_user

router = APIRouter()

@router.get('/location/stop/alltickets', tags=["Stops"],)
def get_Tickets(current_user: Annotated[User, Depends(get_current_user)]):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        tickets_nevada = conn_nevada.getAllTickets()
        if tickets_nevada is None:
            return ({'Mensaje':'No hay datos guardados en la base de datos.'})
        
        if tickets_nevada is not None:
            
            return tickets_nevada
    finally:
        conn_nevada.closeConnection

@router.get('/getcodescanners/', tags=["Stops"],)
async def get_Tickets():
    try:
        conn_codebar = BdCodeBarConnect()
        conn_codebar.connect()
        scanners = await conn_codebar.getScanners()
        print(scanners)
        if scanners is None:
            return ({'Mensaje':'No hay registros nuevos.'})
        if scanners is not None:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            for scan in scanners:
                print(f'SCANNNNNN: {scan}')
                equipo_id = int(scan.id_equipo)
                if scan.entrada_scanner.startswith('TE'):
                    ticket_adempiere = conn_adempiere.getTicket(scan.entrada_scanner)
                    if ticket_adempiere is None:
                        print('el ticket no existe en adempiere')
                    if ticket_adempiere is not None:
                        try:
                            conn_nevada = NevadaConnect()
                            conn_nevada.connect()
                            ticket_nevada = conn_nevada.getTicket(scan.entrada_scanner)
                            if ticket_nevada is None:
                                conn_nevada.insertTrakingDynamic(ticket_adempiere, scan, None)
                                conn_nevada.firstScannerDynamic(ticket_adempiere, scan, False)
                                conn_codebar.syncCodeBar(scan.entrada_scanner)
                                print({'Mensaje':f'Ticket insertado en {scan.nombre_scanner}'})

                            if ticket_nevada is not None:  
                                    
                                    if equipo_id == 4 and scan.tipo == 'S':
                                            conn_nevada.insertTrakingDynamic(ticket_adempiere, scan, None)
                                            conn_nevada.finishDocument(scan.entrada_scanner)
                                            conn_codebar.syncCodeBar(scan.entrada_scanner)
                                            print({'Mensaje':f'el documento: {scan.entrada_scanner} a finalizado su recorrido'})

                                    conn_nevada.insertTrakingDynamic(ticket_adempiere, scan, None)
                                    conn_nevada.updateTicket(ticket_nevada.ticket_entrada, scan.nombre_scanner, equipo_id)
                                    conn_codebar.syncCodeBar(scan.entrada_scanner)
                                    print({'Mensaje':f'ticket actualizado a {scan.nombre_scanner}'}) 
                        finally:
                            conn_nevada.closeConnection
                else:
                    try:
                        conn_nevada = NevadaConnect()
                        conn_nevada.connect()
                        code_insai = conn_nevada.getTicketInsai(scan.entrada_scanner)

                        if code_insai is None:
                            conn_nevada.insertTrakingDynamic(None, scan, None)
                            if scan.detalle_guia.startswith('CI'):
                                 #quedaste aqui, agregarle el atributo nombre al objeto scan para mostrarlo
                                 name_bp = conn_adempiere.getNameBPartner(scan.detalle_guia)
                                 if name_bp is not None:
                                    scan.nombre_productor = name_bp
                                         
                            conn_nevada.firstScannerDynamic(None, scan, True)
                            conn_codebar.syncCodeBar(scan.entrada_scanner)
                            print({'Mensaje':f'Ticket insertado en {scan.nombre_scanner}'})
                        
                        if code_insai is not None:
                                
                                ticket_insai = conn_adempiere.getTicketxInsai(scan.entrada_scanner)

                                if equipo_id == 4 and scan.tipo == 'S':
                                            conn_nevada.insertTrakingDynamic(None, scan, ticket_insai)
                                            conn_nevada.finishDocument(scan.entrada_scanner)
                                            conn_codebar.syncCodeBar(scan.entrada_scanner)
                                            print({'Mensaje':f'el documento: {scan.entrada_scanner} a finalizado su recorrido'}) 
                                
                                if ticket_insai is None:
                                    conn_nevada.insertTrakingDynamic(None,scan,code_insai)
                                    conn_nevada.updateTicket(scan.entrada_scanner, scan.nombre_scanner, equipo_id)
                                    conn_codebar.syncCodeBar(scan.entrada_scanner)
                                    print({'Mensaje':f'ticket actualizado a {scan.nombre_scanner}'})
                                
                                if ticket_insai is not None:
                                    conn_nevada.insertTrakingDynamic(None,scan,ticket_insai)
                                    conn_nevada.addTicketToInsai(ticket_insai, scan)
                                    conn_codebar.syncCodeBar(scan.entrada_scanner)
                                    print({'Mensaje':f'ticket actualizado a {scan.nombre_scanner}'}) 
                    finally:
                            conn_nevada.closeConnection

            conn_adempiere.closeConnection
            return scanners
    finally:
        conn_codebar.closeConnection


@router.get('/location/stop/allticketsadmin/{company_id}/{user_id}', tags=["Stops"],)
async def get_Tickets(company_id:str, user_id:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        permissionUser = await conn_nevada.accessAdmin(user_id, company_id)
        if permissionUser is None:
            return ({'Mensaje':'No tiene permiso de acceso el Usuario.'})
        
        tickets_nevada = conn_nevada.getAllTicketsAdmin(company_id, permissionUser)
        if tickets_nevada is None:
            return ({'Mensaje':'No hay datos guardados en la base de datos.'})
        
        if tickets_nevada is not None:
            return tickets_nevada
    finally:
        conn_nevada.closeConnection

@router.get('/getPolygons/{company_id}', tags=["Stops"],)
async def get_Tickets(company_id:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        polygons = await conn_nevada.getPolygons(company_id)
        if polygons is None:
            return ({'Mensaje':'No hay poligonos.'})
        
        if polygons is not None:
            return polygons
    finally:
        conn_nevada.closeConnection

@router.get('/getPositionsPolygons/{polygon_id}/{lengthVehicles}', tags=["Stops"],)
async def get_Tickets(polygon_id:str, lengthVehicles:int):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        polygons = await conn_nevada.getPositions(polygon_id, lengthVehicles)
        if polygons is None:
            return ({'Mensaje':'No hay posiciones para ese poligono.'})
        
        if polygons is not None:
            return polygons
    finally:
        conn_nevada.closeConnection

@router.get('/location/stop/getrackingadmin/{ticket}', tags=["Stops"],)
def get_Tracking(ticket:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        tracking_nevada = conn_nevada.getTracking(ticket)
        print(tracking_nevada)
        if tracking_nevada is None:
            return ({'Mensaje':'No hay datos guardados en la base de datos.'})  
        if tracking_nevada is not None:            
            return tracking_nevada
    finally:
        conn_nevada.closeConnection


@router.get('/location/stop/getracking/{ticket}', tags=["Stops"],)
def get_Tracking(current_user: Annotated[User, Depends(get_current_user)], ticket:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        tracking_nevada = conn_nevada.getTracking(ticket)
        print(tracking_nevada)
        if tracking_nevada is None:
            return ({'Mensaje':'No hay datos guardados en la base de datos.'})  
        if tracking_nevada is not None:            
            return tracking_nevada
    finally:
        conn_nevada.closeConnection

@router.get('/location/stop/getimetrackingadmin/{ticket}', tags=["Stops"],)
def get_Tracking(ticket:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        tracking_nevada = conn_nevada.getTimeTracking(ticket)
        print(tracking_nevada)
        if tracking_nevada is None:
            return ({'Mensaje':'No hay datos guardados en la base de datos.'})  
        if tracking_nevada is not None:            
            return tracking_nevada
    finally:
        conn_nevada.closeConnection

@router.get('/location/stop/getimetracking/{ticket}', tags=["Stops"],)
def get_Tracking(current_user: Annotated[User, Depends(get_current_user)], ticket:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        tracking_nevada = conn_nevada.getTimeTracking(ticket)
        print(tracking_nevada)
        if tracking_nevada is None:
            return ({'Mensaje':'No hay datos guardados en la base de datos.'})  
        if tracking_nevada is not None:            
            return tracking_nevada
    finally:
        conn_nevada.closeConnection

@router.get('/location/stop/getplanningadmin/{orden_carga}', tags=["Stops"],)
def get_Tracking(orden_carga:str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        planing = conn_adempiere.getPlanning(orden_carga)
        print(planing)
        if planing is None:
            return ({'Mensaje':'No hay planificacion de carga.'})  
        if planing is not None:            
            return planing
    finally:
        conn_adempiere.closeConnection


@router.get('/location/stop/getplanning/{orden_carga}', tags=["Stops"],)
def get_Tracking(current_user: Annotated[User, Depends(get_current_user)], orden_carga:str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        planing = conn_adempiere.getPlanning(orden_carga)
        print(planing)
        if planing is None:
            return ({'Mensaje':'No hay planificacion de carga.'})  
        if planing is not None:            
            return planing
    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/workshop/{ticket}', tags=["Stops"],)
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        print(f'checkout es igual a:{checkout}')
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                print(ticket_nevada)
                if ticket_nevada is None:
                    conn_nevada.insertTraking(ticket_status, 'taller')
                    conn_nevada.firstScanner(ticket_status, 'taller')
                    return ({'Mensaje':'Ticket insertado y actualizado a Taller y Transporte'})
                if ticket_nevada is not None:   
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'taller')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return ({'Mensaje':'ticket actualizado a Taller y Transporte y registro de peso Completado'}) 
                    else:
                        conn_nevada.insertTraking(ticket_status, 'taller')
                        conn_nevada.stoptransport(ticket_nevada)
                        return ({'Mensaje':'ticket actualizado a Taller y Transporte'}) 
            finally:
                conn_nevada.closeConnection
    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/yard/{ticket}', tags=["Stops"],)
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    
                    conn_nevada.insertTraking(ticket_status, 'patio')
                    conn_nevada.firstScanner(ticket_status, 'patio')
                    return {'Mensaje': 'Ticket insertado por primera vez en Patio'}
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'patio')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a patio y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'patio')
                        conn_nevada.stopyard(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a patio'}
                                 
            finally:
                conn_nevada.closeConnection

    finally:
        conn_adempiere.closeConnection


@router.get('/location/stop/updatevigilance/{ticket}', tags=["Stops"],)
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            print(ticket_status.esPropio)
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                conn_nevada.insertTraking(ticket_status, 'vigilancia')
                ticket_nevada = conn_nevada.getTicket(ticket)
                if ticket_nevada is None:

                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    
                    conn_nevada.firstScanner(ticket_status, 'vigilancia')
                    return {'Mensaje': 'Ticket insertado por primera vez en vigilancia'}
                
                if ticket_nevada is not None:

                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'vigilancia')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a vigilancia y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'vigilancia')
                        conn_nevada.stopvigilance(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a vigilancia'}
            finally:
                conn_nevada.closeConnection
    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/updatevigilance2/{ticket}', tags=["Stops"],)
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            print(ticket_status.esPropio)
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                conn_nevada.insertTraking(ticket_status, 'vigilancia2')
                ticket_nevada = conn_nevada.getTicket(ticket)
                if ticket_nevada is None:

                    if ticket_status.esPropio == 'Y':
                        return {'Mensaje': 'El primer scanner de ticket debe ser en Taller.'}
                    
                    conn_nevada.firstScanner(ticket_status, 'vigilancia2')
                    return {'Mensaje': 'Ticket insertado por primera vez en vigilancia2'}
                
                if ticket_nevada is not None: 
                     if checkout:
                        conn_nevada.insertTraking(ticket_status, 'vigilancia2')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a vigilancia2 y registro de peso Completado'}
                     else:
                        conn_nevada.insertTraking(ticket_status, 'vigilancia2')
                        conn_nevada.stopvigilance2(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a vigilancia2'}
                         
            finally:
                conn_nevada.closeConnection
    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/updateromana/{ticket}', tags=["Stops"])
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    conn_nevada.insertTraking(ticket_status, 'romana')
                    conn_nevada.firstScanner(ticket_status, 'romana')
                    return ({'Mensaje':'Ticket insertado y actualizado a romana'})
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'romana')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a romana y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'romana')
                        conn_nevada.stopromana(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a romana'}
            finally:
                conn_nevada.closeConnection


    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/updatep1/{ticket}', tags=["Stops"])
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    conn_nevada.insertTraking(ticket_status, 'p1')
                    conn_nevada.firstScanner(ticket_status, 'p1')
                    return ({'Mensaje':'Ticket insertado y actualizado a p1'})
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'p1')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a p1 y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'p1')
                        conn_nevada.stop1(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a p1'}              
            finally:
                conn_nevada.closeConnection

    finally:
        conn_adempiere.closeConnection

    

@router.get('/location/stop/updatep2/{ticket}', tags=["Stops"])
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                print(ticket_nevada)
                

                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    conn_nevada.insertTraking(ticket_status, 'p2')
                    conn_nevada.firstScanner(ticket_status, 'p2')
                    return ({'Mensaje':'Ticket insertado y actualizado a planta 2'})
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'p2')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a p2 y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'p2')
                        conn_nevada.stop2(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a p2'}    
            finally:
                conn_nevada.closeConnection
    finally:
        conn_adempiere.closeConnection


@router.get('/location/stop/updatep3/{ticket}', tags=["Stops"])
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                print(ticket_nevada)
                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    conn_nevada.insertTraking(ticket_status, 'p3')
                    conn_nevada.firstScanner(ticket_status, 'p3')
                    return ({'Mensaje':'Ticket insertado y actualizado a vigilancia'})
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'p3')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a p3 y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'p3')
                        conn_nevada.stop3(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a p3'}
            finally:
                conn_nevada.closeConnection


    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/updatep4/{ticket}', tags=["Stops"])
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                print(ticket_nevada)
                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    conn_nevada.insertTraking(ticket_status,'p4')
                    conn_nevada.firstScanner(ticket_status,'p4')
                    return ({'Mensaje':'Ticket insertado y actualizado a vigilancia'})
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'p4')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a p4 y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'p4')
                        conn_nevada.stop4(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a p4'}
            finally:
                conn_nevada.closeConnection


    finally:
        conn_adempiere.closeConnection

@router.get('/location/stop/updatep5/{ticket}', tags=["Stops"])
def update_ticket(ticket: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        ticket_status = conn_adempiere.getTicket(ticket)
        checkout =  conn_adempiere.confirmRecordWeight(ticket)
        if ticket_status is None:
            return HTTPException(status_code=404, detail="El ticket no existe en Adempiere")
        
        if ticket_status is not None:
            try:
                conn_nevada = NevadaConnect()
                conn_nevada.connect()
                ticket_nevada = conn_nevada.getTicket(ticket)
                print(ticket_nevada)

                if ticket_nevada is None:
                    if ticket_status.esPropio == 'Y':
                        return {'Error': 'El primer scanner de ticket debe ser en Taller.'}
                    conn_nevada.insertTraking(ticket_status,'p5')
                    conn_nevada.firstScanner(ticket_status, 'p5')
                    return ({'Mensaje':'Ticket insertado y actualizado a p5'})
                
                if ticket_nevada is not None:
                    if checkout:
                        conn_nevada.insertTraking(ticket_status, 'p5')
                        conn_nevada.completeRecordWeight(ticket_status)
                        return {'Mensaje': 'ticket actualizado a p5 y registro de peso Completado'}
                    else:
                        conn_nevada.insertTraking(ticket_status, 'p5')
                        conn_nevada.stop5(ticket_nevada)
                        return {'Mensaje': 'ticket actualizado a p5'}
            finally:
                conn_nevada.closeConnection

    finally:
        conn_adempiere.closeConnection