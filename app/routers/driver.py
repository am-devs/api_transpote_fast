from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from models.driver import Ubication, StatusVehicle, ReturnInvoice, CreateReturn, AuthorizationOrder
from .logins import get_current_user
from models.tokenModels import User
from services.nevada import NevadaConnect
from services.adempiere import AdempiereConnect

router = APIRouter()
#codigo para arrancar servidor local para pruebas
#uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
  
#ruta que ingresa la ubicacion de los choferes en BD Nevada
@router.post('/ubicacion',tags=["driver"])
def save_ubi(current_user: Annotated[User, Depends(get_current_user)], ubicacion: Ubication):
    #codigo que insertara en la base de datos nevada las ubicaciones de los choferes
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = conn_nevada.post_ubi(ubicacion)
        if result == 1:
            return {'detail':'Se Insertaron y Actualizaron los datos Correctamente'}
        if result == 2:
            return {'detail':'Se Insertaron los datos Correctamente'}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_nevada.closeConnection()

@router.post('/createReturn',tags=["driver"])
def save_ubi(return_invoice: ReturnInvoice):
    #codigo que insertara en la base de datos nevada las devoluciones.
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = conn_nevada.createReturn(return_invoice)
        if result == 1:
            return {'detail':'Se Insertaron los datos de la devolucion correctamente Correctamente'}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_nevada.closeConnection()

@router.get('/loginAdmin/{username}/{password}', tags=["logins"])
async def loginAdmin(username:str, password:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = await conn_nevada.loginAdmin(username, password)
        
        if result is not None:
            return result
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos")
        
    finally:
        conn_nevada.closeConnection()

@router.get('/AdminAccess/{user_id}', tags=["driver"])
async def loginAdmin(user_id:int):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = await conn_nevada.accessAdmin(user_id)

        if result is not None:
            return result
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos")
        
    finally:
        conn_nevada.closeConnection()

@router.get('/getCompanies/{user_id}', tags=["driver"])
async def loginAdmin(user_id:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = await conn_nevada.getCompanies(user_id)
        if result is not None:
            return result
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos")    
    finally:
        conn_nevada.closeConnection()

@router.post('/createOrderReturn',tags=["driver"])
async def createOrderReturn(order_return: CreateReturn):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        order_return_id  = await conn_adempiere.createReturnOrder(order_return)
        if order_return_id is not None:
            return {'record_id':order_return_id}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_adempiere.closeConnection()

@router.post('/createOrderReturnLine/',tags=["driver"])
async def createOrderReturn(order_id: str,product_id: str, quantity: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        order_line_id  = await conn_adempiere.createReturnOrderLine(order_id, product_id, quantity)
        if order_line_id is not None:
            return {'record_id':order_line_id}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_adempiere.closeConnection()

@router.post('/updateInvoiceConfirm/{c_invoice_id}',tags=["driver"])
async def createOrderReturn(c_invoice_id: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        order_return_id  = await conn_adempiere.UpdateInvoiceConfirm(c_invoice_id)
        if order_return_id is not None:
            return {'record_id':order_return_id}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_adempiere.closeConnection()

@router.post('/updateDeliveryConfirm/{invoice_id}',tags=["driver"])
async def createOrderReturn(invoice_id: str):
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        listDeliveryIds = conn_adempiere.getDelivery(invoice_id)

        if listDeliveryIds is None:
            return HTTPException(status_code=404, detail="Error, no se en contraron Entregas") 

        if listDeliveryIds is not None:
            
            for delivery_id in listDeliveryIds:

                confirmDelivery_ID  = await conn_adempiere.UpdateDeliveryConfirm(delivery_id)

                if confirmDelivery_ID is not None:
                    print(f'se confirmo  la entrega: {delivery_id}')
                
                if confirmDelivery_ID is None:
                    print(f'No se pudo confirmar la entrega: {delivery_id}')

            return  {'status':'Entregas confirmadas'}
                
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos")
        
    finally:
        conn_adempiere.closeConnection()

@router.post('/createAuthorizationReturn/', tags=["driver"])
async def authotizationReturn(order_line: AuthorizationOrder):
    #quedaste en guardar crear la funcion que crea la autorizacion en la tabla que tambien tienes que crear la tabla en Nevada
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = await conn_nevada.insertAuthorization(order_line)
        if result is not None:
            return {'estatus: ':result}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_nevada.closeConnection()

@router.get('/updateAuthorization/{response}/{order_id}/{product_id}', tags=["driver"])
async def updateAuthorization(response:str, order_id: str, product_id: str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = await conn_nevada.updateAuthorization(order_id, response, product_id)
        if result is not None:
            return {'estatus: ':result}
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_nevada.closeConnection()
    

@router.get('/getAuthorizations/{chofer_id}', tags=["driver"])
async def getAuthorizations(chofer_id:str):
    try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        result  = await conn_nevada.getAuthorizations(chofer_id)
        if result is not None:
            return result
        else:
            return HTTPException(status_code=404, detail="Error al insertar datos") 
    finally:
        conn_nevada.closeConnection()


#ruta que obtiene las ubicaciones de todos los choferes en el dia de hoy, vehiculos, en transito y no disponibles
@router.get('/locations',tags=["driver"])
def get_locations(current_user: Annotated[User, Depends(get_current_user)]):
     try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        ubications  = conn_nevada.getLastLocations()
        if ubications is None:
            return HTTPException(status_code=404, detail="Error al realizar consulta")  
        if ubications:
            try:
                conn_adempiere = AdempiereConnect()
                conn_adempiere.connect()
                
                for ubi in ubications:
                    status_vehicle = conn_adempiere.statusVehicle(ubi.placa)
                    if status_vehicle is None:
                        ubi.parada = 'No posee'
                        ubi.ticket_entrada = 'No posee'
                        ubi.ticket_entrada ='No posee'
                        ubi.fecha_ticket ='No posee'
                        ubi.orden_carga = 'No posee'
                        ubi.tipo_viaje = 'No posee'
                        ubi.estatus_vehicle ='No posee'

                    if  status_vehicle is not None:
                        ubi.parada = status_vehicle.parada
                        ubi.ticket_entrada = status_vehicle.ticket_entrada
                        ubi.fecha_ticket = status_vehicle.fecha_ticket
                        ubi.orden_carga = status_vehicle.orden_carga
                        ubi.tipo_viaje = status_vehicle.tipo_viaje
                        ubi.estatus_vehicle = status_vehicle.estatus_vehicle
                    print(ubi) 
                return ubications

            finally:
                conn_adempiere.closeConnection
               
     finally:
        conn_nevada.closeConnection()

@router.get('/Invoices/{driver_id}',tags=["driver"])
def get_OrdersDriver(driver_id: str):
        try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            invoices = conn_adempiere.getInvoicesDriver(driver_id)
            if invoices is not None:
                return invoices
            
            if invoices is None:
                return None

        finally:
            conn_adempiere.closeConnection

@router.get('/detailInvoices/{invoice_id}',tags=["driver"])
def get_OrdersDriver(invoice_id: str):
        try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            products = conn_adempiere.getDetailInvoice(invoice_id)
            if products is not None:
                return products
            
            if products is None:
                return None
        
        finally:
            conn_adempiere.closeConnection


@router.get('/returnOrder/',tags=["driver"])
def get_OrdersDriver():
        try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            invoices = conn_adempiere.getReturns()
            if invoices is not None:
                return invoices
            
            if invoices is None:
                return None
        finally:
            conn_adempiere.closeConnection

@router.get('/detailOrder/{order_id}',tags=["driver"])
def get_OrdersDriver(order_id: str):
        try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            products = conn_adempiere.getDetailReturn(order_id)
            if products is not None:
                return products
            
            if products is None:
                return None
        
        finally:
            conn_adempiere.closeConnection

@router.get('/driverTicket/{ticket}',tags=["driver"])
def get_OrdersDriver(ticket: str):
        try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            driver = conn_adempiere.driverTicket(ticket)
            if driver is not None:
                return driver
            if driver is None:
                return None
        finally:
            conn_adempiere.closeConnection


@router.get('/confirmInvoice/{invoice_id}', tags=["driver"])
def confirm_invoice(invoice_id: str):
    try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            response = conn_adempiere.confirmInvoice(invoice_id)
            if response is not None:
                return response
            
            if response is None:
                return HTTPException(status_code=404, detail="Error no se pudo confirmar la factura")
    finally:
        conn_adempiere.closeConnection

@router.get('/confirmDelivery/{invoice_id}', tags=["driver"])
def confirm_invoice(invoice_id: str):
    try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            listDeliveryIds = conn_adempiere.getDelivery(invoice_id)

            if listDeliveryIds is None:
                return HTTPException(status_code=404, detail="No se encontraron entregas")
            
            if listDeliveryIds is not None:
                response = conn_adempiere.confimDelivery(listDeliveryIds)

            if response is None:
                return HTTPException(status_code=404, detail="Error al confirmar las entregas")
            
            if response is not None:
                return response
    finally:
        conn_adempiere.closeConnection


@router.get('/confirmInvoiceDelivery/{invoice_id}/{delivery_id}',tags=["driver"])
def get_OrdersDriver(invoice_id: str, delivery_id: str):
        try:
            conn_adempiere = AdempiereConnect()
            conn_adempiere.connect()
            response = conn_adempiere.confirmInvoiceDelivery(invoice_id, delivery_id)
            if response is not None:
                return response
            
            if response is None:
                return None
        finally:
            conn_adempiere.closeConnection


@router.get('/location/specific/{placa}',tags=["driver"])
def get_locations(current_user: Annotated[User, Depends(get_current_user)], placa: str):
     try:
        conn_nevada = NevadaConnect()
        conn_nevada.connect()
        ubication  = conn_nevada.getLocationSpecific(placa)
        if ubication is None:
            return HTTPException(status_code=404, detail="Error al realizar consulta")
        if ubication:
            return ubication
        else:
            return HTTPException(status_code=404, detail="No autorizado") 
     finally:
        conn_nevada.closeConnection()