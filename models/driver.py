from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from typing import Text, Optional, List
from datetime import datetime

class User(BaseModel):
    username: str
    code: str | None = None
    id: str | None = None
    password: str | None = None
    admin: bool | None = None
    plate_vehicle: str | None = None
    company: str | None = None

class Access(BaseModel):
    name: str
    is_raw_material: str
    is_terminate: str

class Polygon(BaseModel):
    name: str
    latitud1: str
    longitud1: str
    latitud2: str
    longitud2: str
    latitud3: str
    longitud3: str
    latitud4: str
    longitud4: str
    latitud5: str
    longitud5: str
    tp_point_control_id: str
    total_vehicles: str

class PositionPolygon(BaseModel):
    tp_position_point_control_id: str
    tp_point_control_id: str
    latitud: str
    longitud: str
   

class Company(BaseModel):
    company_id: str
    name: str
    latitud: str
    longitud: str

class Ticket(BaseModel):
    id: str
    code: str

class Invoice(BaseModel):
    documentoNo: str
    date: str
    invoiceID: str
    org: str
    businessPartner: str
    businessPartnerId: str
    ticket: str

class ReturnInvoice(BaseModel):
    invoiceID: str
    businessPartnerId: str
    product_id: str
    reason: str
    quantity: str

class OrderReturn(BaseModel):
    orderReturnID: str
    documentNo: str
    name_org: str
    name_bp: str
    dateordered: str
    
class Product(BaseModel):
    name: str
    product_id: str
    quantity: float
   
class Vehicle(BaseModel):
     plate: str

class Ubication(BaseModel):
    cod_socio: str
    nombre_socio: str | None = None
    parada: str | None = None
    ticket_entrada: str | None = None
    fecha_ticket: str | None = None
    orden_carga:  str | None = None
    tipo_viaje:  str | None = None
    estatus_vehicle: str | None = None
    placa: str 
    latitud:str
    longitud: str

class StatusVehicle(BaseModel):
    placa: str | None = None
    parada: str | None = None
    ticket_entrada: str | None = None
    fecha_ticket: str | None = None
    orden_carga:  str | None = None
    tipo_viaje:  str | None = None
    estatus_vehicle: str | None = None
    status_parada: str | None = None
    registro_peso: str | None = None
    esPropio: str | None = None
    dateScanner: str | None = None
    tp_point_control_id: str | None = None
    guia_insai: str | None = None
    turno: str | None = None
    detalle_guia: str | None = None
    nombre_productor: str | None = None
    es_materia_prima: bool | None = None


class InfoScanner(BaseModel):
    id_acceso: str | None = None
    codigo: str | None = None
    fecha_hora: str | None = None
    entrada_scanner: str | None = None
    tipo:  str | None = None
    turno:  str | None = None
    id_equipo: str | None = None
    foto: str | None = None
    id_estatus: str | None = None
    id_tracking: str | None = None
    nombre_scanner: str | None = None
    compania: str | None = None
    detalle_guia: str | None = None
    nombre_productor: str | None = None
   
   
class Planning(BaseModel):
    punto_escaner: str | None = None
    

class StatusScanner(BaseModel):
    status_scanner: str | None = None
    date_created: str | None = None
    time_spent: str | None = None

class CreateReturn(BaseModel):
    driver_id: str 
    name_driver: str 
    c_bpartner_id: str

class AuthorizationOrder(BaseModel):
    order_id: str 
    documentoNo:str
    name_item:str
    businessPartner:str
    product_id: str 
    quantity: str
    reason: str
    status: str | None = None
    date_create: str | None = None
    chofer_id: str 

class ReturnLine(BaseModel):
    order_id: str 
    product_id: str 
    quantity: str 
    