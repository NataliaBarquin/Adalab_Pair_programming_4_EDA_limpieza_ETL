import pandas as pd
import mysql.connector

import requests
from datetime import datetime, timedelta

import soporte as sp

respuesta = input("¿Quieres crear la base de datos?")

if respuesta.lower() == "si":

    db = Creacion_bbdd("AlumnaAdalab", "energy2")

    db.crear_bbdd()

    db.crear_tablas()

    df = ETL_energia(2021, 2022, "AlumnaAdalab", "energy2")

    df_energia = df.energy_spain()

    df_ccaa = df.energy_location()

    df.clean(df_energia)
    
    df.clean(df_ccaa)

    df.load_fechas()

    df.load_nacional()

    df.load_comunidades()

    df.export(df_energia, "prueba_nacional")

    df.export(df_ccaa, "prueba_ccaa")

else:

    