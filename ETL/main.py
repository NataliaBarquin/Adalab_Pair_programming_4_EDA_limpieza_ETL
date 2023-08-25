import pandas as pd
import mysql.connector

import requests
from datetime import datetime, timedelta

import soporte as sp

respuesta = input("¿Tiene ya la base de datos creada?")

if respuesta.lower() == "no":

    db = sp.Creacion_bbdd("AlumnaAdalab", "energy")

    db.crear_bbdd()

    db.crear_tablas()

    df = sp.ETL_energia(2018, 2019, "AlumnaAdalab", "energy")

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

    df = sp.ETL_energia(2018, 2019, "AlumnaAdalab", "energy")

    df_energia = df.energy_spain()

    df_ccaa = df.energy_location()

    df.clean(df_energia)
    
    df.clean(df_ccaa)

    df.load_fechas(df_energia)

    df.load_nacional(df_energia)

    df.load_comunidades(df_ccaa)

    df.export(df_energia, "prueba_nacional")

    df.export(df_ccaa, "prueba_ccaa")