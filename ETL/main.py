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
    
    start_year = int(input("¿Desde qué año quieres exportar?"))
    end_year = int(input("¿Hasta qué año quieres exportar?"))

    df = sp.ETL_energia(start_year, end_year, "AlumnaAdalab", "energy")

    df_energia = df.energy_spain()

    df_ccaa = df.energy_location()

    df.clean(df_energia)
    
    df.clean(df_ccaa)

    df.load_fechas(df_energia)
    
    print("Exportación de fechas a SQL completada con éxito")

    df.load_nacional(df_energia)
    
    print("Exportación de renovables_nacional  a SQL completada con éxito")

    df.load_comunidades(df_ccaa)
    
    print("Exportación de renovables_comunidades a SQL completada con éxito")

    df.export(df_energia, "prueba_nacional")

    df.export(df_ccaa, "prueba_ccaa")
    
    print("Guardado de los dataframes completado con éxito")

else:

    start_year = input("¿Desde qué año quieres exportar?")
    end_year = input("¿Hasta qué año quieres exportar?")

    df = sp.ETL_energia(start_year, end_year, "AlumnaAdalab", "energy")

    df_energia = df.energy_spain()

    df_ccaa = df.energy_location()

    df.clean(df_energia)
    
    df.clean(df_ccaa)

    df.load_fechas(df_energia)
    
    print("Exportación de fechas a SQL completada con éxito")

    df.load_nacional(df_energia)
    
    print("Exportación de renovables_nacional  a SQL completada con éxito")

    df.load_comunidades(df_ccaa)
    
    print("Exportación de renovables_comunidades a SQL completada con éxito")

    df.export(df_energia, "prueba_nacional")

    df.export(df_ccaa, "prueba_ccaa")
    
    print("Guardado de los dataframes completado con éxito")