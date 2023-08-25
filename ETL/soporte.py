import pandas as pd
import mysql.connector

import requests
from datetime import datetime, timedelta

class Creacion_bbdd:

    def __init__(self, password, db_name): 
        
        self.password = password
        self.db_name = db_name

    def crear_bbdd(self):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password= f"{self.password}",
        auth_plugin = 'mysql_native_password') 
        print("Conexión realizada con éxito")
        
        mycursor = mydb.cursor()

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name};")
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

    def crear_tablas(self):
        
        cnx = mysql.connector.connect(host="localhost", user="root", password= f"{self.password}", database=f"{self.db_name}", auth_plugin = 'mysql_native_password') 

        mycursor = cnx.cursor()

        try: 
            mycursor.execute(f""" CREATE TABLE IF NOT EXISTS `{self.db_name}`.`fechas` (
                `id_date` INT NOT NULL AUTO_INCREMENT,
                `date` DATE NOT NULL,
                PRIMARY KEY (`id_date`))
                ENGINE = InnoDB;
                
                CREATE TABLE IF NOT EXISTS `{self.db_name}`.`comunidades` (
                `id_location` INT NOT NULL,
                `location` VARCHAR(45) NOT NULL,
                PRIMARY KEY (`id_location`))
                ENGINE = InnoDB;
                
                CREATE TABLE IF NOT EXISTS `{self.db_name}`.`comunidades_renovable_no_renovable` (
                `id_comunidades_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
                `value` FLOAT NULL,
                `percentage` FLOAT NULL,
                `energy_type` VARCHAR(45) NULL,
                `comunidades_id_location` INT NOT NULL,
                `fechas_id_date1` INT NOT NULL,
                PRIMARY KEY (`id_comunidades_renovable_no_renovable`),
                INDEX `fk_comunidades_renovable_no_renovable_comunidades1_idx` (`comunidades_id_location` ASC) VISIBLE,
                INDEX `fk_comunidades_renovable_no_renovable_fechas1_idx` (`fechas_id_date1` ASC) VISIBLE,
                CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
                FOREIGN KEY (`comunidades_id_location`)
                REFERENCES `{self.db_name}`.`comunidades` (`id_location`)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION,
                CONSTRAINT `fk_comunidades_renovable_no_renovable_fechas1`
                FOREIGN KEY (`fechas_id_date1`)
                REFERENCES `{self.db_name}`.`fechas` (`id_date`)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION)
                ENGINE = InnoDB;
                
                CREATE TABLE IF NOT EXISTS `{self.db_name}`.`nacional_renovable_no_renovable` (
                `idnacional_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
                `value` FLOAT NULL,
                `percentage` FLOAT NULL,
                `energy_type` VARCHAR(45) NULL,
                `fechas_id_date` INT NOT NULL,
                PRIMARY KEY (`idnacional_renovable_no_renovable`),
                INDEX `fk_nacional_renovable_no_renovable_fechas1_idx` (`fechas_id_date` ASC) VISIBLE,
                CONSTRAINT `fk_nacional_renovable_no_renovable_fechas1`
                FOREIGN KEY (`fechas_id_date`)
                REFERENCES `{self.db_name}`.`fechas` (`id_date`)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION)
                ENGINE = InnoDB;""")
            
            cnx.commit() 

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

#creacion de tabla comunidades:

        cod_location = {"Ceuta": 8744, "Melilla": 8745, "Andalucía": 4, "Aragón": 5, "Cantabria": 6, "Castilla - La Mancha": 7, "Castilla y León": 8, "Cataluña": 9, "País Vasco": 10,
                            "Principado de Asturias": 11, "Comunidad de Madrid": 13, "Comunidad Foral de Navarra": 14, "Comunitat Valenciana": 15, "Extremadura": 16, "Galicia": 17,
                            "Illes Balears": 8743, "Canarias": 8742, "Región de Murcia": 21, "La Rioja": 20}

        df_localidades = pd.DataFrame(pd.Series(cod_location)).reset_index()

        for indice, fila in df_localidades.iterrows():

            try:
                mycursor.execute(f"""INSERT INTO comunidades (id_location, location) 
                        VALUES ("{fila[0]}", "{fila["index"]}");""")
                print(mycursor)

            except mysql.connector.Error as err:
                print(err)
                print("Error Code:", err.errno)
                print("SQLSTATE", err.sqlstate)
                print("Message", err.msg)


class ETL_energia:
    
    def __init__(self, start_year, end_year, password, db_name): 
        """Método constructor, recibe los siguientes parámetros:
            - start_year: año de inicio de la consulta
            - end_year: año fin de la consulta"""
        
        self.start_year = start_year
        self.end_year = end_year
        self.password = password
        self.db_name = db_name
        
    def energy_spain(self): 
        """Recibe los parámetros del método constructor. Llama a API Red Eléctrica de España
            Output: devuelve un dataframe con el consumo, fecha y tipo de energía consumida para todos los años consultados (renovable/no renovable) por cada día total de España"""

        df_spain = pd.DataFrame() # Creamos un dataframe vacío en el que se vayan uniendo nuestros resultados. 

        
        for year in range(self.start_year, self.end_year):

            url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day"
            
            response = requests.get(url)

            print(f"Año {year}: {response.status_code}")

            for i in range(len(pd.json_normalize(response.json()["included"]))):

                df = pd.json_normalize(response.json()["included"][i]["attributes"]["values"]) #Creamos un dataframe únicamente con los valores que nos interesan, que son "value", "percentage",
                # y "datetime"

                df["energy_type"] = response.json()["included"][i]["attributes"]["type"] # Creamos nueva columna con "tipo_energia", dejando fuera lo que no nos interesaba.

                df_spain = pd.concat([df_spain, df], axis = 0) # Hacemos un concat para que cada for se vaya añadiendo al DataFrame vacío creado anteriormente.
                
        return df_spain
    
    def energy_location(self):
        """Recibe los parámetros del método constructor. Llama a API Red Eléctrica de España
            Output: devuelve un dataframe con el consumo, fecha y tipo de energía consumida para todos los años consultados (renovable/no renovable) por cada año dividido por comunidades"""
        

        cod_location = {"Ceuta": 8744, "Melilla": 8745, "Andalucía": 4, "Aragón": 5, "Cantabria": 6, "Castilla - La Mancha": 7, "Castilla y León": 8, "Cataluña": 9, "País Vasco": 10,
                        "Principado de Asturias": 11, "Comunidad de Madrid": 13, "Comunidad Foral de Navarra": 14, "Comunitat Valenciana": 15, "Extremadura": 16, "Galicia": 17,
                        "Illes Balears": 8743, "Canarias": 8742, "Región de Murcia": 21, "La Rioja": 20}

        df_location = pd.DataFrame() #Creamos un dataframe vacío en el que se vayan uniendo nuestros resultados. 

        
        for year in range(self.start_year, self.end_year):

            for key, value in cod_location.items(): 

                url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=year&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={value}"
                
                response = requests.get(url)

                print(f"Año {year}, Comunidad {key}: {response.status_code}")

                for i in range(len(pd.json_normalize(response.json()["included"]))):

                    df2 = pd.json_normalize(response.json()["included"][i]["attributes"]["values"]) #Creamos un dataframe únicamente con los valores que nos interesan, que son "value", "percentage",
                    # y "datetime"

                    df2["energy_type"] = response.json()["included"][i]["attributes"]["type"]

                    df2["location"] = key

                    df2["id_location"] = value #Cremos columnas con otros elementos que nos interesaban de la API, dejando fuera lo que no nos interesaba.

                    df_location = pd.concat([df_location, df2], axis = 0) # Hacemos un concat para que cada for se vaya añadiendo al DataFrame vacío creado anteriormente.
                
        return df_location
    
    def clean(self, dataframe):
        """Recibe el siguiente parámetro:
            - dataframe: nombre del dataframe que se quiere limpiar 
            Output: devuelve el dataframe con:
                - las columnas "percentage" y "value" redondeadas a 2 decimales
                - la fecha en formato YYYY-MM-DD y tipo datetime
                - eliminando la columna inicial "datetime"""
        
        dataframe[["percentage", "value"]] = dataframe[["percentage", "value"]].apply(lambda element : element.round(2))
        
        dataframe["date"] = dataframe["datetime"].str.split("T", n = 1, expand = True).get(0).astype("datetime64[ns]")
        
        dataframe.drop("datetime", axis= 1, inplace= True)
        
        return dataframe
    
    def load_fechas(self, dataframe):

        fechas = pd.Series(dataframe["date"].unique())

        for fila in fechas:

            cnx = mysql.connector.connect(host="localhost", user="root", password=f"{self.password}", database=f"{self.db_name}", auth_plugin = 'mysql_native_password') 

            mycursor = cnx.cursor()

            try: 
                mycursor.execute(f"""
                            #INSERT INTO fechas (date)
                            #VALUES ('{fila}')""")
                cnx.commit() 

            except mysql.connector.Error as err:
                        print(err)
                        print("Error Code:", err.errno)
                        print("SQLSTATE", err.sqlstate)
                        print("Message", err.msg)


    

    def load_nacional(self, dataframe):

        for indice, fila in dataframe.iterrows():
    
            cnx = mysql.connector.connect(user='root', password=f'{self.password}',
                                    host='localhost', database=f"{self.db_name}",  auth_plugin = 'mysql_native_password')
            mycursor = cnx.cursor()

            try: 
                mycursor.execute(f"""SELECT id_date
                                FROM fechas WHERE date = '{fila["date"]}'""")
                id_date = mycursor.fetchall()[0][0]

                try: 
                    mycursor.execute(f"""
                            INSERT INTO nacional_renovable_no_renovable (value, percentage, energy_type, fechas_id_date) 
                            VALUES ({fila["value"]}, {fila["percentage"]}, "{fila["energy_type"]}", {id_date});
                            """)
                    cnx.commit() 

                except mysql.connector.Error as err:
                    print(err)
                    print("Error Code:", err.errno)
                    print("SQLSTATE", err.sqlstate)
                    print("Message", err.msg)

            except: 

                return "Sorry, no tenemos esa fecha en la BBDD y por lo tanto no te podemos dar su id. "
    
    def load_comunidades(self, dataframe):

        for indice, fila in dataframe.iterrows():
    
            cnx = mysql.connector.connect(user='root', password=f'{self.password}',
                                    host='localhost', database=f"{self.db_name}",  auth_plugin = 'mysql_native_password')
            mycursor = cnx.cursor()

            try: 
                mycursor.execute(f"""SELECT id_date
                                FROM fechas WHERE date = '{fila["date"]}'""")
                id_date = mycursor.fetchall()[0][0]

                try: 
                    mycursor.execute(f"""
                            INSERT INTO comunidades_renovable_no_renovable (value, percentage, energy_type, comunidades_id_location, fechas_id_date1) 
                            VALUES ({fila["value"]}, {fila["percentage"]}, "{fila["energy_type"]}", "{fila["id_location"]}", {id_date});
                            """)
                    cnx.commit() 

                except mysql.connector.Error as err:
                    print(err)
                    print("Error Code:", err.errno)
                    print("SQLSTATE", err.sqlstate)
                    print("Message", err.msg)
    
            except: 

                return "Sorry, no tenemos esa fecha en la BBDD y por lo tanto no te podemos dar su id. "

    def export(self, dataframe, path_name, format= "pkl"):
        """Recibe los siguientes parámetros:
            - dataframe: dataframe a guardar
            - path_name: ruta y nombre del archivo que se quiere guardar
            - format: formato en el que se quiere guardar, puede ser: pkl, csv o excel. Por defecto exporta a pkl.
            Output: dataframe exportado"""
    
        if format == "pkl":
            
            return dataframe.to_pickle(f"{path_name}.pkl")
        
        elif format == "csv":
            
            return dataframe.to_csv(f"{path_name}.csv")
            
        elif format == "excel":
            
            return dataframe.to_excel(f"{path_name}.xlsx")
        else:
            print("Formato incorrecto, selecciona 'pkl', 'csv' o 'excel'")
