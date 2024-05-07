from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd 

def connect_to_database(database):
    try:
        connection_string = f'mssql+pyodbc://localhost/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
        engine = create_engine(connection_string)
        return engine 
    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None


if __name__ == "__main__": 
    load_dotenv()
    database = os.getenv('DW')

    teste = connect_to_database(database)

    if teste == None: 
        print('erro')
    else: 
        print('sucesso')