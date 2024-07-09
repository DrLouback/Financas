import pandas as pd
import yfinance as yf
import sqlalchemy
from dotenv import load_dotenv
import os

load_dotenv()

# Configurações de conexão com o banco de dados
DB_HOST = 'dpg-cq69l3ss1f4s73duqalg-a.oregon-postgres.render.com'
DB_PORT = '5432'
DB_NAME = 'financedb_lpoj'
DB_USER = 'user'
DB_PASS = 'TGf3S03GatiPdmXAn1xzXicuoaeSvH2G'


DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = sqlalchemy.engine.create.create_engine(DATABASE_URL)

commodities = ['CL=F', 'GC=F', 'SI=F']


def buscar_dados_commodities(símbolos, período='3mo', intervalo='1d'):
    ticker = yf.Ticker(símbolos)
    dados = ticker.history(period=período, interval=intervalo)[['Close']]
    dados['símbolo'] = símbolos
    return dados


def buscar_todas_commodities(commodities):
    todos_dados = []
    for símbolos in commodities:
        dados = buscar_dados_commodities(símbolos)
        print(f'Buscando dados {dados}')
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_no_postgres(df, schema):
    df.to_sql('commodities', engine, schema, index = True, index_label= True, if_exists = 'replace')
    print('salvando no banco ...')


if __name__ == '__main__':
    dados_concat = buscar_todas_commodities(commodities)
    salvar_no_postgres(dados_concat, 'public')
    
  
