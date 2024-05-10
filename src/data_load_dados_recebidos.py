#Importações
import pandas as pd
import os
from fun_conn_db import connect_to_database
from sqlalchemy import  text
from dotenv import load_dotenv
from pathlib import Path


##Variaveis globais
load_dotenv()
DATABASE = os.getenv('DW')
ENGINE = connect_to_database(DATABASE)


def data_load(fk_key, table_name, file_path):
    dataset = pd.read_csv(file_path, sep='\t')

    with ENGINE.connect() as connection:
        for _, row in dataset.iterrows():
            select_verifica = f"SELECT {fk_key} FROM {table_name} WHERE {fk_key} = :fk_key"
            result = connection.execute(text(select_verifica), {'fk_key': row[fk_key]}).fetchall()

            if result: 
                pass
            else: 
                df = pd.DataFrame([row])
                df.to_sql(table_name, ENGINE, if_exists='append', index=False)
        
        connection.close()

        print(f'Carga concluida - {table_name}')

############
# devido a regra de mapeamento, carregar o banco nessa ordem
# dim_unidade
# dim_empresa_executante
# dim_conta_bancaria
# dim_conta_financeira
# depara_conta_financeira -- MANUAL na criação do banco
# dim_fornecedor
# dim_centro_custos 
# depara_centro_custos -- MANUAL na criação do banco
# fato_saldo_inicial
# fato_lancamento



if __name__ == "__main__": 

    ### Carga
    dict_carga = {
        'dim_unidade': ['ds_unidade', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\dim_unidade\dim_unidade_extractDate=2024_05_09.txt'],
        'dim_empresa_executante': ['pk_unidade', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\dim_empresa_executante\dim_empresa_executante_extractDate=2024_05_09.txt'],
        'dim_conta_bancaria': ['sk_empresa_exec', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\dim_conta_bancaria\dim_conta_bancaria_extractDate=2024_05_09.txt'],
        'dim_conta_financeira': ['sk_normal_conta_financeira',r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\dim_conta_financeira\dim_conta_financeira_extractDate=2024_05_09.txt'],
        'dim_fornecedor': ['cd_fornecedor', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\dim_fornecedor\dim_fornecedor_extractDate=2024_05_09.txt'],
        'dim_centro_custos': ['sk_normal_centro_custos', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\dim_centro_custos\dim_centro_custos_extractDate=2024_05_09.txt'],
        'fato_saldo_inicial': ['sk_conta_bancaria', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\fato_saldo\fato_saldo_extractDate=2024_05_09.txt'],
        'fato_lancamento': ['pk_lancamento', r'C:\Users\gabri\OneDrive\Documentos\Projetos\HData\Datalake\gold\dados_recebidos\fato_lancamento\fato_lancamento_extractDate=2024_05_09.txt']
    }

    for table_name, list in dict_carga.items():
        fk_key, file_path = list[0], list[1] 

        try: 
            data_load(fk_key, table_name, Path(file_path))
        except Exception as e:
            print(f"Ocorreu um erro ao carregar os dados para a tabela {table_name}: {str(e)}")

