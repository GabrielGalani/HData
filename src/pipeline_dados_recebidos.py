from move_bronze import move_to_bronze
from tratamento_silver import TratamentoSilverLancamentos
from tratamento_gold import TratamentoGold
from data_load_dados_recebidos import data_load
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os

# Iniciando dotenv
load_dotenv()
# Variaveis globais
TODAY = datetime.now()
DATE = TODAY.strftime('%Y_%m_%d')
BASEFOLDER = os.getenv('BASEFOLDER')
DW = os.getenv('DW')


# variaveis locais
path_folder = r"C:\Users\gabri\OneDrive\Documentos\Projetos\Case 1 - Dados Fin"
destination_bronze = os.path.join(BASEFOLDER, r'bronze\dados_recebidos')
destination_silver = os.path.join(BASEFOLDER, r'silver\dados_recebidos')
destination_gold = os.path.join(BASEFOLDER, r'gold\dados_recebidos')
file_name = f'dados_recebidos_extractDate={DATE}.csv'
silver_path_file = os.path.join(destination_silver, file_name)



######
# Movendo para cada bronze
######
move_to_bronze_ = move_to_bronze(path_folder, destination_bronze)

if not move_to_bronze_ == True: 
    print('Erro ao mover para camada bronze')


######
# Tratamento Silver
######
tratamento_silver = TratamentoSilverLancamentos(destination_bronze, destination_silver, silver_path_file)
silver = tratamento_silver.execute()


if not silver == True: 
    print('Erro ao realizar tratamento silver')


######
# Tratamento Silver
######
tratamento_gold = TratamentoGold(silver_path_file, destination_gold, DATE, DW)
gold = tratamento_gold.execute()


if not gold == True: 
    print('Erro no tratamento gold')
else: 
    print('Pipeline concluido com sucesso!')


###### 
# Carregando dados
######
dict_carga = {
    'dim_unidade': ['ds_unidade', r'dim_unidade'],
    'dim_empresa_executante': ['pk_unidade', r'dim_empresa_executante'],
    'dim_conta_bancaria': ['sk_empresa_exec', r'dim_conta_bancaria'],
    'dim_conta_financeira': ['sk_normal_conta_financeira',r'dim_conta_financeira'],
    'dim_fornecedor': ['cd_fornecedor', r'dim_fornecedor'],
    'dim_centro_custos': ['sk_normal_centro_custos', r'dim_centro_custos'],
    'fato_saldo_inicial': ['sk_conta_bancaria', r'fato_saldo'],
    'fato_lancamento': ['pk_lancamento', r'fato_lancamento']
}
for table_name, list in dict_carga.items():
    fk_key, folder_path = list[0], list[1] 
    folder = os.path.join(destination_gold, folder_path)
    for file in os.listdir(folder):
        if file.endswith(f'{DATE}.txt'):
            file_path = os.path.join(folder, file)
            try: 
                data_load(fk_key, table_name, file_path)
            except Exception as e:
                print(f"Ocorreu um erro ao carregar os dados para a tabela {table_name}: {str(e)}")