from move_bronze import move_to_bronze
from tratamento_silver import TratamentoSilverLancamentos
from tratamento_gold import TratamentoGold
from datetime import datetime
from dotenv import load_dotenv
import os
import sys


# Iniciando dotenv
load_dotenv()
# Variaveis globais
TODAY = datetime.now()
DATE = TODAY.strftime('%Y_%m_%d')
BASEFOLDER = os.getenv('BASEFOLDER')


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
tratamento_gold = TratamentoGold(silver_path_file, destination_gold, DATE)
gold = tratamento_gold.execute()


if not gold == True: 
    print('Erro no tratamento gold')
else: 
    print('Pipeline concluido com sucesso!')