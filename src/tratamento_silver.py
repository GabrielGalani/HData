# Importações
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import os

class TratamentoSilverLancamentos():

    def __init__(self, folder_path, output_folder, output_path):

        self.folder_path = folder_path
        self.output_folder = output_folder
        self.output_path = output_path

    
    def montando_dataframe(self):
        #########
        # Lendo arquivo e criando o dataset
        ########

        dataset_lancamento = pd.DataFrame()
        dataset_saldo = pd.DataFrame()
        dataset_cdc = pd.DataFrame()
        dataset_contas = pd.DataFrame()

        for file in os.listdir(folder_path):
            if file.endswith('xlsx'):
                # Carrega o arquivo Excel
                xls = pd.ExcelFile(os.path.join(folder_path,file))

                # Criando os datasets
                dataset_lancamento = pd.read_excel(os.path.join(folder_path,file), sheet_name=xls.sheet_names[0])
                dataset_saldo = pd.read_excel(os.path.join(folder_path,file), sheet_name=xls.sheet_names[1])
                dataset_cdc = pd.read_excel(os.path.join(folder_path,file), sheet_name=xls.sheet_names[2])
                dataset_contas = pd.read_excel(os.path.join(folder_path,file), sheet_name=xls.sheet_names[3])

        return dataset_saldo, dataset_lancamento, dataset_cdc, dataset_contas
    
    def tratando_dataframes(self):
        # Montando dataframes
        dataframes = self.montando_dataframe()
        dataset_lancamento = dataframes[1]


        ########
        # Tratando dataset saldo
        ########
        dataset_saldo = dataframes[0]

        rename_saldo = {
            'CONTA BANCARIA (REF)': 'ds_conta_bancaria',
            'AGENCIA': 'cd_agencia',
            'CONTA': 'cd_conta_bancaria',
            'DATA SALDO INICIAL': 'dt_saldo_inicial',
            'SALDO INICIAL': 'vl_saldo_inicial'
        }

        dataset_saldo.rename(columns=rename_saldo, inplace=True)


        #######
        # Tratando dataset cdc
        #######
        dataset_cdc = dataframes[2]


        rename_cdc = {
            'ds_normal_centro_custo': 'ds_normal_centro_custos',
            'sk_normal_centro_custo': 'sk_normal_centro_custos',
            'ds_centro_custo': 'ds_centro_custo'
        }
        dataset_cdc.rename(columns=rename_cdc, inplace=True)


        # Organizando df
        dataset_cdc = dataset_cdc.sort_values('sk_normal_centro_custos')

        
        # Criando a coluna cd_centro_custos
        dataset_cdc['cd_centro_custos'] = range(1, len(dataset_cdc)+1)



        ########
        # Tratando Contas Financeiras
        ########
        dataset_contas = dataframes[3]


        # Reordenando dataset para criar chave
        dataset_contas.sort_values('sk_normal_conta_financeira', inplace=True)


        # Criando chave única
        dataset_contas['cd_conta_financeira'] = range(1, len(dataset_contas)+1)

        
        ## Criando coluna de débito e crédito baseado na regra de contas
        # Definindo a condição
        condicao = (
            (dataset_contas['ds_normal_conta_financeira'].str.contains('RECEITA', case=False)) & (dataset_contas['sk_normal_conta_financeira'] != 5) |
            (dataset_contas['sk_normal_conta_financeira'].isin([1, 2])) & (dataset_contas['ds_conta_financeira'].str.contains('ENTRADA') | dataset_contas['ds_conta_financeira'].str.contains('SALDO'))
        )

        # Criando a coluna debito e crédito
        dataset_contas['ds_unidade'] = np.where(condicao, 'C', 'D')

        #######
        # Tratando dataset fornecedores
        #######

        dataset_fornecedor = dataset_lancamento['FORNECEDOR']
        dataset_fornecedor = dataset_fornecedor.to_frame().reset_index()
        dataset_fornecedor['FORNECEDOR'] = dataset_fornecedor['FORNECEDOR'].drop_duplicates()
        dataset_fornecedor.dropna(inplace=True)
        dataset_fornecedor = dataset_fornecedor.drop('index', axis=1)
        dataset_fornecedor['sk_fornecedor'] = range(1, len(dataset_fornecedor)+1)
        dataset_fornecedor['cd_fornecedor'] = dataset_fornecedor['sk_fornecedor']
        dataset_fornecedor.rename(columns={'FORNECEDOR': 'ds_fornecedor'}, inplace=True)
        dataset_fornecedor.head()


        #######
        # Tratando dataset lançamentos
        #######

        # Renomeadno colunas
        rename_lancamento = {
            'CONTA': 'ds_conta_financeira',
            'ID': 'pk_lancamento',
            'DATA VENCIMENTO': 'dt_vencimento',
            'MÊS REFERÊNCIA': 'dt_mes_referencia',
            'VALOR TOTAL': 'vl_total_nf',
            'VALOR PARCELA': 'vl_parcela',
            'FORNECEDOR': 'ds_fornecedor',
            'DATA PAGAMENTO': 'dt_realizado',
            'VALOR REALIZADO': 'vl_realizado',
            'CONSIDERAÇÕES LANÇAMENTO': 'ds_observacao',
            'NOTA FISCAL': 'cd_nf_doc',
            'STATUS': 'tp_situacao',
            'CENTRO DE CUSTO': 'ds_normal_centro_custos',
            'FORMA DE PAGAMENTO': 'tp_forma_pgto',
            'CONTA BANCARIA': 'ds_conta_bancaria'
        }
        dataset_lancamento.rename(columns=rename_lancamento, inplace=True)

        # Criando coluna cd_lancamento
        dataset_lancamento['cd_lancamento'] = dataset_lancamento['pk_lancamento']


        # Mesclando com left o dataset co o dataset de contas
        dataset_lancamento = dataset_lancamento.merge(dataset_contas, how='left', on='ds_conta_financeira')


        # Criando a coluna_sk_unidade
        dataset_lancamento['sk_unidade'] = pd.factorize(dataset_lancamento['ds_unidade'])[0] +1
        dataset_lancamento['sk_unidade'] = dataset_lancamento['sk_unidade'].replace(0, np.nan)


        # Criando a coluna sk_conta_financeira
        dataset_lancamento['sk_conta_financeira'] = dataset_lancamento['cd_conta_financeira']


        ## Dropando linhas completamente nulas
        dataset_lancamento= dataset_lancamento.dropna(how='all')


        # Unindo com dataset de fornecedores
        dataset_lancamento = dataset_lancamento.merge(dataset_fornecedor, how='left', on='ds_fornecedor')


        # Criando a coluna de data de vencimento
        dataset_lancamento['dt_venc_parcela'] = dataset_lancamento['dt_vencimento']


        # Criando colunas vazias mapeadas no fluxo de caixa
        dataset_lancamento['dt_emissao_nf_doc'] = ''
        dataset_lancamento['dt_inclusao'] = ''
        dataset_lancamento['sk_conta_bancaria'] = 0
        dataset_lancamento['cd_parcela'] = 0
        dataset_lancamento['vl_juros'] = 0.0
        dataset_lancamento['vl_descontos'] = 0.0
        dataset_lancamento['cd_documento'] = ''
        dataset_lancamento['tp_lancamento'] = ''


        # Unindo com centro de custos
        dataset_lancamento = dataset_lancamento.merge(dataset_cdc, how='left', on='ds_normal_centro_custos')

        ######
        # Criando dataset finalizado
        ######
        select_column = [
                 'sk_unidade', 
                 'pk_lancamento',
                 'cd_lancamento',
                 'sk_conta_financeira',
                 'dt_vencimento',
                 'dt_mes_referencia',
                 'vl_total_nf',
                 'dt_venc_parcela',
                 'sk_fornecedor',
                 'dt_realizado',
                 'vl_realizado',
                 'ds_observacao',
                 'dt_emissao_nf_doc',
                 'dt_inclusao',
                 'cd_nf_doc',
                 'sk_conta_bancaria',
                 'cd_parcela',
                 'vl_juros',
                 'vl_descontos',
                 'tp_lancamento',
                 'sk_normal_centro_custos',
                 'tp_forma_pgto',
                 'cd_documento',
                 'tp_situacao'
        ]

        dataset_final = dataset_lancamento[select_column]


        ## COletando colunas das dimensões
        dataset_colunas_fora = dataset_lancamento.columns.difference(select_column)
        dataset_fora = dataset_lancamento[dataset_colunas_fora]


        # Unindo dataset conta bancaria
        dataset_fora = dataset_fora.merge(dataset_saldo, how='left', on='ds_conta_bancaria')


        ########
        # Dataset output
        ########
        dataset_final = pd.concat([dataset_final, dataset_fora], axis=1)



        #######
        # Tipagem de dados
        #######
        columns_type = {
            'sk_unidade': int, 
            'pk_lancamento': int,
            'cd_lancamento': int, 
            'sk_conta_financeira': int,
            'dt_vencimento': 'datetime64[ns]',
            'dt_mes_referencia': 'datetime64[ns]',
            'vl_total_nf': float, 
            'dt_venc_parcela': 'datetime64[ns]', 
            'sk_fornecedor': int, 
            'dt_realizado': 'datetime64[ns]', 
            'vl_realizado': float, 
            'ds_observacao': object, 
            'dt_emissao_nf_doc': 'datetime64[ns]', 
            'dt_inclusao': 'datetime64[ns]', 
            'cd_nf_doc': object, 
            'sk_conta_bancaria': int, 
            'cd_parcela': int, 
            'vl_juros': float, 
            'vl_descontos': float, 
            'tp_lancamento': object, 
            'sk_normal_centro_custos': int, 
            'tp_forma_pgto': object, 
            'cd_documento': object, 
            'tp_situacao': object
        }

        dataset_final = dataset_final.astype(columns_type)

        return dataset_final

    def execute(self):
        dataset_final = self.tratando_dataframes()

        if  not os.path.isdir(self.output_folder): 
            Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        
        dataset_final.to_csv(self.output_path, sep='\t')

if __name__ == "__main__": 
    BASEFOLDER = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Datalake\{stage}\{folder}'
    folder_path = BASEFOLDER.format(stage='bronze', folder='dados_recebidos')
    output_folder = BASEFOLDER.format(stage='silver', folder='dados_recebidos')
    today = datetime.now()
    date = today.strftime('%Y_%m_%d')
    file_name = f'dados_recebidos_extractDate={date}.csv'
    output_path = os.path.join(output_folder, file_name)


    tratamento_silver = TratamentoSilverLancamentos(folder_path=folder_path, output_folder=output_folder, output_path=output_path)
    tratamento_silver.execute()