# Importando bibliotecas
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import os


class TratamentoGold(): 
    def __init__(self, file_path, output_folder, date): 
        self.file_path = file_path
        self.output_folder = output_folder
        self.date = date

    
    def lendo_dados(self):
        ######
        # Lendo dados do arquivo
        ######
        dataset = pd.read_csv(self.file_path, sep='\t')

        # Retornando dataset
        return dataset
    
    def dataset_lancamento(self): 
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando as colunas do mapa de aderencia
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

        fato_lancamento = dataset[select_column]


        ## Dropando nulos da coluna sk_conta_bancaria
        fato_lancamento['sk_conta_bancaria'] = fato_lancamento['sk_conta_bancaria'].fillna(1)


        ## Tipagem dos dados
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

        fato_lancamento = fato_lancamento.astype(columns_type)


        ## Caminho de saida
        folder_file_output = os.path.join('fato_lancamento', f'fato_lancamento_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)

        return fato_lancamento, output_path


    def dataset_fornecedor(self): 
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas do mapa de aderencia
        select_column = [
            'cd_fornecedor',
            'ds_fornecedor'
        ]

        dim_fornecedor = dataset[select_column]


        #Dropando duplicadas
        dim_fornecedor.drop_duplicates(inplace=True)


        ## Caminho de saida
        folder_file_output = os.path.join('dim_fornecedor', f'dim_fornecedor_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)

        return dim_fornecedor, output_path

    
    def dataset_conta_financeira(self): 
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas do mapa de aderencia
        select_column = [
            'cd_conta_financeira',
            'ds_conta_financeira',
            'sk_normal_conta_financeira',
            'ds_normal_conta_financeira'
        ]

        dim_conta_financeira = dataset[select_column]


        # Tipagem de dados
        type_conta_financeira = {
            'cd_conta_financeira': int,
            'ds_conta_financeira': object,
            'sk_normal_conta_financeira': int,
            'ds_normal_conta_financeira': object
        }

        dim_conta_financeira = dim_conta_financeira.astype(type_conta_financeira)


        # Sorteando por chave primaria
        dim_conta_financeira.sort_values('sk_normal_conta_financeira', inplace=True)


        # Dropando dupliacadas
        dim_conta_financeira.drop_duplicates(inplace=True)


        ## Caminho de saida
        folder_file_output = os.path.join('dim_conta_financeira', f'dim_conta_financeira_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)


        return dim_conta_financeira, output_path


    def dataset_de_para_conta_financeira(self): 
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas do mapeamento de aderencia
        select_column = [
            'ds_normal_conta_financeira',
            'sk_normal_conta_financeira'
        ]

        depara_conta_financeira = dataset[select_column]


        # Dropando duplicadas e sorteando por chave unica e fazer a tipagem dos dados
        depara_conta_financeira.drop_duplicates(inplace=True)
        depara_conta_financeira.sort_values('sk_normal_conta_financeira', inplace=True)
        select_column_depara_conta = {
            'ds_normal_conta_financeira': object,
            'sk_normal_conta_financeira': int
        }

        depara_conta_financeira = depara_conta_financeira.astype(select_column_depara_conta)


        ## Caminho de saida
        folder_file_output = os.path.join('depara_conta_financeira', f'depara_conta_financeira_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)

        return depara_conta_financeira, output_path
    

    def dataset_cdc(self): 
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas do mapeamento de aderencia
        select_column = [
            'ds_centro_custo',
            'cd_centro_custos',
            'sk_normal_centro_custos'

        ]

        dim_centro_custos = dataset[select_column]


        ## Sorteando por chave primaria
        dim_centro_custos.sort_values('sk_normal_centro_custos', inplace=True)


        ## Dropando duplicadas
        dim_centro_custos.drop_duplicates(inplace=True)


        ## Caminho de saida
        folder_file_output = os.path.join('dim_centro_custos', f'dim_centro_custos_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)


        return dim_centro_custos, output_path

    def dataset_de_para_cdc(self): 
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas do mapeamento de aderencia
        select_column = [
            'sk_normal_centro_custos',
            'ds_centro_custo'
        ]

        depara_centro_custos = dataset[select_column]


        ## Sorteando dados por chave primaria
        depara_centro_custos.sort_values('sk_normal_centro_custos', inplace=True)

        
        ## Dropando duplicadas
        depara_centro_custos.drop_duplicates(inplace=True)


        ## Caminho de saida
        folder_file_output = os.path.join('depara_centro_custos', f'depara_centro_custos_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)


        return depara_centro_custos, output_path


    def dataset_unidade(self):
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas do mapeamento de aderencia
        select_column = [
            'ds_unidade',
            'sk_unidade'
        ]
        dim_unidade = dataset[select_column]


        ## Dropando duplicadas
        dim_unidade.drop_duplicates(inplace=True)


        ## Caminho de saida
        folder_file_output = os.path.join('dim_unidade', f'dim_unidade_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)


        return dim_unidade, output_path


    def dataset_fato_saldo(self):
        ## Iniciando o dataset
        dataset = self.lendo_dados()


        ## Selecionando colunas
        select_columns = [
            'dt_saldo_inicial',
            'vl_saldo_inicial',
            'sk_conta_bancaria',
            'ds_conta_bancaria',
            'cd_conta_bancaria',
            'cd_agencia'
        ]

        fato_saldo = dataset[select_columns]


        ## Dropando duplicadas e nulas
        fato_saldo.drop_duplicates(inplace=True)
        fato_saldo.dropna(inplace=True)


        ## Tipagem dos dados
        type_conta = {
            'dt_saldo_inicial': 'datetime64[ns]',
            'vl_saldo_inicial': float,
            'sk_conta_bancaria': int,
            'ds_conta_bancaria': object,
            'cd_conta_bancaria': int,
            'cd_agencia': int
        }

        fato_saldo = fato_saldo.astype(type_conta)


        ## Coluna codigo do banco
        fato_saldo['cd_banco'] = pd.factorize(fato_saldo['cd_conta_bancaria'])[0] +1
        fato_saldo['cd_banco'] = fato_saldo['cd_banco'].replace(0, np.nan)


        # Coluna sn_ativo
        fato_saldo['sn_ativo'] = True


        # Sorteando por chave primaria
        fato_saldo.sort_values('sk_conta_bancaria', inplace=True)


        ## Caminho de saida
        folder_file_output = os.path.join('fato_saldo', f'fato_saldo_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)


        return fato_saldo, output_path


    def dataset_conta_bancaria(self):
        ## Iniciando o dataset de fato saldo
        fato_saldo, _ = self.dataset_fato_saldo()


        ## Selecionando colunas do mapeaento
        select_column = [
            'sk_conta_bancaria',
            'cd_conta_bancaria',
            'cd_agencia',
            'ds_conta_bancaria',
            'cd_banco',
            'sn_ativo'
        ]

        dim_conta_bancaria = fato_saldo[select_column]


        ## Caminho de saida
        folder_file_output = os.path.join('dim_conta_bancaria', f'dim_conta_bancaria_extractDate={self.date}.txt')
        output_path = os.path.join(self.output_folder, folder_file_output)


        return dim_conta_bancaria, output_path

    
    ## Função de exportação
    def export_gold_files(self, dataset, folder, output_path): 
        if  not os.path.isdir(folder): 
            Path(folder).mkdir(parents=True, exist_ok=True)

        dataset.to_csv(output_path, sep='\t', index=False)

    
    def execute(self): 
        ## Fato_lancamento
        dataset_fato_lancamento, output_lancamento = self.dataset_lancamento()
        self.export_gold_files(dataset_fato_lancamento, os.path.dirname(output_lancamento), output_lancamento)

        ## Fornecedor
        dataset_fornecedor, output_fornecedor = self.dataset_fornecedor()
        self.export_gold_files(dataset_fornecedor, os.path.dirname(output_fornecedor), output_fornecedor)

        ## conta_financeira
        dataset_conta_financeira, output_conta_financeira = self.dataset_conta_financeira()
        self.export_gold_files(dataset_conta_financeira, os.path.dirname(output_conta_financeira), output_conta_financeira)

        ## de_para_conta_financeira
        dataset_de_para_conta_financeira, output_de_para_conta_financeira = self.dataset_de_para_conta_financeira()
        self.export_gold_files(dataset_de_para_conta_financeira, os.path.dirname(output_de_para_conta_financeira), output_de_para_conta_financeira)

        ## cdc
        dataset_cdc, output_cdc = self.dataset_cdc()
        self.export_gold_files(dataset_cdc, os.path.dirname(output_cdc), output_cdc)

        ## de_para_cdc
        dataset_de_para_cdc, output_de_para_cdc = self.dataset_de_para_cdc()
        self.export_gold_files(dataset_de_para_cdc, os.path.dirname(output_de_para_cdc), output_de_para_cdc)


        ## unidade
        dataset_unidade, output_unidade = self.dataset_unidade()
        self.export_gold_files(dataset_unidade, os.path.dirname(output_unidade), output_unidade)

        ## fato_saldo
        dataset_fato_saldo, output_fato_saldo = self.dataset_fato_saldo()
        self.export_gold_files(dataset_fato_saldo, os.path.dirname(output_fato_saldo), output_fato_saldo)

        ## conta_bancaria
        dataset_conta_bancaria, output_conta_bancaria = self.dataset_conta_bancaria()
        self.export_gold_files(dataset_conta_bancaria, os.path.dirname(output_conta_bancaria), output_conta_bancaria)

        return True



if __name__ == "__main__": 

    ## Arquivo silver
    BASEFOLDER = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Datalake\{stage}\{folder}'
    today = datetime.now()
    date = today.strftime('%Y_%m_%d')
    folder_path = BASEFOLDER.format(stage='silver', folder='dados_recebidos')
    file_name = f'dados_recebidos_extractDate={date}.csv'
    file_path = os.path.join(folder_path, file_name)

    
    ## Output
    BASEFOLDER_DOIS = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Datalake\{stage}'
    output_folder = BASEFOLDER_DOIS.format(stage='gold')
    

    
    tratamento_gold = TratamentoGold(file_path, output_folder, date)
    executar = tratamento_gold.execute()


    if not executar == True: 
        print('Erro')
    else: 
        print('Deu tudo certo')