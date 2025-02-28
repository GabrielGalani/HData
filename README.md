﻿# HData
# Hospital Estrela Data Engineering Project

## Descrição

### Situação
Sob nova direção desde agosto de 2023, o Hospital Estrela (HE) procurou a equipe da H:Data para apoiar seu processo de estruturação financeira. O time de Negócios da H:Data já possui aplicações de BI e painéis integrados ao seu Data Warehouse (DW), porém, requer a colaboração do time de Analistas de Dados para garantir que esses dados sejam facilmente consumíveis. Para otimizar o fluxo de trabalho, o time de Negócios elaborou o Mapa de Aderência referente ao dataset fornecido pelo HE.

### Case
Baseado no material do Case:
1. Coleta das informações da base de dados recebida;
2. Enriquecimento dos dados coletados a partir do Layout de Dados, das tabelas-bases da H:Data e do Mapa de Aderência;
3. Exportação dos dados enriquecidos em uma base única (tabelão);
4. Exportação dos dados enriquecidos na estrutura do DW (utilizando as tabelas e campos citados no Mapa de Aderência para modelar as tabelas de destino).


### Estrutura
<img src="Case 1 - Dados Fin\image.png">

### Bônus
Desenvolvimento de um método ou função que interprete o arquivo de Layout de Dados de Fluxo de Caixa e, utilizando a estrutura de Mapa de Aderência apresentada, adapte-se a outros mapas desenvolvidos pelo time de Negócios. Isso facilitará o processo de ingestão de dados de outras fontes para o mesmo propósito (Fluxo de Caixa), exigindo apenas customizações simples por parte do Analista de Dados, como o caminho do arquivo de leitura e de destino.

## Tecnologias Utilizadas

- Python
- SQL

## Funcionalidades

O pipeline desenvolvido pode ser agendado para consumir um arquivo completo, realizando todo o mapeamento, normalização e tratamentos necessários. Posteriormente, carrega as informações no banco de dados, garantindo que apenas novos dados sejam adicionados.
O pipeline já faz o mapeamento por prioridade, conforme mapa de aderencia e estrutura de fluxo de caixa

## Teste do Pipeline

Caso queira simular o pipeline, dentro da pasta `sql`, há os scripts de criação do banco e estrutura para que possa ser testado;
É importante que seja instalado o ambiente virtual e instalado todas as dependencias do projeto.
