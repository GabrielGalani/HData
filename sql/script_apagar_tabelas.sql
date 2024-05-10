delete from [fato_lancamento]
dbcc checkident ('fato_lancamento', reseed, 0)

delete from fato_saldo_inicial
dbcc checkident ('fato_saldo_inicial', reseed, 0)

delete from dim_fornecedor
dbcc checkident ('dim_fornecedor', reseed, 0)

delete from dim_conta_financeira
dbcc checkident ('dim_conta_financeira', reseed, 0)

delete from dim_conta_bancaria
dbcc checkident ('dim_conta_bancaria', reseed, 0)

delete from dim_centro_custos
dbcc checkident ('dim_centro_custos', reseed, 0)

delete from dim_empresa_executante
dbcc checkident ('dim_empresa_executante', reseed, 0)

delete from dim_unidade
dbcc checkident ('dim_unidade', reseed, 0)





