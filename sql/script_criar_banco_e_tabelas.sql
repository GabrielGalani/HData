if not exists (select name from sys.databases where name = 'DW')
CREATE DATABASE DW

USE DW
if not exists (select * from sysobjects where name='dim_unidade' and xtype='U')
create table dim_unidade (
	pk_unidade int identity(1,1) primary key,
	ds_unidade varchar(max)
)

if not exists (select * from sysobjects where name='fato_lancamento' and xtype='U')
create table fato_lancamento (
	sk_unidade int foreign key references dim_unidade(pk_unidade),
	sk_lancamento int identity(1,1) primary key,
	pk_lancamento int,
	cd_lancamento int,
	sk_conta_financeira int,
	dt_vencimento date,
	dt_mes_referencia date,
	vl_total_nf float,
	dt_venc_parcela date,
	sk_fornecedor int,
	dt_realizacao date,
	vl_realizacao float,
	ds_observacao text,
	dt_emissao_nf_doc date,
	dt_inclusao date,
	cd_nf_doc varchar(max),
	sk_conta_bancaria int,
	cd_parcela int,
	vl_juros float,
	vl_descontos float,
	tp_lancamento varchar(max),
	sk_centro_custos int,
	tp_forma_pgto varchar(max),
	cd_documento varchar(max),
	tp_situacao varchar(max)
)

if not exists (select * from sysobjects where name='dim_conta_financeira' and xtype='U')
create table dim_conta_financeira (
	pk_conta_financeira int identity(1,1) primary key,
	cd_conta_financeira int,
	ds_conta_financeira varchar(max),
	sk_normal_conta_financeira int
)

if not exists (select * from sysobjects where name='depara_conta_financeira' and xtype='U')
create table depara_conta_financeira (
	pk_normal_conta_financeira int identity(1,1) primary key,
	ds_normal_conta_financeira varchar(max)
)

if not exists (select * from sysobjects where name='dim_fornecedor' and xtype='U')
create table dim_fornecedor (
	pk_fornecedor int identity(1,1) primary key,
	cd_fornecedor int,
	ds_fornecedor varchar(max)
)

if not exists (select * from sysobjects where name='dim_empresa_executante' and xtype='U')
create table dim_empresa_executante (
	sk_empresa_exec int identity(1,1) primary key,
	ds_empresa_exec varchar(max),
	pk_unidade int foreign key references dim_unidade(pk_unidade)
)

if not exists (select * from sysobjects where name='dim_conta_bancaria' and xtype='U')
create table dim_conta_bancaria (
	pk_conta_bancaria int identity(1,1) primary key,
	sk_empresa_exec int foreign key references dim_empresa_executante(sk_empresa_exec),
	cd_conta_bancaria int,
	cd_agencia int,
	ds_conta_bancaria varchar(max),
	cd_banco int,
	sn_ativo bit
)

if not exists (select * from sysobjects where name='dim_centro_custos' and xtype='U')
create table dim_centro_custos (
	pk_centro_custos int identity(1,1) primary key,
	ds_centro_custos varchar(max),
	cd_centro_custos int,
	sk_normal_centro_custos int
)

if not exists (select * from sysobjects where name='depara_centro_custos' and xtype='U')
create table depara_centro_custos (
	pk_normal_centro_custos int identity(1,1) primary key,
	ds_normal_centro_custos varchar(max)
)

if not exists (select * from sysobjects where name='fato_saldo_inicial' and xtype='U')
create table fato_saldo_inicial (
	cd_saldo_inicial int identity(1,1) primary key,
	dt_saldo_inicial date,
	vl_saldo_inicial float,
	sk_conta_bancaria int foreign key references dim_conta_bancaria(pk_conta_bancaria)
)
