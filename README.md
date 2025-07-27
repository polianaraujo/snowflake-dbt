# snowflake-dbt
Utilizando DBT para construir um fluxo de transformação de dados de bancos de dados no Snowflake.

- venv
```
python3 -m venv <nome_venv>
```

- dbt
```
pip install dbt-core dbt-snowflake
dbt init <nome_do_projeto_dbt>
```
Setando as configurações do dbt com o snowflake

## Snowflake

Dois bancos de dado:
- AIR_BRONZE: conectado à S3 para carregar os arquivos CSV em tabelas com colunas VARCHAR (schema RAW), e um schema STAGING temporário para fazer ajustes de coluna necessários nas colunas pelo DBT, sem materializar as tabelas no snowflake

- AIR_PRD: Um schema SILVER para junção de tabelas, um schema GOLD para consultas específicas.
Possui um schema para inferir dados nulos com KNN.

### AIR_BRONZE

1. 


### AIR_PRD

1. 