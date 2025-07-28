-- models/staging/stg_qualidade_ar.sql
-- renomeando as colunas.

-- passo que vem depois da Bronze e que prepara o terreno para a camada Silver

with
    estacoes as ( -- nome temporário e legível que você está dando para o resultado de uma consulta SELECT (uma tabela virtual temporária que existe apenas durante a execução desta consulta específica)
        select *
        -- ler os dados brutos da camada bronze (snowflake)
        -- schema, tabela (do database)
        from {{ source('raw', 'raw_estacoes') }}
    )

    , padroniza_tipos_colunas_estacoes as (
        select
            cast(station as string) as station_name
            , cast(id as string) as station_id
            
            -- Dados de Localização (a parte mais complexa)
            -- Primeiro, convertemos a string DMS para um número decimal (FLOAT)
            -- Fórmula: Graus + (Minutos / 60) + (Segundos / 3600), com sinal negativo para 'S' e 'W'
            , cast(
                (regexp_substr(latitude, '\\d+', 1, 1) + 
                regexp_substr(latitude, '\\d+', 1, 2) / 60 + 
                regexp_substr(latitude, '\\d+(\\.\\d+)?', 1, 3) / 3600) *
                case when upper(regexp_substr(latitude, '[NS]')) = 'S' then -1 else 1 end
            as float) as latitude

            , cast(
                (regexp_substr(longitude, '\\d+', 1, 1) + 
                regexp_substr(longitude, '\\d+', 1, 2) / 60 + 
                regexp_substr(longitude, '\\d+(\\.\\d+)?', 1, 3) / 3600) *
                case when upper(regexp_substr(longitude, '[WE]')) = 'W' then -1 else 1 end
            as float) as longitude
            
            -- Criando um ponto GEOGRAPHY para análises espaciais (MELHOR PRÁTICA)
            -- st_makepoint(longitude, latitude) as location_geography,

            , cast(altitude_metros as integer) as altitude_m
            , cast(altura_metros as integer) as altura_m
            , cast(localidade as string) as localidade
            , cast(tipo_zona as string) as tipo_zona
            , cast(tipo_estacao as string) as tipo_estacao
            , cast(direcao as string) as direcao
        from estacoes
    )

    , final as (
        select
            *
        from padroniza_tipos_colunas_estacoes
        qualify row_number() over (partition by station_id order by station_name) = 1
    )

select *
from final