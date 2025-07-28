-- models/staging/stg_qualidade_ar.sql
-- renomeando as colunas.

-- passo que vem depois da Bronze e que prepara o terreno para a camada Silver

with
    poluentes as ( -- nome temporário e legível que você está dando para o resultado de uma consulta SELECT (uma tabela virtual temporária que existe apenas durante a execução desta consulta específica)
        select *
        -- ler os dados brutos da camada bronze (snowflake)
        -- schema, tabela (do database)
        from {{ source('raw', 'raw_poluentes') }}
    )

    , padroniza_tipos_colunas_poluentes as (
        select
            cast(pm10 as float) as pm10
            , cast(pm2_5 as float) as pm2_5
            , cast(no as float) as no
            , cast(no2 as float) as no2
            , cast(nox as float) as nox
            , cast(co as float) as co
            , cast(ozono as float) as ozono
            , cast(STATION as string) as station
            -- LÓGICA PARA TRATAR A HORA "24:00"
            , case 
                -- Se a hora for '24:00'...
                when datetime_text like '% 24:00' 
                -- ...então pegue a parte da data, converta, some 1 dia e transforme em timestamp.
                then dateadd(day, 1, to_date(left(datetime_text, 10), 'DD-MM-YYYY'))::timestamp
                -- Para todos os outros casos, use a conversão normal e segura.
                else try_to_timestamp(datetime_text, 'DD-MM-YYYY HH24:MI')
              end as data_medicao

        from poluentes
    )

    , dedup as (
        select
            *
            , row_number() over (
                partition by station, data_medicao
                order by data_medicao desc
            ) as order_index
        from padroniza_tipos_colunas_poluentes
        qualify order_index = 1
    )

select * exclude (order_index)
from dedup
