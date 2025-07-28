with
    source_poluentes as (
        select *
        from {{ source('raw', 'raw_poluentes') }}
    )

    , padroniza_tipos_colunas as (
        select
            cast(pm10 as decimal(10,5)) as pm10
            , cast(pm2_5 as decimal(10,5)) as pm2_5
            , cast("NO" as decimal(10,5)) as "NO"
            , cast(no2 as decimal(10,5)) as no2
            , cast(nox as decimal(10,5)) as nox
            , cast(co as decimal(10,5)) as co
            , cast(ozono as decimal(10,5)) as ozono
            , cast(station as string) as station_id
            , case
                when right(datetime, 5) = '24:00'
                -- Se a hora for 24:00, pega a data, adiciona 1 dia e define a hora para 00:00
                then to_timestamp(left(datetime, 10) || ' 00:00', 'DD-MM-YYYY HH24:MI') + interval '1 day'
                -- Caso contrário, apenas converte o datetime para um timestamp padrão
                else to_timestamp(datetime, 'DD-MM-YYYY HH24:MI')
            end as medicao_datetime_utc -- Renomeado para um nome mais descritivo e indicando o fuso horário (assumindo UTC)
        from source_poluentes
    )

    , dedup as (
        select
            *,
            row_number() over (
                partition by station_id, medicao_datetime_utc
                order by medicao_datetime_utc
            ) as order_index
        from padroniza_tipos_colunas
        -- CORRIGIDO: O QUALIFY já faz o filtro. A cláusula WHERE foi removida.
        qualify order_index = 1
    )

select
    * exclude (order_index)
from dedup