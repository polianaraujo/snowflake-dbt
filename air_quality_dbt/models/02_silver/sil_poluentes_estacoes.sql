with
    staging_poluentes as (
        select *
        from {{ ref('stg_qualidade_ar') }}
    ),

    staging_estacoes as (
        select
            station_id
            , latitude
            , longitude
        from {{ ref('stg_infos_estacoes') }}
    )

select
    p.pm10
    , p.pm2_5
    , p."NO" as no -- Usar aspas se o nome da coluna for "NO", que é uma palavra reservada
    , p.no2
    , p.nox
    , p.co
    , p.ozono
    , p.station
    , p.data_medicao

    , e.latitude
    , e.longitude

from
    staging_poluentes as p -- 'p' é um apelido (alias) para facilitar a escrita

left join staging_estacoes as e
    on p.station = e.station_id

order by
    p.station,
    p.data_medicao