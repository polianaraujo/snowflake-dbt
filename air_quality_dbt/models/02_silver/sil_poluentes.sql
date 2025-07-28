with
    staging_poluentes as (
        select
            pm10
            , pm2_5
            , 'no'
            , no2
            , nox
            , co
            , ozono
            , station
            , data_medicao
        from {{ ref('stg_qualidade_ar') }}
    )

select *
from staging_poluentes
