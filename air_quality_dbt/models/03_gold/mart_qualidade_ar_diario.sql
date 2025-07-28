-- models/03_gold/mart_qualidade_ar_diario.sql
-- Tabela agregada com métricas diárias de qualidade do ar por estação.
-- Pronta para ser consumida por ferramentas de BI e análise.

with
    poluentes_silver as (
        select * from {{ ref('sil_poluentes') }}
    )

select
    -- Trunca a data/hora para o início do dia, criando nossa chave de dia
    date_trunc('day', data_medicao)::DATE as data_dia,
    station,
    avg(pm10) as media_diaria_pm10,
    max(pm10) as pico_diario_pm10,
    avg(pm2_5) as media_diaria_pm2_5,
    max(pm2_5) as pico_diario_pm2_5,
    avg(no2) as media_diaria_no2,
    max(no2) as pico_diario_no2,
    count(*) as total_de_medicoes_dia
    
from
    poluentes_silver
group by
    1, 2 -- Agrupa por data_dia e station
order by
    data_dia desc,
    station