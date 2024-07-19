with commodities as (
    select 
    * from
    {{ ref ('stg_commodities')}}

),

movimentacao_commodities as(

    select * from
    {{ ref ('stg_movimentacao_commodities')}}
),

joined as(
    select
    c.data,
    c.símbolo,
    c.valor_fechamento,
    m.acao,
    m.quantidade,
    (m.quantidade * c.valor_fechamento) as valor,
    case
        when m.acao = 'sell' then (m.quantidade * c.valor_fechamento)
        else -(m.quantidade*c.valor_fechamento)
    end as ganho
    from
    stg_commodities c
    inner join
    stg_movimentacao_commodities m
    on c.data = m.data
    and c.símbolo = m.simbolo

)
select * from joined