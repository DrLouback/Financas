with source as (
        select
            "True",
            "Close",
            símbolo
        from
            {{source ('financedb_lpoj',"commodities")}}


),

renamed as (

        select
            cast("True" as date) as data,
            "Close" as valor_fechamento,
            símbolo
        from
            source

)

    select * from renamed