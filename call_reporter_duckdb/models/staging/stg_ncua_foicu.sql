with source as (

    select * from {{source('call_reporter','foicu')}}

),

renamed as (

    select
        *

    from source

)

select * from renamed
