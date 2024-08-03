with foicu as (

    select * from {{ ref('stg_ncua_foicu') }}

)

select * from foicu
