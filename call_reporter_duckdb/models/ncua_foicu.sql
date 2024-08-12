with foicu as (

    select
        cu_number::bigint as cu_number,
        strptime(regexp_extract(cycle_date,'(\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}).*',1), '%-M/%-d/%Y %-H:%M:%S')::date as cycle_date,
        join_number::bigint as join_number,
        rssd::bigint as rssd,
        cu_type::bigint as cu_type,
        cu_name::varchar as cu_name,
        city::varchar as city,
        state::varchar as state,
        charterstate::varchar as charterstate,
        state_code::bigint as state_code,
        zip_code::varchar as zip_code,
        county_code::bigint as county_code,
        cong_dist::bigint as cong_dist,
        smsa::bigint as smsa,
        attention_of::varchar as attention_of,
        street::varchar as street,
        region::bigint as region,
        se::varchar as se,
        district::bigint as district,
        year_opened::bigint as year_opened,
        tom_code::varchar as tom_code,
        limited_inc::bigint as limited_inc,
        strptime(regexp_extract(issue_date,'(\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}).*',1), '%-M/%-d/%Y %-H:%M:%S')::date as issue_date,
        peer_group::bigint as peer_group,
        quarter_flag::bigint as quarter_flag,
        ismdi::bigint,
        strptime(regexp_extract(insured_date,'(\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}).*',1), '%-M/%-d/%Y %-H:%M:%S')::date as insured_date,
        source_year::bigint as source_year,
        source_month::bigint as source_month,
        source::varchar as source,
        cu_number_mon_year_key::varchar as cu_number_mon_year_key

    from {{ ref('stg_ncua_foicu') }}

)

select * from foicu




