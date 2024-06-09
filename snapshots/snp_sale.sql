{% snapshot snp_sale %}
    {{
        config(
          target_schema='snapshots',
          unique_key='id',
          strategy='check',
          check_cols='all'
        )
    }}

    select * from {{ source('external_source', 'sale')}}
{% endsnapshot %}