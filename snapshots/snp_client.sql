{% snapshot snp_client %}
    {{
        config(
          target_schema='snapshots',
          unique_key='id',
          strategy='check',
          check_cols='all'
        )
    }}

    select * from {{ source('external_source', 'client')}}
{% endsnapshot %}