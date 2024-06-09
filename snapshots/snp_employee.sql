{% snapshot snp_employee %}
    {{
        config(
          target_schema='snapshots',
          unique_key='id',
          strategy='check',
          check_cols='all'
        )
    }}

    select * from {{ source('external_source', 'employee')}}
{% endsnapshot %}