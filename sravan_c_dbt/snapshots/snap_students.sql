{% snapshot snap_students %}

{{
    config(
        target_schema='snapshots',
        unique_key='student_id',
        strategy='check',
        check_cols=[
            'first_name',
            'last_name',
            'gender',
            'ethnicity',
            'grade_level',
            'home_language'
        ],
        updated_at='ingestion_timestamp'
    )
}}

select
    student_id,
    first_name,
    last_name,
    gender,
    ethnicity,
    grade_level,
    home_language,
    current_timestamp() as ingestion_timestamp

from {{ ref('stg_students') }}

{% endsnapshot %}