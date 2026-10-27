{#- Bronze rows of the newest committed batch of one domain=metadata dataset.
    Registry exports are full snapshots, so only the latest manifest counts:
    an entry removed from the registry disappears here on the next export. -#}
{% macro metadata_latest(dataset) %}
    with latest_manifest as (
        select cast(bronze.key as varchar) as bronze_key
        from read_json_auto(
            '{{ var("bronze_manifest_root") }}/source=solo-empire-registry/batch_id=*.json',
            union_by_name = true
        )
        where domain = 'metadata'
          and dataset = '{{ dataset }}'
        qualify row_number() over (order by received_at desc) = 1
    )

    select parts.payload_json, parts.received_at as exported_at
    from read_parquet(
        '{{ var("bronze_root") }}/domain=metadata/dataset={{ dataset }}/schema_version=*/ingest_date=*/source=*/part-*.parquet',
        filename = true
    ) as parts
    inner join latest_manifest
        on ends_with(replace(parts.filename, '\\', '/'), latest_manifest.bronze_key)
{% endmacro %}
