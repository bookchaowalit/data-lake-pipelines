-- Ops check: rows and parts per dataset per ingest date (compare with validate.py).
select dataset, date, count(*) as rows, count(distinct filename) as parts,
       count(*) filter (where _coerce_errors is not null) as rows_with_coerce_errors
from (
    select regexp_extract(filename, 'raw/([a-z_]+)/date=', 1) as dataset,
           regexp_extract(filename, 'date=([0-9-]+)', 1) as date, filename, _coerce_errors
    from read_parquet('{lake}/raw/*/date=*/*.parquet', filename = true, union_by_name = true,
                      hive_partitioning = false)
)
group by all
order by dataset, date;
