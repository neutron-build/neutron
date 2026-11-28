-- TPC-H query 13 (standard specification text, DuckDB dialect).
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer
            left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as orders_per_customer
group by
    c_count
order by
    custdist desc,
    c_count desc;
