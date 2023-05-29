/*
Author: Denis Babichev
Date: 25.01.2023
Description: This queries will help you analyze your Timescaledb table sizes, system information etc.
You can find lots of best practices for query analysis on Timescaledb official doc page: https://docs.timescale.com/

This file will display queries specific to certain cases, as well as offer additional and alternative analytical queries

Some of queries are designed to be used only for multi-node setups, and will be named accordingly
*/




-- <------------------------------------- COMMON QUERIES ------------------------------------->

-- ============================================================================================ --
-- Get metrics volume, order by hypertable size --
-- ============================================================================================ --

select
    hypertable_name,
    pg_size_pretty(ht)
from (
    select
        hypertable_name,
        hypertable_size(format('%I.%I',
        hypertable_schema,
        hypertable_name)::regclass) as ht
    from
        timescaledb_information.hypertables
    order by
        ht desc
) t;

-- ============================================================================================ --
-- Get all hypertables real size
-- ============================================================================================ --

select
    hypertable_name, 
    pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) as size
from 
    timescaledb_information.hypertables order by size;



-- <------------------------------------- MULTINODE QUERIES ------------------------------------->

-- ============================================================================================ --
-- Get info about which metric lies on which datanode
-- ============================================================================================ --

select
    t.data_node,
    t.hypertable_name,
    (_timescaledb_internal.data_node_hypertable_info(
    t.data_node,
    t.hypertable_schema,
    t.hypertable_name)).*
from (
    select
        hypertable_schema,
        hypertable_name,
        unnest(data_nodes) as data_node
    from
        timescaledb_information.hypertables
    ) as t;

-- ============================================================================================ --
-- Get metrics distribution by datanodes +
-- max value for each metric by datanode + 
-- must loaded datanode by metric in separate column
-- 
-- !!! This script takes quite a time to fetch data, especially if you have tons of data !!!
-- !!! Very useful, if you use experimental functions, such as block_new_chunks and distribute your metrics not evenly !!!
-- ============================================================================================ --

with tmp_as as (
    select 
        t.data_node, 
        t.hypertable_name, 
        (_timescaledb_internal.data_node_hypertable_info(t.data_node, t.hypertable_schema, t.hypertable_name)).*
    from (
        select
            hypertable_schema,
            hypertable_name,
            unnest(data_nodes) as data_node
        from
            timescaledb_information.hypertables
        ) as t
    )
    select *, 
        MAX(tmp_as.total_bytes) OVER ( PARTITION BY tmp_as.hypertable_name) as max_total_bytes_dn
    from tmp_as
    left join (
        select hypertable_name, total_bytes, data_node as highest_loaded_dn
            from (
                select *,
                MAX(total_bytes) OVER ( PARTITION BY hypertable_name order by total_bytes desc) as max_total_bytes_dn
                from tmp_as
                order by max_total_bytes_dn desc
            ) as tmp2
        where max_total_bytes_dn = total_bytes
    ) m
  on tmp_as.total_bytes = m.total_bytes and tmp_as.hypertable_name = m.hypertable_name
  order by max_total_bytes_dn desc  

-- ============================================================================================ --
-- !!! THIS SCRIPT CHANGES TSDB NATIVE LOGIC. READ THE DESCRIPTION CAREFULLY BEFORE RUNNING !!!
-- Disable creating new chunks for all hypertables only for one specific datanode
-- ============================================================================================ --

select timescaledb_experimental.block_new_chunks(dn, ht)
from (
    select
    	ht,
        row_number() over (order by ht) as num
        from (
            select
                format('%I.%I',
                hypertable_schema,
                hypertable_name)::regclass as ht
            from
                timescaledb_information.hypertables
        ) ht
    )t
left join (
    values 
    (0,'dn5') -- In this example creating new chunks will be disabled for datanode - 'dn5'.
              -- You can switch this value for desirable datanode value 
              -- You can also specify several datanodes to block chunks (for example: (0,'dn5'), (0,'dn3'))
) as datanodes (n, dn)
on 
datanodes.n != t.num 

-- You can also allow creating new chunks for specific datanode in case you want to prevent blocking using query:

select timescaledb_experimental.allow_new_chunks(datanodes.dn)
    from (
        values ('dn1') -- You can also specify several datanodes to allow chunks (for example: ('dn5'), ('dn3'))
    ) as datanodes (dn)


