/*
Author: Denis Babichev
Date: 13.02.2023
Description: This queries will help you automate process of reordering your disk space, in case you are running out of it. By adding new disk to your system
you can create new tablespace, pointing to this disk, attach desirable hypertables to this tablespace and then move chunks to it.

More about chunks: https://docs.timescale.com/timescaledb/latest/overview/core-concepts/hypertables-and-chunks/ 
More about chunks movement: https://docs.timescale.com/api/latest/hypertable/move_chunk/
*/

/* <------------------------------------- PREREQUISITES ------------------------------------->
   1) Chunks can be moved through tablespaces. So first of all you need to create one. It can be done on the same disk, where your system is running,
   but it is more logical to create it on separate disk/partition. In total:
    * Create folder where new tablespace will be pointing. Do not forget to set postgres owner and group
    * Create tablespace itself (https://www.postgresql.org/docs/current/sql-createtablespace.html). Imagine we want to create tablespace called dbspace, pointing to /mnt/second_data/dbs:
      
      CREATE TABLESPACE dbspace OWNER your_tsdb_owner LOCATION '/mnt/second_data/dbs';
    
    * Attach your created tablespace to desirable hypertable. Imagine it is called protoBytes:
      
      SELECT attach_tablespace('dbspace', 'protoBytes', if_not_attached => true);
    
    * Ensure hypertable was attached:

      SELECT * FROM show_tablespaces('protoBytes');
*/


-- ============================================================================================ --
-- Get chunks, you want to move
-- ============================================================================================ --

select * from 
  chunks_detailed_size('prom_data."fanState"') 
where node_name = 'dn4'
order by total_bytes desc;

/*
Here fanState is a name of your hypertable. prom_data is a name of schema, wher your hypertables are stored
If you use promscale, this schema name is default
Node_name parameter is iptional. If you use multinode - setup, and you want to move chunks from specific datanode, this might be usefull
In case of singlenode - setup, or you want to get chunk info through all datanodes you can skip it
Also, using order by will help you sort chunks by size

The result of this query will help you figure out, if the size of chunks is worth moving it to another tablespace, or not
*/

-- We can modify the script above a bit, to get chunks_detailed_size info across all hypertables:
SELECT ht.hypertable_schema,
       ht.hypertable_name,
       cs.chunk_name,
       cs.total_bytes,
       cs.index_bytes,
       cs.toast_bytes,
       cs.table_bytes
FROM timescaledb_information.hypertables AS ht
JOIN LATERAL chunks_detailed_size(format('%I.%I', ht.hypertable_schema, ht.hypertable_name)) AS cs ON true
WHERE ht.hypertable_schema = 'prom_data' -- Be carefull, in case you have lots of chunk/hypertables, script completion might take a while.

--order by cs.table_bytes
-- Order by in that case will highly increase query completion time. So once again, if you have lots of data, take your time. You might even need to increse timeouts                                  


/*
Combining chunks_detailed_size() and show_chunks() we can filter chunks, that we want to move based not only on size, but also on their age.
Example below:
*/

SELECT * FROM show_chunks('prom_data."fanState"', older_than => INTERVAL '10 day') 
-- Will show us all chunks from "fanState" hypertbale, that are older than 10 days

SELECT hypertable_schema,hypertable_name, cs
FROM timescaledb_information.hypertables AS ht
JOIN LATERAL show_chunks(format('%I.%I', ht.hypertable_schema, ht.hypertable_name),older_than => INTERVAL '10 days') AS cs ON true
WHERE ht.hypertable_schema = 'prom_data';
-- Modyfying script a bit, we can get all chunks for all ht, that are older than 10 days

with chunks_interval_data as ( 
	SELECT hypertable_schema,hypertable_name, chunk_full_name 
	FROM timescaledb_information.hypertables AS ht
	JOIN LATERAL show_chunks(format('%I.%I', ht.hypertable_schema, ht.hypertable_name),older_than => INTERVAL '10 days') AS chunk_full_name ON true
	WHERE ht.hypertable_schema = 'prom_data'
)
select cid.hypertable_schema,cid.hypertable_name, cid.chunk_full_name, cs.total_bytes from chunks_interval_data cid
join (
	select (chunk_schema || '.' || chunk_name)::regclass as chunks_full_name,total_bytes from 
	chunks_detailed_size('prom_data."fanState"') 
) cs on (cs.chunks_full_name = cid.chunk_full_name)
-- order by cs.total_bytes desc - Optional. If you need to sort output by size

-- Сombining all the conditions considered above, we can form a query, that will show us chunks older than 10 days 
-- and it's total size for specified metric. In that case - 'prom_data."fanState"'. It might be key values to decide if you need to move chunks

with max_chunk_size as ( 
    SELECT ht.hypertable_schema, ht.hypertable_name, cs.total_bytes,cs.chunk_name
    FROM timescaledb_information.hypertables AS ht
    JOIN LATERAL show_chunks(format('%I.%I', ht.hypertable_schema, ht.hypertable_name),newer_than => INTERVAL '10 days') AS chunk_full ON true
  
    JOIN (
        SELECT (chunk_schema || '.' || chunk_name)::regclass AS chunk_full_name, chunk_name, total_bytes
        FROM chunks_detailed_size('prom_data."node_cpu_seconds_total"')
    ) cs ON cs.chunk_full_name = chunk_full   
    
    WHERE ht.hypertable_schema = 'prom_data'
),

max_chunk_bytes AS (
    SELECT hypertable_schema, hypertable_name, MAX(total_bytes) AS max_bytes
    FROM max_chunk_size
    GROUP BY hypertable_schema, hypertable_name
)

SELECT mcs.hypertable_schema, mcs.hypertable_name, mcs.chunk_name, mcs.total_bytes
FROM max_chunk_size mcs
JOIN max_chunk_bytes mcb ON mcs.hypertable_schema = mcb.hypertable_schema
    AND mcs.hypertable_name = mcb.hypertable_name
    AND mcs.total_bytes = mcb.max_bytes;
-- Adding an aggregation function, we can retrieve maximum chunk size for selected hypertable.
-- Combining it with older_than/newer_than intervals, we can play with conditions, that can become a rule for a cron-job, for example
-- That will move chunks to external tablespace every specified period of time


/*
Final example of pl/sql script, that can be used as chunks auto-mover for specified hypertables:
*/

DO $$
DECLARE
    hypertable varchar[] := array['fanState','node_cpu_seconds_total']; -- Hypertables we are working with
    -- !!! DO NOT FORGET, that this specified hypertables must be attached to your created tablespace !!!

    hypertable_schema_concat varchar(100) := 'prom_data'; -- Schema name, where hypertables are stored
    var varchar(100); -- loop var
    res_chunk varchar(100); -- result var, we will write chunk select result here
    res_index varchar(100); -- result var, we will write index select result here
    res_move varchar(100); -- result var, we will write chunk movement result here
    chunk_schema_concat varchar(100) := '_timescaledb_internal'; -- schema where chunks are stored
    start_time interval := interval '10 days'; -- chunk interval, we want to consider
BEGIN
    FOREACH var IN ARRAY hypertable
    LOOP
        RAISE NOTICE 'Working with hypertable: %', var;

        WITH max_chunk_size AS (
            SELECT ht.hypertable_schema, ht.hypertable_name, cs.total_bytes, cs.chunk_name
            FROM timescaledb_information.hypertables AS ht
            JOIN LATERAL show_chunks(format('%I.%I', ht.hypertable_schema, ht.hypertable_name), older_than => start_time) AS chunk_full ON true
            -- You can choose between newer_than and older_than options
            JOIN (
                SELECT (chunk_schema || '.' || chunk_name)::regclass AS chunk_full_name, chunk_name, total_bytes
                FROM chunks_detailed_size(format('%I.%I', hypertable_schema_concat, var))
            ) cs ON cs.chunk_full_name = chunk_full
            WHERE ht.hypertable_schema = 'prom_data'
        ),

        max_chunk_bytes AS (
            SELECT hypertable_schema, hypertable_name, MAX(total_bytes) AS max_bytes
            FROM max_chunk_size
            GROUP BY hypertable_schema, hypertable_name
        )
        SELECT mcs.chunk_name INTO res_chunk
        FROM max_chunk_size mcs
        JOIN max_chunk_bytes mcb ON mcs.hypertable_schema = mcb.hypertable_schema
            AND mcs.hypertable_name = mcb.hypertable_name
            AND mcs.total_bytes = mcb.max_bytes;
        
        RAISE NOTICE 'Chunk retrieved: %', res_chunk;

		select indexname into res_index
		from pg_indexes
		where tablename = var
		and schemaname = 'prom_data';
		
        RAISE NOTICE 'Corresponding index: %', res_index;
        RAISE NOTICE 'Moving chunk, rebuilding index';
        
        SELECT move_chunk into res_move (
        chunk => (chunk_schema_concat || '.' || res_chunk), -- retrieved chunk + chunk schema
        destination_tablespace => 'dbspace', -- specify your chunks destination tablespace name
        index_destination_tablespace => 'dbspace', -- specify your indexes tablespace name. May differ from chunks destination
        reorder_index => (hypertable_schema_concat || '.' || res_index), -- index retrieved + index schema. 
        -- for this example index location is same as hypertable location. You might need to specify index location, 
        -- if you store indexes separately from hypertables 
        
        verbose => TRUE ); -- verbose output      
       
    END LOOP;
END;
$$;