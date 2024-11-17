create or replace procedure xxdba_allocate_partition(schname text, tabname text, partcount int)
    language plpgsql
  as
$$
declare
    part_name_tag text;
    part_start_date text;
    part_end_date text;
    part_create_count int;
    bsql text; 
begin

-----------------------------------------------------------------------------
-- Purpose       :       Procedure to create future partitions
-- Authors       :       Jay C
-----------------------------------------------------------------------------

    RAISE NOTICE '-----------------------------------------------------------------------'; 
    RAISE NOTICE ' ';
    RAISE NOTICE 'This procedure will create future monthly partitions for table based on';
    RAISE NOTICE 'future-months parameter value passed to the program'; 
    RAISE NOTICE ' ';
    RAISE NOTICE 'Generating Script To Create Future Monthly Partition For Table - %', tabname; 
    RAISE NOTICE ' ';
    part_create_count := 0;

    if NOT EXISTS (SELECT tablename from pg_tables where tablename=tabname and schemaname=schname) then 

    RAISE NOTICE 'ERROR : Given Table Name Not Found in Database. Please Check! - %', tabname;
    return;

    end if;     

    for mnt in 0..partcount loop

        part_name_tag := to_char(now()::date + make_interval(months => mnt), '_YYYY_MM') ;
        part_start_date := '('||''''|| to_char(now()::date + make_interval(months => mnt), 'YYYY-MM-01') ||''''||')' ;
        part_end_date   := '('||''''|| to_char(now()::date + make_interval(months => mnt+1), 'YYYY-MM-01') ||''''||')';
        
        -- construct SQL
        bsql := 'CREATE TABLE '||schname||'.'||tabname||part_name_tag||' PARTITION OF '||schname||'.'||tabname||' FOR VALUES FROM '||part_start_date||' TO '||part_end_date ;

        RAISE NOTICE '  Attempting To Create Partition - %',tabname||part_name_tag ;

        -- Check if partition already exist. If Not Create it 
        if NOT EXISTS(SELECT relname FROM pg_class WHERE relname=tabname||part_name_tag) then
            execute bsql ; 
            part_create_count := part_create_count +1 ; 
            RAISE NOTICE '   Partition Created - %',bsql ;
        else
            RAISE NOTICE '   Skipping... Partition already exist %',tabname||part_name_tag ;
        end if; 

    end loop;

    RAISE NOTICE ' ';
    RAISE NOTICE 'Total New Partition Created For Table  - %',part_create_count ;
    RAISE NOTICE ' ';
    RAISE NOTICE 'Partition Creation Procedure Completed For Table - %',tabname ;
    RAISE NOTICE ' ';
    RAISE NOTICE '-----------------------------------------------------------------------'; 
end;
$$
