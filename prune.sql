create or replace procedure xxdba_prune_partition(schname text, tabname text, retmonths int)
    language plpgsql
  as
$$
declare
    part_name_tag text;
    part_start_date text;
    part_end_date text;
    bsql1 text; 
    bsql2 text; 
    part_full_name text ;
    part_drop_count int;
    retain_part text[]; 
    pdate date;
    cdate date ;
    cplist_record  record;
    cplist cursor for 
        SELECT 
            nmsp_parent.nspname AS parent_schema,
            parent.relname      AS parent,
            nmsp_child.nspname  AS child_schema,
            child.relname       AS part_name
        FROM pg_inherits
            JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
        WHERE parent.relname=tabname and nmsp_parent.nspname = schname 
    ORDER BY 4 ;

begin
-----------------------------------------------------------------------------
-- Purpose       :       Procedure to drop partitions
-- Authors       :       Jay C
-----------------------------------------------------------------------------

    part_drop_count :=0; 

    RAISE NOTICE '-----------------------------------------------------------------------';
    RAISE NOTICE ' ';
    RAISE NOTICE 'This procedure will drop monthly partitions for table based on retention';
    RAISE NOTICE 'months parameter value passed to the program. Future Partitions will not';
    RAISE NOTICE 'be dropped';
    RAISE NOTICE ' ';
    RAISE NOTICE 'Assessing Partitions To Be Retained For Table - %', tabname;
    RAISE NOTICE ' ';

    if NOT EXISTS (SELECT tablename from pg_tables where tablename=tabname and schemaname=schname) then

        RAISE NOTICE 'ERROR : Given Table Name Not Found in Database. Please Check! - %', tabname;
        return;

    end if;

    for mnt in 0..retmonths loop
        part_name_tag := to_char(now()::date - make_interval(months => mnt), '_YYYY_MM') ;

    if EXISTS(SELECT 1 FROM pg_inherits
                JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                 WHERE child.relname=tabname||part_name_tag and nmsp_parent.nspname = schname) then 
            part_full_name := tabname||part_name_tag;
            retain_part := retain_part||part_full_name ;
    end if ;

    end loop; 

    RAISE NOTICE '  Partitions To Be Retained Are : %',retain_part::text; 
    RAISE NOTICE ' ';
    RAISE NOTICE 'Generating Script To Drop Partitions...';
    RAISE NOTICE ' ';

    open cplist ;
    loop
        fetch cplist into cplist_record ; 
        exit when not found; 
        
        RAISE NOTICE ' ';
    RAISE NOTICE '  Analyzing Partition For Drop - %',cplist_record.part_name;

    if cplist_record.part_name::text != all(retain_part)::text  then 

        cdate := to_date(to_char(now()::date,'YYYY_MM_01'),'YYYY_MM_DD') ;
        pdate := to_date(right(cplist_record.part_name,7)||'_01','YYYY_MM_DD');
        
        if ( pdate < cdate ) then 

            bsql1 := 'ALTER TABLE '||schname||'.'||tabname||' DETACH PARTITION '||schname||'.'||cplist_record.part_name ;
            execute bsql1 ;
            bsql2 := 'DROP TABLE '||schname||'.'||cplist_record.part_name ;
            execute bsql2 ; 
                RAISE NOTICE '  -- Dettached Partition - %', bsql1 ;
                RAISE NOTICE '  -- Dropped Partition   - %', bsql2 ;
            part_drop_count := part_drop_count + 1;
         else 
            RAISE NOTICE '  - Ignoring Future Partition For Drop - %', cplist_record.part_name;

         end if;

        else 
            RAISE NOTICE '  - Skipping Partition Drop (To Be Retained) - %', cplist_record.part_name;

        end if; 

    end loop; 

    RAISE NOTICE ' ';
    RAISE NOTICE 'Total Partition Dropped  - %',part_drop_count ;
    RAISE NOTICE ' ';
    RAISE NOTICE 'Partition Drop Procedure Completed For Table - %',tabname ;
    RAISE NOTICE ' ';
    RAISE NOTICE '-----------------------------------------------------------------------';

end;
$$
