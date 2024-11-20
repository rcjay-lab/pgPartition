Refer to article published in Medium for more details. - https://medium.com/@rcjay.work/postgres-automated-data-pruning-43a9f5e672ba

To create new partitions (future & current), use below call statement in postgres
call xxba_allocate_partition("Schema Name", "Table Name", "Count of Future Partitions to be created") ; 

To drop older partitions use bwlow call statement in postgres
call xxdba_prune_partition("Schema Name","Table Name", "Count of months to be retained"); 

Above procedures will throw back complete stack of messages back to calling program. 
