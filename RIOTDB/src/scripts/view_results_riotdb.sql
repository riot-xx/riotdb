show tables;
select next_vec_index from DB_Sources;
select metadata_id, table_name, size,
       is_view, ref_counter, sxp_type
       from Metadata order by metadata_id;
select * from View_Reference order by view_id;
