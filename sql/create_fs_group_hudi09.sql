create table test_hudi_table
(  id int,
   name string,
   price double,
   ts long,
   dt string
 )
  using hudi
  partitioned by (@partition_key@)
  options ( primaryKey = @primaryKey@,  type = 'cow' )

  field = Regex.Replace(field.ToLower(), @"select\s+|\s+from\s+|\[|\]", "");