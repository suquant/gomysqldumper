Golang mysql dumper
=====

Dump table data into csv files chunked by slice size with gzip compression

At complete printed into stdout file_path[(any errors)]: count


Requirements
=====

* github.com/go-sql-driver/mysql


Install requirements

```
# go get github.com/go-sql-driver/mysql
```


How to use?
=====

1. Build it 

```
# go build main.go
```

2. Run it

```
# ./main -db_dsl="dbuser:dbpass@/test_db" -table="users" -to_path="/dump_path/" -slice_size=100000 -concurrency=5 -gzip_level=7
```

Or get help information

```
# ./main -h
```