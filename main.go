package main

import (
	"runtime"
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/suquant/gomysqldumper/dumper"
	"flag"
	"os"
	"compress/gzip"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	// Use all available CPU
	runtime.GOMAXPROCS(runtime.NumCPU())
	
	var dbDsl string
	var table string
	var concurrency uint64
	var sliceSize uint64
	var toPath string
	var gzipLevel int
	var help bool
	
	flag.StringVar(&dbDsl, "db_dsl", "", "DB DSL, for example: 'user:pass@/test_db'")
	flag.StringVar(&table, "table", "", "Table for dumping")
	flag.StringVar(&toPath, "to_path", "", "Path to directory where dumps are stored")
	flag.Uint64Var(&concurrency, "concurrency", 5, "Concurrency workerks")
	flag.Uint64Var(&sliceSize, "slice_size", 1000, "Slice size")
	flag.IntVar(&gzipLevel, "gzip_level", gzip.DefaultCompression, "Slice size")
	flag.BoolVar(&help, "h", false, "Print help message")
	flag.Parse()
	
	if help == true {
		Usage()
		os.Exit(0)
	}
	
	if dbDsl == "" || concurrency < 0 || toPath == "" || table == "" || sliceSize < 0 {
		Usage()
		os.Exit(1)
	}
	
	db, db_err := sql.Open("mysql", dbDsl)
	
	if db_err != nil {
	    panic(db_err.Error())
	}
	defer db.Close()
	
	ping_err := db.Ping()
	if ping_err != nil {
	    panic(ping_err.Error())
	}
	
	dmp := dumper.NewDumper(db, table, concurrency, sliceSize, gzipLevel)
	result, err := dmp.Dump(toPath)
	
	if err != nil {
		panic(db_err.Error())
	}
	
	for _, job := range result {
		fmt.Println(job.ToString())
	}
}