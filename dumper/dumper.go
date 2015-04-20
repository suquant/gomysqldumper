package dumper
import (
	"fmt"
	"database/sql"
	"strconv"
	"math"
	"time"
	"path"
	"encoding/csv"
	"compress/gzip"
	"os"
)

type job struct {
	offset uint64
	filePath string
	counter uint64
	err error
}

func (self *job) incrementCounter() {
	self.counter++
}

func (self *job) ToString() string {
	result := self.filePath
	if self.err != nil {
		result += fmt.Sprintf(" (error: '%s')", self.err.Error())
	}
	result += fmt.Sprintf(": %d", self.counter)
	return result
}

type Dumper struct {
	concurrency	uint64
	sliceSize uint64
	tableName string
	
	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db *sql.DB
	
	baseFileName string
	gzipLevel int
}

func NewDumper(db *sql.DB, tableName string, concurrency, sliceSize uint64, gzipLevel int) *Dumper {
	dumper := new(Dumper)
	dumper.db = db
	dumper.tableName = tableName
	dumper.concurrency = concurrency
	dumper.sliceSize = sliceSize
	dumper.gzipLevel = gzipLevel
	
	utcNow := time.Now().UTC()
	dumper.baseFileName = fmt.Sprintf(
		"%d-%02d-%02d_%02d-%02d-%02d",
		utcNow.Year(), utcNow.Month(), utcNow.Day(),
		utcNow.Hour(), utcNow.Minute(), utcNow.Second())
	
	return dumper
}

func (self *Dumper) getRowsCount() (uint64, error) {
	var res sql.NullString
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", self.tableName)
	err := self.db.QueryRow(query).Scan(&res)
	if err != nil {
		return 0, err
	}
	
	val, err := res.Value()
	if err != nil {
		return 0, err
	}
	
	return strconv.ParseUint(val.(string), 0, 64)
}

func (self *Dumper) getJobs(toPath string) ([] *job, error) {
	total, err := self.getRowsCount()
	if err != nil {
		return nil, err
	}
	
	sliceCount :=int(math.Ceil(float64(total) / float64(self.sliceSize)))
	jobs := make([] *job, sliceCount)
	for i := 0; i < sliceCount; i++ {
		filePath := path.Join(
			toPath,
			fmt.Sprintf("%s_%d.csv.gz", self.baseFileName, i))
		offset := uint64(i) * uint64(self.sliceSize)
		jobs[i] = &job{offset: offset, filePath: filePath, counter: 0}
	}
	return jobs, nil
}

func (self *Dumper) dumpCsv(job *job) (error) {
	file, err := os.OpenFile(job.filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	defer file.Close()
	if err != nil {
		return err
	}
	
	query := fmt.Sprintf(
		"SELECT * FROM `%s` LIMIT %d OFFSET %d",
		self.tableName, self.sliceSize, job.offset)
	rows, err := self.db.Query(query)
	if err != nil {
        return err
    }
	
	columns, err := rows.Columns()
	if err != nil {
        return err
    }
	
	gzwriter, err := gzip.NewWriterLevel(file, self.gzipLevel)
	defer gzwriter.Flush()
	defer gzwriter.Close()
	if err != nil {
		return err
	}
	
	writer := csv.NewWriter(gzwriter)
	defer writer.Flush()
	
	err = writer.Write(columns)
	if err != nil {
		return err
	}
	
	columnsCount := len(columns)
    values := make([]sql.RawBytes, columnsCount)
    
    scanArgs := make([]interface{}, len(values))
    for i := range values {
        scanArgs[i] = &values[i]
    }
	
	for rows.Next() {
        err = rows.Scan(scanArgs...)
        if err != nil {
            return err
        }

        var value string
        csvValues := make([]string, columnsCount)
        for i, col := range values {
            // Here we can check if the value is nil (NULL value)
            if col == nil {
                value = "NULL"
            } else {
                value = string(col)
            }
            csvValues[i] = value
        }
        err = writer.Write(csvValues)
        if err != nil {
        	return err
        }
        job.incrementCounter()
	}
	
	return nil
} 

func (self *Dumper) worker(jobsChannel <- chan *job, resultsChannel chan <- *job) {
	for job := range jobsChannel {
        err := self.dumpCsv(job)
        if err != nil {
        	job.err = err
        }
        resultsChannel <- job
    }
}

func (self *Dumper) Dump(toPath string) ([] *job, error) {
	jobs, err := self.getJobs(toPath)
	if err != nil {
		return nil, err
	}
	
	workersCount := int(math.Min(float64(self.concurrency), float64(len(jobs))))
	if workersCount < 1 {
		return nil, nil
	}
	
	jobsCount := len(jobs)
	jobsChannel := make(chan *job)
	resultsChannel := make(chan *job)
	for i := 0; i < workersCount; i++ {
		go self.worker(jobsChannel, resultsChannel)
	}
	
	go func() {
		for _, job := range jobs {
			jobsChannel <-job
		}
		close(jobsChannel)
	}()
	
	result := make([] *job, jobsCount)
	for i := 0; i < jobsCount; i++ {
		job := <-resultsChannel
		result[i] = job
	}
	close(resultsChannel)
	
	return result, nil
}