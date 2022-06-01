package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	PostgresHostFlag     = "localhost"
	PostgresPortFlag     = "5432"
	PostgresUserFlag     = "postgres"
	PostgresPasswordFlag = "postgres"
	PostgresDatabaseFlag = "pricing_sample"

	limitQuery int = 5000
)

func FormatDSN(props map[string]string) string {
	var s strings.Builder
	for k, v := range props {
		s.WriteString(k)
		s.WriteString("=")
		s.WriteString(v)
		s.WriteString(" ")
	}
	return s.String()
}
func NewDB() (*sqlx.DB, error) {
	const driverName = "postgres"
	connStr := FormatDSN(map[string]string{
		"host":     PostgresHostFlag,
		"port":     PostgresPortFlag,
		"user":     PostgresUserFlag,
		"password": PostgresPasswordFlag,
		"dbname":   PostgresDatabaseFlag,
		"sslmode":  "disable",
	})

	return sqlx.Connect(driverName, connStr)
}

type Price struct {
	Price     float64 `json:"price"`
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
}

type PriceAPI struct {
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

func TestGet(db *sqlx.DB, symbol string, fromTime, toTime, interval int64) ([]Price, error) {
	res := make([]Price, 0)

	query := `
		SELECT * FROM prices 
		WHERE symbol = $1 AND timestamp IN 
		  ( SELECT max(timestamp) FROM prices
			WHERE symbol = $1 AND timestamp < $3
			GROUP BY 
				CASE timestamp > $2
					WHEN TRUE THEN (timestamp - $2)/ $4
					WHEN FALSE THEN -1
				END )
		ORDER BY timestamp
		LIMIT $5
		`

	err := db.Select(&res, query, symbol, fromTime, toTime, interval, limitQuery+1000)

	return res, err
}

type Server struct {
	db *sqlx.DB
}

func main() {
	db, err := NewDB()
	if err != nil {
		log.Fatal("can not connect to db: ", err)
	}

	s := &Server{
		db: db,
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	gin.SetMode(gin.ReleaseMode)

	api := engine.Group("/api")
	api.GET("/", s.helloWorld)

	pprof.Register(engine, "/debug")
	engine.Run("localhost:8001")

}

func removeDup(prices []Price) []Price {
	result := make([]Price, 0)
	for i, p := range prices {
		if !(i > 0 && p.Timestamp == result[len(result)-1].Timestamp) {
			result = append(result, p)
		}
	}
	return result
}

func sum(a []int64) int64 {
	var s int64
	for _, e := range a {
		s += e
	}
	return s
}
func checkErr(c *gin.Context, err error) {
	if err != nil {
		c.JSON(
			http.StatusInternalServerError,
			err.Error(),
		)
	}
}

// true if timestamp + interval > x[id+1].Timestamp
func getNearestPrice(prices []Price, id, timestamp, interval int64) (PriceAPI, bool) {
	if prices[id].Timestamp <= timestamp {
		return PriceAPI{
			Timestamp: timestamp,
			Price:     prices[id].Price,
		}, int(id+1) < len(prices) && interval+timestamp >= prices[id+1].Timestamp
	}
	return PriceAPI{}, false
}

func (s *Server) helloWorld(c *gin.Context) {

	from := c.Query("from")
	fromTime, err := strconv.Atoi(from)
	checkErr(c, err)

	to := c.Query("to")
	toTime, err := strconv.Atoi(to)
	checkErr(c, err)

	interval := c.Query("interval")
	intervalInt, err := strconv.Atoi(interval)
	checkErr(c, err)
	if intervalInt < 100 {
		checkErr(c, errors.New("interval must be > 100"))
		return
	}

	// process to time
	maxToTime := fromTime + limitQuery*intervalInt
	if toTime > maxToTime {
		toTime = maxToTime
	}

	symbol := c.Query("symbol")

	fmt.Println(symbol, fromTime, toTime, interval)

	startTs := time.Now().UnixMilli()
	prices, err := TestGet(s.db, symbol, int64(fromTime), int64(toTime), int64(intervalInt))
	fmt.Println("query data: ", len(prices), "time: ", time.Now().UnixMilli()-startTs)

	// remove duplicate data in test, cause we fake to much of data
	prices = removeDup(prices)

	if len(prices) == 0 {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	var indicator int64 = 0
	result := make([]PriceAPI, 0)
	start := int64(fromTime)
	for {
		if start > int64(toTime) {
			break
		}
		p, isInc := getNearestPrice(prices, indicator, start, int64(intervalInt))
		if p.Timestamp == 0 {
			fmt.Println("Bug vl", start, toTime, prices[0])
		}
		result = append(result, p)
		start += int64(intervalInt)
		if isInc {
			indicator += 1
		}
	}

	fmt.Println("return data time: ", time.Now().UnixMilli()-startTs)

	c.JSON(http.StatusOK, result)

}

// go run ./10-binance-ws/test1/main.go
// min-timestamp = 1654052374471
// http://localhost:8001/api/?symbol=AAVEUSDT&from=1654052380000&to=1654052410000&interval=3000
