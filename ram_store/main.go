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
	PostgresHostFlag         = "localhost"
	PostgresPortFlag         = "5432"
	PostgresUserFlag         = "postgres"
	PostgresPasswordFlag     = "postgres"
	PostgresDatabaseFlag     = "pricing_sample"
	limitQuery           int = 5000
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

func loadData() ([]Price, error) {
	var res []Price
	db, err := NewDB()
	if err != nil {
		return nil, err
	}
	fmt.Println("start loading data from db ...")
	ts := time.Now().UnixMilli()
	err = db.Select(&res, "SELECT * FROM prices ORDER BY timestamp")
	if err != nil {
		return nil, err
	}
	fmt.Println("load success, time: ", time.Now().UnixMilli()-ts)
	fmt.Println("number of records: ", len(res))
	return res, err
}

func filterBySymbol(prices []Price, s string) []Price {
	result := make([]Price, 0)
	for _, p := range prices {
		if p.Symbol == s {
			result = append(result, p)
		}
	}
	return result
}

type Price struct {
	Price     float64 `json:"price"`
	Symbol    string  `json:"symbol"`
	Timestamp int     `json:"timestamp"`
}

type PriceAPI struct {
	Price     float64 `json:"price"`
	Timestamp int     `json:"timestamp"`
}

type Server struct {
	ps  []Price
	len int
}

func (s *Server) getPriceAtTs(prices []Price, id, timestamp, interval int) (PriceAPI, int) {
	if timestamp >= prices[id].Timestamp {
		for index := id; index < len(prices)-1; index++ {
			if prices[index+1].Timestamp > timestamp {
				return PriceAPI{
					Timestamp: timestamp,
					Price:     prices[index].Price,
				}, index
			}
		}
	}

	return PriceAPI{
		Timestamp: timestamp,
	}, 0
}

func (s *Server) doQuery(prices []Price, symbol string, from, to, interval int) []PriceAPI {
	var index int = 0
	result := make([]PriceAPI, 0)
	timestamp := from
	for {
		if timestamp > to {
			break
		}
		p, newIndex := s.getPriceAtTs(prices, index, timestamp, interval)
		result = append(result, p)
		timestamp += interval
		index = newIndex
	}
	return result
}

func main() {
	ps, err := loadData()
	if err != nil {
		log.Fatal(err)
	}
	s := NewServer(ps)

	engine := gin.New()
	engine.Use(gin.Recovery())
	gin.SetMode(gin.ReleaseMode)

	api := engine.Group("/api")
	api.GET("/", s.getPrices)

	pprof.Register(engine, "/debug")
	engine.Run("localhost:8001")
}

func NewServer(ps []Price) *Server {
	return &Server{ps, len(ps)}
}

func (s *Server) getPrices(c *gin.Context) {

	from := c.Query("from")
	fromTime, err := strconv.Atoi(from)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	to := c.Query("to")
	toTime, err := strconv.Atoi(to)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	interval := c.Query("interval")
	intervalInt, err := strconv.Atoi(interval)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	if intervalInt < 10 {
		if err != nil {
			c.JSON(http.StatusInternalServerError, errors.New("interval must be greater than 100"))
			return
		}
	}

	// process to time
	maxToTime := fromTime + limitQuery*intervalInt
	if toTime > maxToTime {
		toTime = maxToTime
	}

	symbol := c.Query("symbol")

	/* Validated and Preprocessed input data successfully*/
	fmt.Println(symbol, fromTime, toTime, interval)

	startTs := time.Now().UnixMilli()
	prices := filterBySymbol(s.ps, symbol)
	fmt.Println("filterBySymbol: ", len(prices), "time: ", time.Now().UnixMilli()-startTs)

	var priceAPIs = make([]PriceAPI, 0)
	if len(prices) > 0 {
		startTs = time.Now().UnixMilli()
		priceAPIs = s.doQuery(prices, symbol, fromTime, toTime, intervalInt)
		fmt.Println("filterByTimestamp: ", len(priceAPIs), "time: ", time.Now().UnixMilli()-startTs)
	}

	c.JSON(http.StatusOK, priceAPIs)
}

// go run ./10-binance-ws/ram_store
// http://localhost:8001/api/?symbol=AAVEUSDT&from=1654052380000&to=1654052410000&interval=3000
