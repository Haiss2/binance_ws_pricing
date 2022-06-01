package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	PostgresHostFlag           = "localhost"
	PostgresPortFlag           = "5432"
	PostgresUserFlag           = "postgres"
	PostgresPasswordFlag       = "postgres"
	PostgresDatabaseFlag       = "pricing_sample"
	_1h                  int64 = 3.6e6
	_2h                  int64 = 7.2e6
	_4h                  int64 = 14.4e6
	_8h                  int64 = 28.8e6
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

// type WsMarketStatEvent struct {
// 	Event              string `json:"e"`
// 	Time               int64  `json:"E"`
// 	Symbol             string `json:"s"`
// 	PriceChange        string `json:"p"`
// 	PriceChangePercent string `json:"P"`
// 	WeightedAvgPrice   string `json:"w"`
// 	PrevClosePrice     string `json:"x"`
// 	LastPrice          string `json:"c"`
// 	CloseQty           string `json:"Q"`
// 	BidPrice           string `json:"b"`
// 	BidQty             string `json:"B"`
// 	AskPrice           string `json:"a"`
// 	AskQty             string `json:"A"`
// 	OpenPrice          string `json:"o"`
// 	HighPrice          string `json:"h"`
// 	LowPrice           string `json:"l"`
// 	BaseVolume         string `json:"v"`
// 	QuoteVolume        string `json:"q"`
// 	OpenTime           int64  `json:"O"`
// 	CloseTime          int64  `json:"C"`
// 	FirstID            int64  `json:"F"`
// 	LastID             int64  `json:"L"`
// 	Count              int64  `json:"n"`
// }

func SavePrices(db *sqlx.DB, prices []*binance.WsMarketStatEvent, str string) error {
	insertParams := []interface{}{}
	query := `INSERT INTO prices(symbol, price, timestamp) VALUES `
	for i, p := range prices {
		p1 := i * 3
		query += fmt.Sprintf("($%d,$%d,$%d),", p1+1, p1+2, p1+3)
		insertParams = append(insertParams, p.Symbol, p.LastPrice, p.Time)
	}
	fmt.Println("inserting...", str)
	query = query[:len(query)-1]
	_, err := db.Exec(query, insertParams...)
	if err != nil {
		fmt.Println("error while saving data: ", err)
	}
	fmt.Println("__inserted__", str)

	return nil
}

func fake24TimesData(prices []*binance.WsMarketStatEvent) []*binance.WsMarketStatEvent {
	result := make([]*binance.WsMarketStatEvent, 0)
	var i int64 = 0
	for i = 0; i < 24; i++ {
		for _, p := range prices {
			result = append(result, &binance.WsMarketStatEvent{
				LastPrice: p.LastPrice,
				Time:      p.Time + i*13,
				Symbol:    p.Symbol,
			})
		}
	}

	return result
}

func testWs(db *sqlx.DB) {

	wsDepthHandler := func(events binance.WsAllMarketsStatEvent) {
		ps := fake24TimesData(events)
		if len(ps) > 21800 {
			var x int = len(ps) / 2
			SavePrices(db, ps[:x], "first batch")
			SavePrices(db, ps[x:], "second batch")
		} else {
			SavePrices(db, ps, "all")
		}

	}
	errHandler := func(err error) {
		fmt.Println(err, "reconnecting...")
		testWs(db)
	}

	doneC, stopC, err := binance.WsAllMarketsStatServe(wsDepthHandler, errHandler)
	if err != nil {
		fmt.Println(err)
		return
	}

	// use stopC to exit
	go func() {
		time.Sleep(2 * time.Hour)
		stopC <- struct{}{}
	}()
	// remove this if you do not want to be blocked here

	<-doneC
}

type Price struct {
	ID        int64  `json:"id"`
	Price     string `json:"price"`
	Symbol    string `json:"symbol"`
	Timestamp int64  `json:"timestamp"`
}

type Server struct {
	db *sqlx.DB
}

func main() {
	db, err := NewDB()
	if err != nil {
		log.Fatal("can not connect to db: ", err)
	}
	testWs(db)
}
