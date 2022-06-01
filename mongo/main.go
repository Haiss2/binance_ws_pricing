package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/jmoiron/sqlx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	_1h int64 = 3.6e6
	_2h int64 = 7.2e6
	_4h int64 = 14.4e6
	_8h int64 = 28.8e6
)

func toFloat(n string) float64 {
	r, _ := strconv.ParseFloat(n, 64)
	return r
}

func SavePrices(db *mongo.Database, prices []*binance.WsMarketStatEvent) error {
	pMap := toPriceMap(prices)
	opts := options.InsertMany().SetOrdered(false)
	for sym, val := range pMap {
		go func(sym string, val []interface{}) {
			_, err := db.Collection(sym).InsertMany(context.Background(), val, opts)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Println("inserted", sym, len(res.InsertedIDs))
		}(sym, val)

	}
	return nil
}

func fake24TimesData(prices []*binance.WsMarketStatEvent) []*binance.WsMarketStatEvent {
	result := make([]*binance.WsMarketStatEvent, 0)
	var i int64 = 0
	for i = 0; i < 12; i++ {
		for _, p := range prices {
			result = append(result, &binance.WsMarketStatEvent{
				LastPrice: p.LastPrice,
				Time:      p.Time + i*_8h,
				Symbol:    p.Symbol,
			})
		}
	}

	return result
}

func testWs(db *mongo.Database) {

	wsDepthHandler := func(events binance.WsAllMarketsStatEvent) {
		fmt.Println("received", len(events), "symbols change")
		ps := fake24TimesData(events)
		SavePrices(db, ps)
		fmt.Println("inserted", runtime.NumGoroutine(), "active goroutine")
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
		time.Sleep(60 * time.Minute)
		stopC <- struct{}{}
	}()
	// remove this if you do not want to be blocked here

	<-doneC
}

type PriceAPI struct {
	Price     float64 `json:"price" bson:"price"`
	Timestamp int64   `json:"timestamp" bson:"timestamp"`
}

func toPriceMap(ps []*binance.WsMarketStatEvent) map[string][]interface{} {
	pMap := make(map[string][]interface{}, 0)
	for _, p := range ps {
		_, ok := pMap[p.Symbol]
		if !ok {
			pMap[p.Symbol] = []interface{}{}
		} else {
			pMap[p.Symbol] = append(pMap[p.Symbol], bson.D{{"price", toFloat(p.LastPrice)}, {"timestamp", p.Time}})
		}
	}
	return pMap
}

type Server struct {
	db *sqlx.DB
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}

	db := client.Database("pricing_sample")
	testWs(db)
}
