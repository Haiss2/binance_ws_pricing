package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	PostgresHostFlag           = "localhost"
	PostgresPortFlag           = "5432"
	PostgresUserFlag           = "postgres"
	PostgresPasswordFlag       = "postgres"
	PostgresDatabaseFlag       = "pricing_sample_2"
	smallestTime         int64 = 1653548647000
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
	Price     string `json:"price"`
	Timestamp int64  `json:"timestamp"`
}

func TestGet(db *sqlx.DB, sym string, from, to, interval int64) ([]Price, error) {
	res := make([]Price, 0)

	query := fmt.Sprintf(`SELECT * FROM "%s" WHERE timestamp %% %d = 0`, sym, interval*1000)
	fmt.Println(query)
	err := db.Select(&res, query)

	return res, err
}
func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}

type Server struct {
	db *sqlx.DB
}

func main() {
	db, err := NewDB()
	if err != nil {
		log.Fatal("can not connect to db: ", err)
	}

	symbols := []string{"ETHUSDC", "GMTUSDT", "IMXBUSD", "JOEBTC"}
	intervals := []int64{1, 2, 5, 30}

	times := make([]int64, 0)

	for i := 0; i < 4; i++ {
		now := time.Now().UnixMilli()
		_, _ = TestGet(db, symbols[i], 100, 1663379200000, intervals[i])
		times = append(times, time.Now().UnixMilli()-now)
	}

	fmt.Println(times)
	fmt.Println("sum: ", sum(times))

	// s := &Server{
	// 	db: db,
	// }

	// engine := gin.New()
	// engine.Use(gin.Recovery())
	// gin.SetMode(gin.ReleaseMode)

	// api := engine.Group("/api")
	// api.GET("/", s.helloWorld)
	// api.GET("/a", s.helloWorld2)

	// pprof.Register(engine, "/debug")
	// engine.Run("localhost:8001")

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

func trunc(x int64) int64 {
	var y int64 = x / 1000
	return y * 1000
}

func getFromTo(from, to int64) (x, y int64) {
	x = from
	if x < smallestTime {
		x = smallestTime
	}
	y = to
	now := trunc(time.Now().UnixMilli())
	if y > now {
		y = now
	}
	return
}

func getTs(from, to, interval int64) []int64 {
	result := make([]int64, 0)
	x := from
	for {
		result = append(result, x)
		x += interval
		if x > to {
			break
		}
	}
	return result
}

func (s *Server) helloWorld(c *gin.Context) {
	symbols := []string{"ETHUSDC", "GMTUSDT", "IMXBUSD", "JOEBTC"}
	intervals := []int64{1, 2, 5, 30}

	times := make([]int64, 0)

	for i := 0; i < 4; i++ {
		now := time.Now().UnixMilli()
		_, _ = TestGet(s.db, symbols[i], 100, 1663379200000, intervals[i])
		times = append(times, time.Now().UnixMilli()-now)
	}

	fmt.Println(times)
	fmt.Println("sum: ", sum(times))
	c.JSON(http.StatusOK, int(sum(times))/len(times))

}

func (s *Server) helloWorld2(c *gin.Context) {

	now := time.Now().UnixMilli()
	res, _ := TestGet(s.db, "BTCUSDT", 100, 1663379200000, 1)

	fmt.Println(time.Now().UnixMilli() - now)
	c.JSON(http.StatusOK, res)

}
