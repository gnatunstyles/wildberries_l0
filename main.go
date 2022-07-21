package main

import (
	"database/sql"
	"fmt"

	"log"
	"time"

	_ "github.com/lib/pq"
)

type Cache interface {
	Set(key string, value interface{}, ttl time.Duration)
	Get(key string) (interface{}, bool)
	Delete(key string)
}

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "db"
	msg      = `{
		"order_uid": "b563feb7b2b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": {
		  "name": "Test Testov",
		  "phone": "+9720000000",
		  "zip": "2639809",
		  "city": "Kiryat Mozkin",
		  "address": "Ploshad Mira 15",
		  "region": "Kraiot",
		  "email": "test@gmail.com"
		},
		"payment": {
		  "transaction": "b563feb7b2b84b6test",
		  "request_id": "321321231",
		  "currency": "USD",
		  "provider": "wbpay",
		  "amount": 1817,
		  "payment_dt": 1637907727,
		  "bank": "alpha",
		  "delivery_cost": 1500,
		  "goods_total": 317,
		  "custom_fee": 0
		},
		"items": [
		  {
			"chrt_id": 9934930,
			"track_number": "WBILMTESTTRACK",
			"price": 453,
			"rid": "ab4219087a764ae0btest",
			"name": "Mascaras",
			"sale": 30,
			"size": "0",
			"total_price": 317,
			"nm_id": 2389212,
			"brand": "Vivienne Sabo",
			"status": 202
		  }
		],
		"locale": "en",
		"internal_signature": "",
		"customer_id": "test",
		"delivery_service": "meest",
		"shardkey": "9",
		"sm_id": 99,
		"date_created": "2021-11-26T06:22:19Z",
		"oof_shard": "1"
	  }`
)

func main() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Error. Cannot reach db")
	}

	err = db.Ping()
	fmt.Println("db connected")
	if err != nil {
		log.Fatal("Error. Cannot ping db")
	}
	fmt.Println("db pinged")

	// sc, err := stan.Connect("test-cluster", "event-store", stan.NatsURL(stan.DefaultNatsURL))
	// if err != nil {
	// 	log.Print(err)
	// 	return
	// }
	// defer sc.Close()

	// fmt.Println("nats connected")

	// // Simple Synchronous Publisher
	// sc.Publish("foo", []byte(msg)) // does not return until an ack has been received from NATS Streaming

	// // Simple Async Subscriber
	// sub, _ := sc.Subscribe("foo", func(m *stan.Msg) {
	// 	fmt.Printf("Received a message: %s\n", string(m.Data))
	// })
	// _ = sub

}
