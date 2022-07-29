package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gnatunstyles/wildberries_l0/models"
	_ "github.com/lib/pq"
	"github.com/lithammer/shortuuid/v4"
	stan "github.com/nats-io/stan.go"
)

// type Cache interface {
// 	Set(key string, value interface{}, ttl time.Duration)
// 	Get(key string) (interface{}, bool)
// 	Delete(key string)
// }

const (
	msg = `{
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
	msg1 = `{
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
	msg2 = `{
		"order_uid": "b563feb7b2123123b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": {
		  "name": "John Doe",
		  "phone": "+79112345678",
		  "zip": "188505",
		  "city": "Annino",
		  "address": "10 Pyatiletki 15",
		  "region": "Leningradskaya Oblast",
		  "email": "test@gmail.com"
		},
		"payment": {
		  "transaction": "b563feb7b2123123b84b6test",
		  "request_id": "321321231",
		  "currency": "USD",
		  "provider": "wbpay",
		  "amount": 1650,
		  "payment_dt": 1637907727,
		  "bank": "alpha",
		  "delivery_cost": 1500,
		  "goods_total": 150,
		  "custom_fee": 0
		},
		"items": [
		  {
			"chrt_id": 312312123,
			"track_number": "WBILMTESTTRACK",
			"price": 150,
			"rid": "ab4219087a764ae0btest",
			"name": "Air Force 1",
			"sale": 0,
			"size": "0",
			"total_price": 150,
			"nm_id": 238ASD2,
			"brand": "Nike",
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
	msg3 = `{
		"order_uid": "111b563feb7b2b84b6test",
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
		  "transaction": "111b563feb7b2b84b6test",
		  "request_id": "321321231",
		  "currency": "EUR",
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
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "db"
	sqlOrder = `INSERT INTO orders (uuid, order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	sqlDelivery = `INSERT INTO delivery (uuid, name, phone, zip, city, address, region, email)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	sqlItem = `INSERT INTO items (uuid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	sqlPayment = `INSERT INTO payment (uuid, transaction, request_id, currency, city, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
)

func main() {
	testQueue := []string{msg1, msg2, msg3}

	// c , err := cache
	//db init
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Error. Cannot reach db")
	}

	//Pinging db
	err = db.Ping()
	fmt.Println("db connected")
	if err != nil {
		log.Fatal("Error. Cannot ping db")
	}
	fmt.Println("db pinged")

	sc, err := stan.Connect("test-cluster", "event-store", stan.NatsURL(stan.DefaultNatsURL))
	if err != nil {
		log.Print(err)
		return
	}
	defer sc.Close()

	fmt.Println("nats connected")

	router := gin.Default()

	router.GET("message/:id", getMsgById)

	router.Run(":8080")

	//cache init

	go func() {
		_, err := sc.QueueSubscribe("channel", "queue", func(msg *stan.Msg) {
			log.Printf("Received on [%s] Queue[%s] Pid[%d]: '%s'", msg.Subject, msg.Sub, os.Getpid(), string(msg.Data))
			storeMsg(db, msg)
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	for i := range testQueue {
		time.Sleep(5 * time.Millisecond)
		err := sc.Publish("channel", []byte(testQueue[i]))
		if err != nil {
			log.Fatalf("Error through message publishing: %s", err)
		}

	}
}

func getMsgById(c *gin.Context) {
	// db = dbname
	// id := c.Param("id")
	// sqlQuery := `select from orders where order_uid = '$1'`
	// _, err := db.Exec()

}

func storeMsg(db *sql.DB, msg *stan.Msg) error {
	var order *models.Order

	err := json.Unmarshal([]byte(msg.Data), &order)
	if err != nil {
		log.Fatal("Error. Wrong type of incoming data.")
	}

	fmt.Println(order.Payment.Amount)
	deliveryUuid := shortuuid.New()

	_, err = db.Exec(sqlDelivery, deliveryUuid, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)

	if err != nil {
		log.Fatalf("Error during saving delivery: %s", err)
	}

	itemUuidList := []string{}

	for i := range order.Items {
		itemUuid := shortuuid.New()
		_, err = db.Exec(sqlItem, itemUuid, order.Items[i].ChrtId, order.Items[i].TrackNumber,
			order.Items[i].Price, order.Items[i].Rid, order.Items[i].Name,
			order.Items[i].Sale, order.Items[i].Size, order.Items[i].TotalPrice,
			order.Items[i].NmId, order.Items[i].Brand, order.Items[i].Status)
		if err != nil {
			log.Fatalf("Error during saving item: %s", err)
		}
		itemUuidList = append(itemUuidList, itemUuid)
	}

	paymentUuid := shortuuid.New()
	_, err = db.Exec(sqlPayment, paymentUuid, order.Payment.Transaction, order.Payment.RequestId, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt, order.Payment.Bank,
		order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee)
	if err != nil {
		log.Fatalf("Error during saving payment: %s", err)
	}

	_, err = db.Exec(sqlOrder, order.OrderUid, order.TrackNumber, order.Entry,
		deliveryUuid, paymentUuid, itemUuidList, order.Locale,
		order.InternalSignature, order.CustomerId, order.DeliveryService,
		order.Shardkey, order.SmId, order.DateCreated, order.OofShard)
	if err != nil {
		log.Fatalf("Error during saving order: %s", err)
	}

	log.Println("Message was saved to db successfully.")
	return nil
}

func formCache() {

}
