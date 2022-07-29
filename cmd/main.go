package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gnatunstyles/wildberries_l0/cache"
	"github.com/gnatunstyles/wildberries_l0/db"
	"github.com/gnatunstyles/wildberries_l0/models"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/lithammer/shortuuid/v4"
	stan "github.com/nats-io/stan.go"
)

var (
	DB    *sql.DB
	Cache *cache.Cache
)

const (
	sqlOrder = `INSERT INTO orders (order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);`
	sqlDelivery = `INSERT INTO delivery (uuid, name, phone, zip, city, address, region, email)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
	sqlItem = `INSERT INTO items (uuid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);`
	sqlPayment = `INSERT INTO payment (uuid, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`
	defaultExpiration = time.Minute * 30
)

func main() {
	// testQueue := []string{msg1, msg, msg3}

	//cache init
	Cache = cache.New(defaultExpiration, time.Minute*1)

	//db init

	DB, err := db.InitDB()
	if err != nil {
		log.Print(err)
		return
	}

	sc, err := stan.Connect("test-cluster", "event-store", stan.NatsURL(stan.DefaultNatsURL))
	if err != nil {
		log.Print(err)
		return
	}
	defer sc.Close()

	fmt.Println("nats connected")

	formCache(Cache, DB)

	router := gin.Default()

	router.GET("message/:id", getMsgById)

	go func() {
		_, err := sc.QueueSubscribe("channel", "queue", func(msg *stan.Msg) {
			log.Printf("Received on [%s] Queue[%s] Pid[%d]: '%s'", msg.Subject, msg.Sub, os.Getpid(), string(msg.Data))
			storeMsg(Cache, DB, msg)
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	router.Run(":8080")

}

func getMsgById(c *gin.Context) {
	id := c.Param("id")
	result, exist := Cache.Get(id)
	if exist {
		c.JSON(http.StatusOK, gin.H{"code": 200, "order_info": result})
	} else {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "order with this id not found"})
	}
}

func storeMsg(c *cache.Cache, db *sql.DB, msg *stan.Msg) error {
	var order *models.Order

	err := json.Unmarshal([]byte(msg.Data), &order)
	if err != nil {
		log.Fatal("Error. Wrong type of incoming data.")
	}

	_, exist := c.Get(order.OrderUid)

	if !exist {
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
			deliveryUuid, paymentUuid, pq.Array(itemUuidList), order.Locale,
			order.InternalSignature, order.CustomerId, order.DeliveryService,
			order.Shardkey, order.SmId, order.DateCreated, order.OofShard)
		if err != nil {
			log.Fatalf("Error during saving order: %s", err)
		}

		Cache.Set(order.OrderUid, order, defaultExpiration)
		log.Println("Message was saved to db successfully.")
		return nil
	}
	log.Println("This message already in db.")
	return nil
}

func formCache(c *cache.Cache, db *sql.DB) {
	rows, err := db.Query("SELECT * FROM orders;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var order *models.Order
		if err := rows.Scan(&order); err != nil {
			log.Printf("Error through forming cache from db: %s", err)
			return
		}
		c.Set(order.OrderUid, order, defaultExpiration)
	}
}
