package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/ratmirtech/techwb-l0/internal/models"
	"github.com/segmentio/kafka-go"
)

func env(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func main() {
	_ = godotenv.Load()

	brokers := env("KAFKA_BROKERS", "kafka:9092")
	topic := env("KAFKA_TOPIC", "orders")

	order := generateOrder()
	msg, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("Can't make JSON: %v", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
		BatchTimeout: 200 * time.Millisecond,
	}
	defer writer.Close()

	if err := writer.WriteMessages(context.Background(),
		kafka.Message{Value: msg}); err != nil {
		log.Fatalf("Can't send to Kafka: %v", err)
	}
	log.Printf("Sent order with ID: %s", order.OrderUID)
}

func generateOrder() models.Order {
	uid := uuid.New().String()[:16] + "test"
	trackNumber := "TRACK" + uuid.New().String()[:8]
	now := time.Now().UTC()

	return models.Order{
		OrderUID:    uid,
		TrackNumber: trackNumber,
		Entry:       "TEST",
		Delivery: models.Delivery{
			Name:    "User",
			Phone:   "+79001234567",
			Zip:     "123456",
			City:    "Test City",
			Address: "Street 1",
			Region:  "Test Region",
			Email:   "user@test.com",
		},
		Payment: models.Payment{
			Transaction:  uid,
			Currency:     "USD",
			Provider:     "testpay",
			Amount:       1000,
			PaymentDt:    time.Now().Unix(),
			Bank:         "testbank",
			DeliveryCost: 200,
			GoodsTotal:   800,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      1000000 + rand.Intn(1000000),
				TrackNumber: trackNumber,
				Price:       500,
				RID:         "",
				Name:        "Product",
				Sale:        10,
				Size:        "M",
				TotalPrice:  450,
				NmID:        2000000 + rand.Intn(1000000),
				Brand:       "Test Brand",
				Status:      200,
			},
		},
		Locale:          "en",
		CustomerID:      "customer1",
		DeliveryService: "testdelivery",
		Shardkey:        "1",
		SmID:            rand.Intn(100),
		DateCreated:     now,
		OofShard:        "1",
	}
}
