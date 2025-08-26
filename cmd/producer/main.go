package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	_ = godotenv.Load()

	brokers := env("KAFKA_BROKERS", "localhost:9092")
	topic := env("KAFKA_TOPIC", "orders")

	payloadPath := ""
	if len(os.Args) > 1 {
		payloadPath = os.Args[1]
	}
	var msg []byte
	var err error
	if payloadPath != "" {
		msg, err = os.ReadFile(payloadPath)
		if err != nil {
			log.Fatalf("read payload: %v", err)
		}
	} else {
		// минимальный валидный пример
		msg = []byte(`{
		 "order_uid":"b563feb7b2b84b6test",
		 "track_number":"WBILMTESTTRACK",
		 "entry":"WBIL",
		 "delivery":{"name":"Test Testov","phone":"+9720000000","zip":"2639809","city":"Kiryat Mozkin","address":"Ploshad Mira 15","region":"Kraiot","email":"test@gmail.com"},
		 "payment":{"transaction":"b563feb7b2b84b6test","request_id":"","currency":"USD","provider":"wbpay","amount":1817,"payment_dt":1637907727,"bank":"alpha","delivery_cost":1500,"goods_total":317,"custom_fee":0},
		 "items":[{"chrt_id":9934930,"track_number":"WBILMTESTTRACK","price":453,"rid":"ab4219087a764ae0btest","name":"Mascaras","sale":30,"size":"0","total_price":317,"nm_id":2389212,"brand":"Vivienne Sabo","status":202}],
		 "locale":"en","internal_signature":"","customer_id":"test","delivery_service":"meest","shardkey":"9","sm_id":99,"date_created":"2021-11-26T06:22:19Z","oof_shard":"1"
		}`)
	}

	// быстрая проверка на JSON
	var tmp map[string]any
	if err := json.Unmarshal(msg, &tmp); err != nil {
		log.Fatalf("invalid json: %v", err)
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(splitComma(brokers)...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
		BatchTimeout: 200 * time.Millisecond,
	}
	defer w.Close()

	if err := w.WriteMessages(context.Background(),
		kafka.Message{Value: msg}); err != nil {
		log.Fatalf("write: %v", err)
	}
	log.Println("sent")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitComma(s string) []string {
	var out []string
	for _, p := range []rune(s) {
		_ = p
	}
	var cur string
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
