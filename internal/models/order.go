package models

import (
	"encoding/json"
	"time"
)

type Order struct {
	OrderUID          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Delivery          Delivery  `json:"delivery"`
	Payment           Payment   `json:"payment"`
	Items             []Item    `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

func (o *Order) UnmarshalJSON(b []byte) error {
	type alias Order
	aux := struct {
		DateCreated string `json:"date_created"`
		*alias
	}{alias: (*alias)(o)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if aux.DateCreated != "" {
		t, err := time.Parse(time.RFC3339, aux.DateCreated)
		if err != nil {
			return err
		}
		o.DateCreated = t
	}
	return nil
}

func (o Order) MarshalJSON() ([]byte, error) {
	type alias Order
	return json.Marshal(struct {
		DateCreated string `json:"date_created"`
		alias
	}{
		DateCreated: o.DateCreated.UTC().Format(time.RFC3339),
		alias:       (alias)(o),
	})
}
