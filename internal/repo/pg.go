package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/ratmirtech/techwb-l0/internal/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PG struct {
	db *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*PG, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PG{db: pool}, nil
}

func (p *PG) Close() { p.db.Close() }

func (p *PG) Migrate(ctx context.Context) error {
	sql := `
CREATE TABLE IF NOT EXISTS orders(
	order_uid TEXT PRIMARY KEY,
	track_number TEXT,
	entry TEXT,
	locale TEXT,
	internal_signature TEXT,
	customer_id TEXT,
	delivery_service TEXT,
	shardkey TEXT,
	sm_id INT,
	date_created TIMESTAMPTZ,
	oof_shard TEXT
);

CREATE TABLE IF NOT EXISTS deliveries(
	order_uid TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
	name TEXT, phone TEXT, zip TEXT, city TEXT, address TEXT, region TEXT, email TEXT
);

CREATE TABLE IF NOT EXISTS payments(
	order_uid TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
	transaction TEXT, request_id TEXT, currency TEXT, provider TEXT,
	amount INT, payment_dt BIGINT, bank TEXT, delivery_cost INT, goods_total INT, custom_fee INT
);

CREATE TABLE IF NOT EXISTS items(
	id SERIAL PRIMARY KEY,
	order_uid TEXT REFERENCES orders(order_uid) ON DELETE CASCADE,
	chrt_id INT, track_number TEXT, price INT, rid TEXT, name TEXT, sale INT,
	size TEXT, total_price INT, nm_id INT, brand TEXT, status INT
);
`
	_, err := p.db.Exec(ctx, sql)
	return err
}

func (p *PG) UpsertOrder(ctx context.Context, o models.Order) error {
	tx, err := p.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// orders
	_, err = tx.Exec(ctx, `
INSERT INTO orders(order_uid, track_number, entry, locale, internal_signature,
                   customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (order_uid) DO UPDATE SET
 track_number=EXCLUDED.track_number, entry=EXCLUDED.entry, locale=EXCLUDED.locale,
 internal_signature=EXCLUDED.internal_signature, customer_id=EXCLUDED.customer_id,
 delivery_service=EXCLUDED.delivery_service, shardkey=EXCLUDED.shardkey, sm_id=EXCLUDED.sm_id,
 date_created=EXCLUDED.date_created, oof_shard=EXCLUDED.oof_shard
`, o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature,
		o.CustomerID, o.DeliveryService, o.Shardkey, o.SmID, o.DateCreated, o.OofShard)
	if err != nil {
		return fmt.Errorf("orders upsert: %w", err)
	}

	// delivery
	d := o.Delivery
	_, err = tx.Exec(ctx, `
INSERT INTO deliveries(order_uid, name, phone, zip, city, address, region, email)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (order_uid) DO UPDATE SET
 name=EXCLUDED.name, phone=EXCLUDED.phone, zip=EXCLUDED.zip, city=EXCLUDED.city,
 address=EXCLUDED.address, region=EXCLUDED.region, email=EXCLUDED.email
`, o.OrderUID, d.Name, d.Phone, d.Zip, d.City, d.Address, d.Region, d.Email)
	if err != nil {
		return fmt.Errorf("deliveries upsert: %w", err)
	}

	// payment
	pay := o.Payment
	_, err = tx.Exec(ctx, `
INSERT INTO payments(order_uid, transaction, request_id, currency, provider, amount, payment_dt,
                     bank, delivery_cost, goods_total, custom_fee)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (order_uid) DO UPDATE SET
 transaction=EXCLUDED.transaction, request_id=EXCLUDED.request_id, currency=EXCLUDED.currency,
 provider=EXCLUDED.provider, amount=EXCLUDED.amount, payment_dt=EXCLUDED.payment_dt,
 bank=EXCLUDED.bank, delivery_cost=EXCLUDED.delivery_cost, goods_total=EXCLUDED.goods_total,
 custom_fee=EXCLUDED.custom_fee
`, o.OrderUID, pay.Transaction, pay.RequestID, pay.Currency, pay.Provider, pay.Amount,
		pay.PaymentDt, pay.Bank, pay.DeliveryCost, pay.GoodsTotal, pay.CustomFee)
	if err != nil {
		return fmt.Errorf("payments upsert: %w", err)
	}

	// items: проще всего пересоздать
	_, err = tx.Exec(ctx, `DELETE FROM items WHERE order_uid=$1`, o.OrderUID)
	if err != nil {
		return fmt.Errorf("items delete: %w", err)
	}
	for _, it := range o.Items {
		_, err = tx.Exec(ctx, `
INSERT INTO items(order_uid, chrt_id, track_number, price, rid, name, sale, size,
                  total_price, nm_id, brand, status)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
`, o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.RID, it.Name, it.Sale,
			it.Size, it.TotalPrice, it.NmID, it.Brand, it.Status)
		if err != nil {
			return fmt.Errorf("items insert: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (p *PG) GetOrder(ctx context.Context, id string) (models.Order, error) {
	var o models.Order
	row := p.db.QueryRow(ctx, `
SELECT order_uid, track_number, entry, locale, internal_signature, customer_id,
       delivery_service, shardkey, sm_id, date_created, oof_shard
FROM orders WHERE order_uid=$1`, id)

	err := row.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
		&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard)
	if err != nil {
		return o, err
	}

	// delivery
	err = p.db.QueryRow(ctx, `
SELECT name, phone, zip, city, address, region, email FROM deliveries WHERE order_uid=$1`, id).
		Scan(&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City,
			&o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
	if err != nil {
		return o, err
	}

	// payment
	err = p.db.QueryRow(ctx, `
SELECT transaction, request_id, currency, provider, amount, payment_dt, bank,
       delivery_cost, goods_total, custom_fee
FROM payments WHERE order_uid=$1`, id).
		Scan(&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency, &o.Payment.Provider,
			&o.Payment.Amount, &o.Payment.PaymentDt, &o.Payment.Bank, &o.Payment.DeliveryCost,
			&o.Payment.GoodsTotal, &o.Payment.CustomFee)
	if err != nil {
		return o, err
	}

	// items
	rows, err := p.db.Query(ctx, `
SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
FROM items WHERE order_uid=$1 ORDER BY id`, id)
	if err != nil {
		return o, err
	}
	defer rows.Close()
	for rows.Next() {
		var it models.Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.RID, &it.Name, &it.Sale,
			&it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			return o, err
		}
		o.Items = append(o.Items, it)
	}
	return o, nil
}

func (p *PG) GetAllOrders(ctx context.Context) ([]models.Order, error) {
	rows, err := p.db.Query(ctx, `SELECT order_uid FROM orders`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	var out []models.Order
	for _, id := range ids {
		o, err := p.GetOrder(ctx, id)
		if err != nil {
			// если кто-то удалил подвязанные строки — пропускаем
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}
