package data

import (
	"context"
	"database/sql"
	"fmt"
)

func EnsureDemoData(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    tenant TEXT NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL
);`,
		`
INSERT OR IGNORE INTO users (id, tenant, name, email, status) VALUES
    (1, 'ACME', 'Alice Zhang', 'alice@acme.ai', 'active'),
    (2, 'ACME', 'Bob Li', 'bob@acme.ai', 'active'),
    (3, 'LABS', 'Carol Wu', 'carol@labs.ai', 'inactive'),
    (4, 'LABS', 'David Chen', 'david@labs.ai', 'active');`,
		`
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    name TEXT NOT NULL
);`,
		`
INSERT OR IGNORE INTO categories (id, parent_id, name) VALUES
    (1, 0, 'AI'),
    (2, 1, 'Agents'),
    (3, 2, 'Planning'),
    (4, 2, 'Execution'),
    (5, 1, 'Workflows');`,
		`
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    level TEXT NOT NULL
);`,
		`
INSERT OR IGNORE INTO customers (id, name, email, level) VALUES
    (1, 'Alice Future', 'alice.future@demo.ai', 'gold'),
    (2, 'Bob Labs', 'bob.labs@demo.ai', 'silver');`,
		`
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL
);`,
		`
INSERT OR IGNORE INTO products (id, name, price) VALUES
    (1, 'Copilot Seat', 99),
    (2, 'Workflow Engine', 249),
    (3, 'Knowledge Pack', 49);`,
		`
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_no TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);`,
		`
INSERT OR IGNORE INTO orders (id, customer_id, order_no, status, created_at) VALUES
    (1, 1, 'SO-1001', 'paid', '2026-04-01T10:00:00Z'),
    (2, 1, 'SO-1002', 'pending', '2026-04-03T14:30:00Z'),
    (3, 2, 'SO-1003', 'paid', '2026-04-04T09:15:00Z');`,
		`
CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL
);`,
		`
INSERT OR IGNORE INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 2, 99),
    (2, 1, 3, 1, 49),
    (3, 2, 2, 1, 249),
    (4, 3, 1, 1, 99),
    (5, 3, 2, 1, 249);`,
	}

	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("exec bootstrap statement: %w", err)
		}
	}

	return nil
}
