# Customer order snippets

## customer-rows

```sql
SELECT id, name, email, level
FROM customers
WHERE lower(email) = :customerEmailKey;
```

## orders-by-customer

```sql
select id, customer_id, order_no, status, created_at
from orders
where customer_id = @customerId
@if orderStatus != "" {
  and status = @orderStatus
}
order by id
```

## order-items-by-order-ids

```sql
SELECT id, order_id, product_id, quantity, unit_price, quantity * unit_price AS amount
FROM order_items
where
@if len(orderIds) > 0 {
  order_id in (@orderIds)
} else {
  1 = 0
}
ORDER BY id;
```

## products-by-product-ids

```sql
SELECT id, name, price
FROM products
where
@if len(productIds) > 0 {
  id in (@productIds)
} else {
  1 = 0
}
ORDER BY id;
```
