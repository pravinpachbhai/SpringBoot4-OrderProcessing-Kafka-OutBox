INSERT INTO users (id, name, email, password, created_at)
VALUES (1, 'Pravin Pachbhai', 'pravin_pachbhai@yahoo.com', '123456', CURRENT_TIMESTAMP),
       (2, 'Raahi Pachbhai', 'raahi_pachbhai@yahoo.com', '12345', CURRENT_TIMESTAMP);

INSERT INTO addresses (id, user_id, address_line, city, state, zip)
VALUES (1, 1, '10475 Garibaldi Pl', 'St Louis', 'MO', '63131'),
       (2, 2, '10475 Garibaldi Pl', 'St Louis', 'MO', '63131');


INSERT INTO products (id, code, name, price)
VALUES (1, 'P001', 'Laptop', 799.99);
INSERT INTO products (id, code, name, price)
VALUES (2, 'P002', 'Smartphone', 599.49);
INSERT INTO products (id, code, name, price)
VALUES (3, 'P003', 'Tablet', 399.00);
INSERT INTO products (id, code, name, price)
VALUES (4, 'P004', 'Monitor', 199.99);
INSERT INTO products (id, code, name, price)
VALUES (5, 'P005', 'Keyboard', 49.99);

INSERT INTO inventory (product_id, available_quantity, reserved_quantity)
VALUES (1, 10, 0),
       (2, 50, 0),
       (3, 30, 0),
       (4, 100, 0),
       (5, 500, 0);