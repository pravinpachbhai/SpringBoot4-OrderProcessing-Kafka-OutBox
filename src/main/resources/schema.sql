CREATE TABLE outbox_events (
                               id UUID PRIMARY KEY,
                               aggregate_type VARCHAR(100),
                               aggregate_id VARCHAR(255),
                               event_type VARCHAR(100),
                               payload TEXT,
                               status VARCHAR(20), -- NEW, PUBLISHED, FAILED
                               created_at TIMESTAMP,
                               updated_at TIMESTAMP,
                               processed_at TIMESTAMP
);

CREATE TABLE processed_events (
                                  event_id VARCHAR(255) PRIMARY KEY,
                                  processed_at TIMESTAMP
);

CREATE INDEX idx_outbox_status ON outbox_events(status);

CREATE TABLE users (
                       id BIGINT AUTO_INCREMENT PRIMARY KEY,
                       name VARCHAR(255),
                       email VARCHAR(255) UNIQUE NOT NULL,
                       password VARCHAR(255) NOT NULL,
                       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE addresses (
                           id BIGINT AUTO_INCREMENT PRIMARY KEY,
                           user_id BIGINT,
                           address_line VARCHAR(255),
                           city VARCHAR(100),
                           state VARCHAR(100),
                           zip VARCHAR(20),
                           FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE orders (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        status VARCHAR(50),
                        total_amount DECIMAL(10,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
                             id BIGINT AUTO_INCREMENT PRIMARY KEY,
                             order_id BIGINT,
                             product_id BIGINT,
                             quantity INT,
                             price DECIMAL(10,2),
                             FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE products (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          code VARCHAR(50) NOT NULL,
                          name VARCHAR(150) NOT NULL,
                          price DECIMAL(10,2) NOT NULL,
                          created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          UNIQUE (code)
);

CREATE TABLE inventory (
                           product_id BIGINT PRIMARY KEY,
                           available_quantity INT,
                           reserved_quantity INT,
                           FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE payments (
                          id BIGINT AUTO_INCREMENT PRIMARY KEY,
                          order_id BIGINT,
                          status VARCHAR(50),
                          amount DECIMAL(10,2),
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE shipments (
                           id BIGINT AUTO_INCREMENT PRIMARY KEY,
                           order_id BIGINT,
                           status VARCHAR(50),
                           tracking_number VARCHAR(100),
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE notifications (
                               id BIGINT AUTO_INCREMENT PRIMARY KEY,
                               type VARCHAR(100),
                               recipient VARCHAR(255),
                               status VARCHAR(50),
                               message VARCHAR(1000),
                               sent_at TIMESTAMP
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);
