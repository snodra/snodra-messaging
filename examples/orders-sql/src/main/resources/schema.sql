CREATE TABLE IF NOT EXISTS orders (
    id     VARCHAR(64) PRIMARY KEY,
    item   TEXT        NOT NULL,
    status VARCHAR(16) NOT NULL
);
