CREATE TABLE categories (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );

CREATE TABLE product_categories (
        product_id INTEGER NOT NULL,
        category_id INTEGER NOT NULL,
        PRIMARY KEY (product_id, category_id),
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
    );

CREATE TABLE product_urls (
        product_id INTEGER NOT NULL,
        shop_id INTEGER NOT NULL,
        url TEXT NOT NULL,
        PRIMARY KEY (product_id, shop_id),
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
        FOREIGN KEY (shop_id) REFERENCES shops(id) ON DELETE CASCADE
    );

CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );

CREATE TABLE scrape_consolidated
(
    session_date INTEGER not null,
    product_id   INTEGER not null,
    best_shop_id INTEGER not null,
    best_shop_url TEXT   not null,
    best_value   INTEGER,
    primary key (session_date, product_id)
);

CREATE TABLE scrape_detailed (
    session_date INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    shop_id INTEGER NOT NULL,
    shop_url TEXT NOT NULL,
    parsed_status INTEGER NOT NULL,
    parsed_value INTEGER
);

CREATE TABLE shops (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );

CREATE INDEX idx_scrape_detailed_session_date
ON scrape_detailed (session_date);
