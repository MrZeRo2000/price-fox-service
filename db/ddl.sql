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

CREATE TABLE "product_urls" (
                            product_id INTEGER NOT NULL,
                            url_id INTEGER NOT NULL,
                            PRIMARY KEY (product_id, url_id),
                            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                            FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
                        );

CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );

CREATE TABLE "scrape_consolidated" (
                            session_date INTEGER NOT NULL,
                            product_id INTEGER NOT NULL,
                            best_url_id INTEGER NOT NULL,
                            best_url TEXT NOT NULL,
                            best_value INTEGER,
                            PRIMARY KEY (session_date, product_id)
                        );

CREATE TABLE "scrape_detailed" (
                            session_date INTEGER NOT NULL,
                            product_id INTEGER NOT NULL,
                            url_id INTEGER NOT NULL,
                            url TEXT NOT NULL,
                            parsed_status INTEGER NOT NULL,
                            parsed_value INTEGER
                        );

CREATE TABLE urls (
                        id INTEGER PRIMARY KEY,
                        url TEXT NOT NULL UNIQUE
                    );

CREATE INDEX idx_scrape_detailed_session_date
                        ON scrape_detailed (session_date);
