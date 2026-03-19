create table main.categories
(
    id   INTEGER
        primary key,
    name TEXT not null
);

create table main.products
(
    id   INTEGER
        primary key,
    name TEXT not null
);

create table main.product_categories
(
    product_id  INTEGER not null
        references main.products
            on delete cascade,
    category_id INTEGER not null
        references main.categories
            on delete cascade,
    primary key (product_id, category_id)
);

create table main.scrape_detailed
(
    session_date  INTEGER not null,
    product_id    INTEGER not null,
    shop_id       INTEGER not null,
    shop_url      TEXT    not null,
    parsed_status INTEGER not null,
    parsed_value  INTEGER
);

create index main.idx_scrape_detailed_session_date
    on main.scrape_detailed (session_date);

create table main.shops
(
    id   INTEGER
        primary key,
    name TEXT not null
);

create table main.product_urls
(
    product_id INTEGER not null
        references main.products
            on delete cascade,
    shop_id    INTEGER not null
        references main.shops
            on delete cascade,
    url        TEXT    not null,
    primary key (product_id, shop_id)
);
