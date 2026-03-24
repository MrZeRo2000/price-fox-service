BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS strategies (
    id INTEGER PRIMARY KEY,
    strategy_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS strategy_domains (
    id INTEGER PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE,
    strategy_id INTEGER NOT NULL,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_domains_domain
ON strategy_domains (domain);

CREATE TABLE IF NOT EXISTS strategy_settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL
);

INSERT OR IGNORE INTO strategies (strategy_name) VALUES ('gemini_url');
INSERT OR IGNORE INTO strategies (strategy_name) VALUES ('playwright');
INSERT OR IGNORE INTO strategies (strategy_name) VALUES ('jina');

UPDATE strategy_domains
SET strategy_id = (SELECT id FROM strategies WHERE strategy_name = 'playwright')
WHERE strategy_id = (SELECT id FROM strategies WHERE strategy_name = 'default');

DELETE FROM strategies
WHERE strategy_name = 'default';

INSERT INTO strategy_domains (domain, strategy_id)
VALUES (
    'itbox.ua',
    (SELECT id FROM strategies WHERE strategy_name = 'gemini_url')
)
ON CONFLICT(domain) DO UPDATE SET
    strategy_id = excluded.strategy_id;

INSERT OR IGNORE INTO strategy_settings (setting_key, setting_value)
VALUES ('default_fetch_strategy', 'playwright');

INSERT OR IGNORE INTO strategy_settings (setting_key, setting_value)
VALUES ('jina_rate_limit_rpm', '20');

INSERT OR IGNORE INTO strategy_settings (setting_key, setting_value)
VALUES ('gemini_model', 'gemini-2.0-flash');

INSERT OR IGNORE INTO strategy_settings (setting_key, setting_value)
VALUES ('gemini_timeout_seconds', '45');

COMMIT;
