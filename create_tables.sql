-- Table 1: btc_iv_spd_skewness
CREATE TABLE btc_iv_spd_skewness (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    expiration_timestamp INT NOT NULL,
    atm_slope FLOAT
);

-- Table 2: btc_options_raw
CREATE TABLE btc_options_raw (
    timestamp BIGINT NOT NULL,
    instrument_name VARCHAR(50) NOT NULL,
    expiration_timestamp BIGINT NOT NULL,
    option_type ENUM('call', 'put') NOT NULL,
    bid_price FLOAT,
    ask_price FLOAT,
    bid_iv FLOAT,
    ask_iv FLOAT,
    underlying_price FLOAT,
    strike_price FLOAT,
    log_moneyness FLOAT,
    delta FLOAT,
    vega FLOAT,
    theta FLOAT
);

-- Table 3: btc_options_tick
CREATE TABLE btc_options_tick (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    instrument_name VARCHAR(50) NOT NULL,
    underlying_price FLOAT,
    strike_price FLOAT,
    mid_price FLOAT,
    mark_iv FLOAT,
    expiration_timestamp BIGINT NOT NULL,
    option_type ENUM('call', 'put') NOT NULL,
    log_moneyness FLOAT,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
