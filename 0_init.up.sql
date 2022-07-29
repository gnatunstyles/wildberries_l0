CREATE TABLE orders(
    order_uid varchar(40) NOT NULL,
	track_number varchar(40) NOT NULL,
	entry varchar(40) NOT NULL,
	delivery integer,
	payment integer,
	items text[],
	locale varchar(40) NOT NULL,
	internal_signature varchar(40) NOT NULL,
	customer_id varchar(40) NOT NULL,
	delivery_service varchar(40) NOT NULL,
	shardkey varchar(40) NOT NULL,
	sm_id integer,
	date_created timestamp,
	oof_shard varchar(40) NOT NULL
);

CREATE TABLE delivery(
    uuid  varchar(40) NOT NULL,
    name varchar(40) NOT NULL,
	phone varchar(40) NOT NULL,
	zip varchar(40) NOT NULL,
	city varchar(40) NOT NULL,
	address varchar(40) NOT NULL,
	region varchar(40) NOT NULL,
	email varchar(40) NOT NULL
);

CREATE TABLE payment(
    uuid  varchar(40) NOT NULL,
    transaction varchar(40) NOT NULL,
	request_id varchar(40) NOT NULL,
	currency varchar(40) NOT NULL,
	provider varchar(40) NOT NULL,
	amount integer,
	payment_dt integer,
	bank varchar(40) NOT NULL,
	delivery_cost integer,
	goods_total integer,
	custom_fee integer
);

CREATE TABLE items(    
    uuid  varchar(40) NOT NULL,
    chrt_id integer,
	track_number varchar(40) NOT NULL,
	price integer,
	rid varchar(40) NOT NULL,
	name varchar(40) NOT NULL,
	sale integer,
	size varchar(40) NOT NULL,
	total_price integer,
	nm_id integer,
	brand varchar(40) NOT NULL,
	status integer
);
