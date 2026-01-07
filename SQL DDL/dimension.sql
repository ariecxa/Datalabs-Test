create table dim_customer (
	customer_id INT,
	name varchar(255),
	email varchar(255),
	city varchar(255),
	signup_date int
);

select * from dim_campaign;
create table dim_products (
	product_id INT,
	product_name varchar(255),
	category varchar(255),
	price INT
);

create table dim_date (
	date_id INT,
	full_date varchar(255),
	`year` INT,
	`month` INT,
	`day` INT,
	month_name varchar(255),
	day_name varchar(255)
);

create table dim_campaign (
	campaign_id INT,
	campaign_name varchar(255),
	start_date INT,
	end_date INT,
	channel varchar(255)
);