drop table if exists dwh.customer_report_datamart;
create table if not exists dwh.customer_report_datamart(
	id bigserial not null,
	customer_id bigint not null,
	customer_name varchar not null,
	customer_address varchar not null,
	customer_birthday date not null,
	customer_email varchar not null,
	customer_money numeric(15,2) not null,
	platform_money bigint not null,
	count_order integer not null,
	avg_price_order numeric(10,2) not null,
	median_time_order_completed numeric(10,1) not null,
	top_product_category varchar not null,
	top_craftsman_category bigint not null,
	count_order_created bigint not null,
	count_order_in_progress bigint not null,
	count_order_delivery bigint not null,
	count_order_done bigint not null,
	count_order_not_done bigint not null,
	report_period varchar not null,
	constraint customer_report_datamart_pk primary key(id)
);