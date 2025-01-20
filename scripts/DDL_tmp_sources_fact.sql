drop table if exists tmp_sources_fact;
create temp table tmp_sources_fact as
select 
	dp.product_id,
	dc.craftsman_id,
	dcs.customer_id,
	src.order_created_date,
	src.order_completion_date,
	src.order_status,
	current_timestamp
from tmp_sources src
INNER join dwh.d_craftsman dc on dc.craftsman_name = src.craftsman_name and dc.craftsman_email = src.craftsman_email
INNER join dwh.d_customer dcs on dcs.customer_name=src.customer_name and dcs.customer_email=src.customer_email
INNER join dwh.d_product dp on dp.product_name = src.product_name and dp.product_description=src.product_description and 
dp.product_price =src.product_price;