drop table if exists tmp_sources;
create temp table tmp_sources as
select
	cpo.craftsman_id,
	cpo.craftsman_name,
	cpo.craftsman_address,
	cpo.craftsman_birthday,
	cpo.craftsman_email,
	cpo.product_id,
	cpo.product_name,
	cpo.product_description,
	cpo.product_type,
	cpo.product_price,
	cpo.order_id,
	cpo.order_created_date,
	cpo.order_completion_date,
	cpo.order_status,
	c.customer_id,
	c.customer_name,
	c.customer_address,
	c.customer_birthday,
	c.customer_email 
FROM external_source.craft_products_orders cpo
join external_source.customers c on cpo.customer_id=c.customer_id; 
