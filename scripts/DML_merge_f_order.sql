merge into dwh.f_order f 
using tmp_sources_fact t
on f.product_id = t.product_id and f.craftsman_id =t.craftsman_id and f.customer_id =t.customer_id and f.order_created_date = t.order_created_date
when matched then 
	update set order_completion_date=t.order_completion_date,
	order_status = t.order_status,load_dttm = current_timestamp
when no matched then 
	insert(product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
	values(t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);
