merge into dwh.d_product d
using (select distinct product_name,product_description,product_type,product_price from tmp_sources) t
on d.product_name=t.product_name and d.product_description=t.product_description and d.product_price=t.product_price
when matched then 
	update set product_type=t.product_type,load_dttm=current_timestamp
when no matched then 
	insert(product_name,product_description,product_type,product_price,load_dttm)
	values(t.product_name,t.product_description,t.product_type,t.product_price,current_timestamp);