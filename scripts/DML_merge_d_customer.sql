merge into dwh.d_customer d 
using (select distinct customer_name,customer_address,customer_birthday,customer_email from tmp_sources) t 
on d.customer_name = t.customer_name and d.customer_email = t.customer_email
when matched then 
	update set customer_address =t.customer_address,
	customer_birthday=t.customer_birthday,load_dttm=current_timestamp
when no matched then
	insert (customer_name,customer_address,customer_birthday,customer_email,load_dttm)
	values(t.customer_name,t.customer_address,t.customer_birthday,t.customer_email,current_timestamp);