merge into dwh.d_craftsman d 
using (select distinct craftsman_name,craftsman_address,craftsman_birthday,craftsman_email from tmp_sources) t
on d.craftsman_name =t.craftsman_name and d.craftsman_email = t.craftsman_email
when matched then
	update set craftsman_address=t.craftsman_address,
craftsman_birthday=t.craftsman_birthday,load_dttm = current_timestamp
when not matched then
	insert (craftsman_name,craftsman_address,craftsman_birthday,craftsman_email,load_dttm)
	values(t.craftsman_name,t.craftsman_address,t.craftsman_birthday,t.craftsman_email,current_timestamp);