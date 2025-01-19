with dwh_delta as(
	select dcs.customer_id as customer_id,
		   dcs.customer_name as customer_name,
		   dcs.customer_address as customer_address,
		   dcs.customer_birthday as customer_birthday,
		   dcs.customer_email as customer_email,
		   fo.order_id as order_id,
		   dc.craftsman_id as craftsman_id,
		   dp.product_id as product_id,
		   dp.product_price as product_price,
		   dp.product_type as product_type,
		   fo.order_completion_date-fo.order_created_date as diff_order_date,
		   fo.order_status as order_status,
		   to_char(fo.order_created_date,'yyyy-mm') as report_period,
		   crd.customer_id as exist_customer_id,
		   dc.load_dttm as craftsman_load_dttm,
		   dcs.load_dttm as customers_load_dttm,
		   dp.load_dttm as products_load_dttm
		   from dwh.f_order fo 
		   inner join dwh.d_craftsman dc on fo.craftsman_id = dc.craftsman_id
		   inner join dwh.d_customer dcs on fo.customer_id = dcs.customer_id 
		   inner join dwh.d_product dp on fo.product_id = dp.product_id
		   left join dwh.customer_report_datamart crd on fo.customer_id=crd.customer_id
		   where (fo.load_dttm > (select coalesce(max(load_dttm),'1900-01-01') 
		   from dwh.load_dates_customer_report_datamart)) or 
		   (dc.load_dttm>(select coalesce(max(load_dttm),'1900-01-01') 
		   from dwh.load_dates_customer_report_datamart)) or
		   (dcs.load_dttm>(select coalesce(max(load_dttm),'1900-01-01') 
		   from dwh.load_dates_customer_report_datamart)) or
		   (dp.load_dttm>(select coalesce(max(load_dttm),'1900-01-01') 
		   from dwh.load_dates_customer_report_datamart))
		   ),
    dwh_update_delta as (
   select exist_customer_id
   from dwh_delta
   where exist_customer_id is not null),
    dwh_delta_insert_result as (
   select  t5.customer_id as customer_id,
   		   t5.customer_name as customer_name,
   		   t5.customer_address as customer_address,
   		   t5.customer_birthday as customer_birthday,
   		   t5.customer_email as customer_email,
   		   t5.customer_payment as customer_payment,
   		   t5.platform_money as platform_money,
   		   t5.count_order as count_order,
   		   t5.avg_price_order as avg_price_order,
   		   t5.median_time_order_completed AS median_time_order_completed,
   		   t5.product_type as top_product_category,
   		   t5.craftsman_id as top_craftsman_id,
   		   t5.count_order_created AS count_order_created,
		   t5.count_order_in_progress AS count_order_in_progress,
		   t5.count_order_delivery AS count_order_delivery,
		   t5.count_order_done AS count_order_done,
		   t5.count_order_not_done AS count_order_not_done,
		   t5.report_period AS report_period
		   from (
		   select *,
		   rank() over(partition by t2.customer_id order by count_product desc) as rank_count_product,
		   row_number() over(partition by t2.customer_id order by order_count desc) as top_craftsman
		   from (select 
		   			t1.customer_id as customer_id,
   		   			t1.customer_name as customer_name,
   		   			t1.customer_address as customer_address,
   		   			t1.customer_birthday as customer_birthday,
   		   			t1.customer_email as customer_email,
   		   			sum(t1.product_price) as customer_payment,
   		   			sum(t1.product_price)*0.1 as platform_money,
   		   			count(t1.order_id) as count_order,
   		   			t1.craftsman_id as craftsman_id, 
   		   			avg(t1.product_price) as avg_price_order,
   		   			percentile_cont(0.5) WITHIN GROUP(ORDER BY t1.diff_order_date) as median_time_order_completed,
   		   			sum(case when t1.order_status = 'created' then 1 else 0 end) as count_order_created,
					sum(case when t1.order_status = 'in progress' then 1 else 0 end) as count_order_in_progress,
					sum(case when t1.order_status = 'delivery' then 1 else 0 end) as count_order_delivery,
					sum(case when t1.order_status = 'done' then 1 else 0 end) as count_order_done,
					sum(case when t1.order_status != 'done' then 1 else 0 end) as count_order_not_done,
					t1.report_period as report_period
					from dwh_delta as t1
					where t1.exist_customer_id is null
					group by t1.customer_id,t1.customer_name,t1.customer_address,t1.customer_birthday,
					t1.customer_email,t1.craftsman_id,t1.report_period
					) as t2 inner join (
						select dd.customer_id as customer_id_for_product_type,
							   dd.product_type,
							   count(product_id) as count_product
							   from dwh_delta dd
							   group by dd.customer_id,dd.product_type
							   order by count_product desc) as t3 on t2.customer_id=t3.customer_id_for_product_type
							   inner join (select d.craftsman_id as craftsman_id_for_order_count,
							   				      count(d.order_id) as order_count
							   			   from dwh_delta d
							   			   group by d.craftsman_id
							   			   order by order_count desc) as t4 on t2.craftsman_id=t4.craftsman_id_for_order_count
				   				      
						) as t5 where t5.rank_count_product=1 and t5.top_craftsman=1
						order by report_period
						),
	dwh_delta_update_result AS (
	select t5.customer_id as customer_id,
   		   t5.customer_name as customer_name,
   		   t5.customer_address as customer_address,
   		   t5.customer_birthday as customer_birthday,
   		   t5.customer_email as customer_email,
   		   t5.customer_payment as customer_payment,
   		   t5.platform_money as platform_money,
   		   t5.count_order as count_order,
   		   t5.avg_price_order as avg_price_order,
   		   t5.median_time_order_completed AS median_time_order_completed,
   		   t5.product_type as top_product_category,
   		   t5.craftsman_id as top_craftsman_id,
   		   t5.count_order_created AS count_order_created,
		   t5.count_order_in_progress AS count_order_in_progress,
		   t5.count_order_delivery AS count_order_delivery,
		   t5.count_order_done AS count_order_done,
		   t5.count_order_not_done AS count_order_not_done,
		   t5.report_period AS report_period
		   from (
		   select *,
		   rank() over(partition by t2.customer_id order by count_product desc) as rank_count_product,
		   row_number() over(partition by t2.customer_id order by order_count desc) as top_craftsman
		   from (select 
		   			t1.customer_id as customer_id,
   		   			t1.customer_name as customer_name,
   		   			t1.customer_address as customer_address,
   		   			t1.customer_birthday as customer_birthday,
   		   			t1.customer_email as customer_email,
   		   			sum(t1.product_price) as customer_payment,
   		   			sum(t1.product_price)*0.1 as platform_money,
   		   			count(t1.order_id) as count_order,
   		   			t1.craftsman_id as craftsman_id, 
   		   			avg(t1.product_price) as avg_price_order,
   		   			percentile_cont(0.5) WITHIN GROUP(ORDER BY t1.diff_order_date) as median_time_order_completed,
   		   			sum(case when t1.order_status = 'created' then 1 else 0 end) as count_order_created,
					sum(case when t1.order_status = 'in progress' then 1 else 0 end) as count_order_in_progress,
					sum(case when t1.order_status = 'delivery' then 1 else 0 end) as count_order_delivery,
					sum(case when t1.order_status = 'done' then 1 else 0 end) as count_order_done,
					sum(case when t1.order_status != 'done' then 1 else 0 end) as count_order_not_done,
					t1.report_period as report_period
					from (
					select dcs.customer_id as customer_id,
						   dcs.customer_name as customer_name,
						   dcs.customer_address as customer_address,
						   dcs.customer_birthday as customer_birthday,
						   dcs.customer_email as customer_email,
						   dc.craftsman_id as craftsman_id,
						   dp.product_id as product_id,
						   dp.product_type as product_type,
						   dp.product_price as product_price,
						   fo.order_id as order_id,
						   fo.order_completion_date-fo.order_created_date as diff_order_date,
						   fo.order_status as order_status,
						   to_char(fo.order_created_date,'yyyy-mm') as report_period
						   from dwh.f_order fo
						   inner join dwh.d_customer dcs on fo.customer_id = dcs.customer_id
						   inner join dwh.d_craftsman dc on fo.craftsman_id = dc.craftsman_id 
						   inner join dwh.d_product dp on fo.product_id = dp.product_id 
						   inner join dwh_update_delta ud on fo.customer_id = ud.exist_customer_id) as t1 
						   group by t1.customer_id,t1.customer_name,t1.customer_address,t1.customer_birthday,
					t1.customer_email,t1.craftsman_id,t1.report_period) as t2 inner join (
							select dd.customer_id as customer_id_for_product_type,
							   dd.product_type,
							   count(product_id) as count_product
							   from dwh_delta dd
							   group by dd.customer_id,dd.product_type
							   order by count_product desc) as t3 on t2.customer_id=t3.customer_id_for_product_type
							   inner join (select d.craftsman_id as craftsman_id_for_order_count,
							   				      count(d.order_id) as order_count
							   			   from dwh_delta d
							   			   group by d.craftsman_id
							   			   order by order_count desc) as t4 on t2.craftsman_id=t4.craftsman_id_for_order_count
						) as t5 
						where t5.rank_count_product=1 and t5.top_craftsman=1
						order by report_period
   		   			),
   	insert_delta AS (
	insert into dwh.customer_report_datamart(customer_id,customer_name,customer_address,customer_birthday,customer_email,
	customer_money,platform_money,count_order,avg_price_order,median_time_order_completed,top_product_category,
	top_craftsman_category,count_order_created,count_order_in_progress,count_order_delivery,count_order_done,
	count_order_not_done,report_period)
	select customer_id,customer_name,customer_address,customer_birthday,customer_email,customer_payment,platform_money,count_order,
	avg_price_order,median_time_order_completed,top_product_category,
	top_craftsman_id,count_order_created,count_order_in_progress,count_order_delivery,count_order_done,
	count_order_not_done,report_period
	from dwh_delta_insert_result
	),
	update_delta as (
	update dwh.customer_report_datamart as updates set
	customer_name=t2.customer_name,
	customer_address = t2.customer_address,
	customer_birthday = t2.customer_birthday,
	customer_email = t2.customer_email,
	customer_money = t2.customer_payment,
	platform_money = t2.platform_money,
	count_order = t2.count_order,
	avg_price_order = t2.avg_price_order,
	median_time_order_completed = t2.median_time_order_completed,
	top_product_category = t2.top_product_category,
	top_craftsman_category = t2.top_craftsman_id,
	count_order_created = t2.count_order_created,
	count_order_in_progress = t2.count_order_in_progress,
	count_order_delivery = t2.count_order_delivery,
	count_order_done = t2.count_order_done,
	count_order_not_done = t2.count_order_done,
	report_period = t2.report_period
	from (
	select customer_id,customer_name,customer_address,customer_birthday,customer_email,customer_payment,platform_money,count_order,
	avg_price_order,median_time_order_completed,top_product_category,
	top_craftsman_id,count_order_created,count_order_in_progress,count_order_delivery,count_order_done,
	count_order_not_done,report_period
	from dwh_delta_update_result) as t2
	where updates.customer_id=t2.customer_id
	),
	insert_load_date AS (
	INSERT INTO dwh.load_dates_customer_report_datamart (
		load_dttm
	)
	SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm),NOW()),COALESCE(MAX(customers_load_dttm),NOW()),COALESCE(MAX(products_load_dttm),NOW()))
		FROM dwh_delta
)
SELECT 'increment datamart';