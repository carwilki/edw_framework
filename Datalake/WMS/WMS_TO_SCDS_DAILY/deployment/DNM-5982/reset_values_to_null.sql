--https://petsmart.atlassian.net/browse/DNM-5982
--this query resets all total_size_on_orders to null if the values
--are 0.00 to be consistent with existing process
update dev_refine.WM_PURCHASE_ORDERS_LINE_ITEM
set TOTAL_SIZE_ON_ORDERS = null, UPDATE_TSTMP = current_timestamp()
where Total_size_on_orders = 0.00;

optimize dev_refine.WM_PURCHASE_ORDERS_LINE_ITEM;