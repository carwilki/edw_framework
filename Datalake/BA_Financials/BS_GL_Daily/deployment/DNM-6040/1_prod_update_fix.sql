
select * from legacy.GL_ACTUAL_DAY
where 
company_cd='2000'
and gl_acct_nbr in ('11611','68920')
and gl_category_cd='00'
and gl_profit_ctr_cd='2802-00'
and gl_doc_type_cd='SK'
and vendor_id='0'
and loc_currency_id='CAD'
and day_dt='2023-11-15 00:00:00'

update legacy.GL_ACTUAL_DAY
set exch_rate_pct=0.7251
where 
company_cd='2000'
and gl_acct_nbr in ('11611','68920')
and gl_category_cd='00'
and gl_profit_ctr_cd='2802-00'
and gl_doc_type_cd='SK'
and vendor_id='0'
and loc_currency_id='CAD'
and day_dt='2023-11-15 00:00:00'
