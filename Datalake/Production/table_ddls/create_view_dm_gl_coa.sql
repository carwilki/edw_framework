-- Databricks notebook source
create view legacy.dm_gl_coa as
select
  GL_ACCT_NBR,
  GL_ACCT_DESC,
  STORE_LNBR,
  STORE_LNBR_DESC,
  SSG_LNBR,
  SSG_LNBR_DESC,
  CENTER,
  GL_CHART_OF_ACCOUNTS_CD
from
  refine.udf_usr_gl_coa
