-- Databricks notebook source
-- For one time historic load from Netezza edw_prd.sku_substitution into Delta Lake legacy.sku_substitution

insert into legacy.SKU_SUBSTITUTION select * from dev_legacy.SKU_SUBSTITUTION
