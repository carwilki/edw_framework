-- Databricks notebook source
update legacy.inv_physical
set DOC_NBR= lpad(DOC_NBR, 10, '0')

