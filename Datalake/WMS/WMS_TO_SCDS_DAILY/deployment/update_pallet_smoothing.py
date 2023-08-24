# Databricks notebook source
%sql
UPDATE <remove_me_>refine.wm_task_hdr set 
    WM_TASK_BATCH = 'Pallet Smoothing',
    UPDATE_TSTMP = current_timestamp()
WHERE 
    WM_TASK_BATCH like '%Pallet%' ;