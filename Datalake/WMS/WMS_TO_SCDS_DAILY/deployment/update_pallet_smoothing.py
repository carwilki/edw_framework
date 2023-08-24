# Databricks notebook source
%sql
UPDATE qa_refine.wm_task_hdr set WM_TASK_BATCH = 'Pallet Smoothing'
where WM_TASK_BATCH like '%Pallet%';