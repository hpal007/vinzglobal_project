-- Databricks notebook source
-- Databricks notebook source
DROP DATABASE IF EXISTS vinz_processed CASCADE;

DROP DATABASE IF EXISTS vinz_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS vinz_presentation 
LOCATION "/mnt/vinz/presentation";

CREATE DATABASE IF NOT EXISTS vinz_processed
LOCATION "/mnt/vinz/processed";

-- COMMAND ----------

-- COMMAND ----------

