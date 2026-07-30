-- PostgreSQL Setup Script for SyniqAI
-- Run this in pgAdmin Query Tool (connected to postgres database)

-- Step 1: Create user
CREATE USER syniqai_user WITH PASSWORD 'syniqai_password';

-- Step 2: Grant create database privilege
ALTER USER syniqai_user WITH CREATEDB;

-- Step 3: Create database
CREATE DATABASE syniqai_metadata OWNER syniqai_user;

-- Step 4: Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE syniqai_metadata TO syniqai_user;

-- Verify user was created
SELECT usename, usecreatedb FROM pg_user WHERE usename = 'syniqai_user';
