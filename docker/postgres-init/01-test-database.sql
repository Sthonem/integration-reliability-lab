-- Separate database for running the test suite against real PostgreSQL
-- (tests drop and recreate their tables; never point them at the app database).
CREATE DATABASE irlab_test;
