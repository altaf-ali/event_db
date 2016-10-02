-- setup database
CREATE DATABASE events;
REVOKE ALL ON schema public FROM public;

-- create read-write user
CREATE ROLE events_writer WITH LOGIN;
ALTER ROLE events_writer WITH ENCRYPTED PASSWORD 'GJmVtmG6MJSDRseQEJcJ2gaU';

GRANT ALL ON schema public TO events_writer;

-- create read-only user
CREATE ROLE events_reader WITH LOGIN;
GRANT USAGE ON SCHEMA public TO events_reader;
