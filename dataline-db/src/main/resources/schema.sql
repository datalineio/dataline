-- extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- database
CREATE DATABASE dataline;

-- types
CREATE TYPE public.JOB_STATUS AS ENUM ('pending', 'running', 'failed', 'completed', 'cancelled');

-- tables
CREATE TABLE public.DATALINE_METADATA (id VARCHAR (255) PRIMARY KEY, value VARCHAR (255));

CREATE TABLE public.JOBS (
    id BIGSERIAL PRIMARY KEY,
    scope VARCHAR (255),
    created_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    status JOB_STATUS,
    config JSONB,
    output JSONB,
    stdout_path VARCHAR (255),
    stderr_path VARCHAR (255)
);

-- entries
INSERT INTO public.DATALINE_METADATA VALUES  ('server-uuid', uuid_generate_v4());

-- grants
GRANT ALL ON SCHEMA public TO docker;
