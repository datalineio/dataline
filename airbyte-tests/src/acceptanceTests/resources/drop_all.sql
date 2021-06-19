SELECT 'DROP TABLE IF EXISTS "' || tablename || '" CASCADE;' from pg_tables WHERE schemaname = 'public';
DROP SCHEMA IF EXISTS output_namespace_public CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;

