-- Local only: the roles infra creates on the server. The database owner is not a superuser, so a
-- migration that would need one fails here too; the app role owns nothing and gets only what
-- the migrations grant it.
CREATE ROLE pipeline LOGIN PASSWORD 'pipeline';
CREATE ROLE pipeline_app LOGIN PASSWORD 'pipeline_app';
ALTER DATABASE pipeline OWNER TO pipeline;
REVOKE CONNECT ON DATABASE pipeline FROM PUBLIC;
GRANT CONNECT ON DATABASE pipeline TO pipeline, pipeline_app;
