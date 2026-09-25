-- Local only. On the server, infra creates the app role; the migrations grant it what it needs.
CREATE ROLE pipeline_app LOGIN PASSWORD 'pipeline_app';
