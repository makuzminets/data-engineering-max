/*
====================================================================================================
 Project:    Users audit + pg_cron daily export (inside Dockerized Postgres)
 File:       postgres_users_update_history_with_cron.sql (single self-contained SQL to meet task criteria)

 What this script does
 - Ensures pg_cron extension is enabled in the current database (example_db)
 - Creates base tables: public.users, public.users_audit
 - Adds trigger function to log changes of 3 fields: name, email, role
 - Creates trigger on public.users (AFTER UPDATE)
 - Creates export function that dumps TODAY's audit rows to /tmp/users_audit_export_YYYY-MM-DD.csv
 - Schedules a daily cron job at 03:00 to run the export
 - Inserts test data and runs an immediate export (if supported), then shows cron jobs

Prerequisite (Docker image in repo data-engineering-max):
  # Build your Postgres image with pg_cron from the repo Dockerfile
  docker build -t my-postgres-cron \
    -f /Users/maksim/repos/data-engineering-max/docker_postgres_ch_mongodb/Dockerfile \
    /Users/maksim/repos/data-engineering-max/docker_postgres_ch_mongodb

How to run (from host, against the container):
  docker cp /Users/maksim/repos/data-engineering-max/postgres_users_update_history_with_cron.sql \
    postgres_db:/tmp/users_update_history_with_cron.sql
  docker exec -it postgres_db psql -U user -d example_db -f /tmp/users_update_history_with_cron.sql

 How to verify in container:
   docker exec -it postgres_db psql -U user -d example_db -c "SELECT jobid, jobname, schedule FROM cron.job;"
   docker exec -it postgres_db bash -lc 'ls -l /tmp/users_audit_export_$(date +%F).csv && head -n 5 /tmp/users_audit_export_$(date +%F).csv'

 Notes
 - pg_cron must be preloaded via shared_preload_libraries and cron.database_name set to this DB.
 - If cron.run_job(text) is unavailable in your pg_cron version, call the export function directly.
====================================================================================================
*/

-- 0) Ensure pg_cron is available in this DB (example_db)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 1) Base tables (idempotent)
CREATE TABLE IF NOT EXISTS public.users (
  id          SERIAL PRIMARY KEY,
  name        text,
  email       text,
  role        text,
  updated_at  timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.users_audit (
  id            SERIAL PRIMARY KEY,
  user_id       integer,
  changed_at    timestamp DEFAULT now(),
  changed_by    text,
  field_changed text,
  old_value     text,
  new_value     text
);

-- 2) Audit trigger function (tracks 3 fields: name, email, role)
CREATE OR REPLACE FUNCTION public.log_user_update()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.name IS DISTINCT FROM OLD.name THEN
    INSERT INTO public.users_audit(user_id, changed_by, field_changed, old_value, new_value)
    VALUES (OLD.id, current_user, 'name', OLD.name::text, NEW.name::text);
  END IF;

  IF NEW.email IS DISTINCT FROM OLD.email THEN
    INSERT INTO public.users_audit(user_id, changed_by, field_changed, old_value, new_value)
    VALUES (OLD.id, current_user, 'email', OLD.email::text, NEW.email::text);
  END IF;

  IF NEW.role IS DISTINCT FROM OLD.role THEN
    INSERT INTO public.users_audit(user_id, changed_by, field_changed, old_value, new_value)
    VALUES (OLD.id, current_user, 'role', OLD.role::text, NEW.role::text);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3) Trigger on public.users
DROP TRIGGER IF EXISTS trigger_log_user_update ON public.users;
CREATE TRIGGER trigger_log_user_update
AFTER UPDATE ON public.users
FOR EACH ROW
EXECUTE FUNCTION public.log_user_update();

-- 4) Export function (writes today's audit to /tmp inside container)
CREATE OR REPLACE FUNCTION public.export_users_audit_for_today() RETURNS void AS $$
DECLARE
  file_path text := '/tmp/users_audit_export_' || to_char(current_date, 'YYYY-MM-DD') || '.csv';
BEGIN
  EXECUTE format($f$
    COPY (
      SELECT user_id, field_changed, old_value, new_value, changed_at
      FROM public.users_audit
      WHERE changed_at::date = current_date
      ORDER BY id
    ) TO %L WITH (FORMAT csv, HEADER true)
  $f$, file_path);
END;
$$ LANGUAGE plpgsql;

-- 5) Schedule at 03:00 daily (idempotent by job name)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'daily_audit_export') THEN
    PERFORM cron.unschedule((SELECT jobid FROM cron.job WHERE jobname = 'daily_audit_export'));
  END IF;
  PERFORM cron.schedule(
    'daily_audit_export',
    '0 3 * * *',
    'SELECT public.export_users_audit_for_today();'
  );
END;
$$;

-- 6) Test data for today
INSERT INTO public.users(name, email, role)
VALUES ('Test','test@example.com','user')
ON CONFLICT DO NOTHING;

UPDATE public.users
SET role = 'admin'
WHERE email = 'test@example.com';

-- 7) Manual run (if supported in your pg_cron)
--    If this errors, comment it and run the fallback below instead.
--    SELECT cron.run_job('daily_audit_export');

-- Fallback manual export (if run_job(text) is unavailable)
-- SELECT public.export_users_audit_for_today();

-- 8) Show cron jobs for visibility (criteria)
SELECT jobid, jobname, schedule, command FROM cron.job ORDER BY jobid;


