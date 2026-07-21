-- Migration: Lock down RLS (Supabase security advisor: rls_disabled_in_public)
-- Makes the public anon key read-only across all four tables. The scraper
-- must write with the service_role key (set SUPABASE_SERVICE_ROLE_KEY as a
-- GitHub Actions secret BEFORE running this, or the nightly scrape will fail).
-- Run via the "Run Migration" GitHub Action or the Supabase SQL Editor.

-- 1. Enable RLS on the two tables that had none
ALTER TABLE listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_snapshots ENABLE ROW LEVEL SECURITY;

-- 2. Anon may read everything (the dashboard is public)
DROP POLICY IF EXISTS "anon_read_listings" ON listings;
CREATE POLICY "anon_read_listings" ON listings
  FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "anon_read_daily_snapshots" ON daily_snapshots;
CREATE POLICY "anon_read_daily_snapshots" ON daily_snapshots
  FOR SELECT TO anon USING (true);

-- 3. Anon may no longer write anywhere (scraper now uses service_role,
--    which bypasses RLS; these insert policies from 002 are obsolete)
DROP POLICY IF EXISTS "anon_insert_price_changes" ON price_changes;
DROP POLICY IF EXISTS "anon_insert_reposts" ON reposts;
