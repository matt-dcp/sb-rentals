-- Migration: Add price tracking (price changes on existing listings + repost detection)
-- Run this in the Supabase SQL Editor (Dashboard → SQL Editor → New Query)

-- 1. Add original_price to listings (set on first insert, never updated)
ALTER TABLE listings ADD COLUMN IF NOT EXISTS original_price integer;

-- Backfill: set original_price = price for all existing rows that don't have it
UPDATE listings SET original_price = price WHERE original_price IS NULL;

-- 2. Price changes table: tracks when a listing's price changes in-place
CREATE TABLE IF NOT EXISTS price_changes (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  listing_id text NOT NULL,
  market text NOT NULL,
  category text,
  bedrooms float,
  old_price integer NOT NULL,
  new_price integer NOT NULL,
  change_pct float NOT NULL,
  change_date date NOT NULL,
  title text,
  UNIQUE(listing_id, change_date)
);

-- Enable RLS and allow anon read/write
ALTER TABLE price_changes ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_read_price_changes" ON price_changes FOR SELECT TO anon USING (true);
CREATE POLICY "anon_insert_price_changes" ON price_changes FOR INSERT TO anon WITH CHECK (true);

-- 3. Reposts table: tracks probable delete-and-repost patterns
CREATE TABLE IF NOT EXISTS reposts (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  original_id text NOT NULL,
  repost_id text NOT NULL,
  market text NOT NULL,
  bedrooms float,
  original_price integer,
  repost_price integer,
  price_change integer,
  price_change_pct float,
  title_similarity float,
  original_title text,
  repost_title text,
  detected_date date NOT NULL,
  UNIQUE(original_id, repost_id)
);

-- Enable RLS and allow anon read/write
ALTER TABLE reposts ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_read_reposts" ON reposts FOR SELECT TO anon USING (true);
CREATE POLICY "anon_insert_reposts" ON reposts FOR INSERT TO anon WITH CHECK (true);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_price_changes_market_date ON price_changes(market, change_date);
CREATE INDEX IF NOT EXISTS idx_reposts_market_date ON reposts(market, detected_date);
CREATE INDEX IF NOT EXISTS idx_listings_original_price ON listings(original_price);
