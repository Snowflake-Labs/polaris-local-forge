-- ============================================================================
-- DuckDB Read-Only Catalog Analysis Script
-- ============================================================================
--
-- This script uses DuckDB for READ-ONLY analysis and verification of Iceberg
-- tables in the Polaris catalog. It avoids INSERT/UPDATE/DELETE operations
-- to prevent the UUID corruption bug in DuckDB v1.4.4.
--
-- Issue: https://github.com/duckdb/duckdb-python/issues/356
-- DuckDB v1.4.4 generates new table UUIDs during mutations, violating Iceberg spec.
-- This causes metadata staleness when syncing to Snowflake via L2C.
--
-- Usage:
--   duckdb < scripts/analyze_catalog.sql
--   
--   # Or interactively:
--   duckdb
--   .read scripts/analyze_catalog.sql
--
-- ============================================================================

.echo off

.print '============================================================================'
.print 'DuckDB Read-Only Catalog Analysis'
.print '============================================================================'
.print 'NOTE: This script uses SELECT queries only to avoid DuckDB UUID bug'
.print 'Issue: https://github.com/duckdb/duckdb-python/issues/356'
.print '============================================================================'
.print ''

-- ============================================================================
-- Step 1: Setup DuckDB Extensions
-- ============================================================================
.print 'Installing and loading extensions...'

INSTALL iceberg;
LOAD iceberg;

INSTALL httpfs;
LOAD httpfs;

.print 'OK: Extensions loaded successfully'
.print ''

-- ============================================================================
-- Step 2: Connect to Polaris REST Catalog
-- ============================================================================
.print 'Connecting to Polaris REST Catalog...'

-- Create OAuth2 secret for Polaris authentication
CREATE OR REPLACE SECRET polaris_secret (
    TYPE iceberg,
    CLIENT_ID 'polaris',
    CLIENT_SECRET 'polaris-secret',
    OAUTH2_SERVER_URI 'http://localhost:18181/oauth/tokens'
);

-- Attach to Polaris catalog
ATTACH 'polaris' AS polaris_catalog (
    TYPE iceberg,
    SECRET polaris_secret,
    ENDPOINT 'http://localhost:18181/api/catalog/v1/polaris'
);

.print 'OK: Connected to Polaris catalog'
.print ''

-- ============================================================================
-- Step 3: Catalog Discovery and Analysis
-- ============================================================================
.print 'Discovering tables in catalog...'

SHOW ALL TABLES;

.print ''

-- ============================================================================
-- Step 4: Wildlife Namespace Analysis (if exists)
-- ============================================================================
.print 'Analyzing wildlife namespace...'
.print ''

-- Check if penguins table exists and analyze
.print 'Penguins Table Analysis:'
SELECT 
    'wildlife.penguins' as table_name,
    COUNT(*) as total_records
FROM polaris_catalog.wildlife.penguins;

.print ''

.print 'Species Distribution:'
SELECT 
    species,
    COUNT(*) as count,
    ROUND(AVG(bill_length_mm), 2) as avg_bill_length_mm,
    ROUND(AVG(body_mass_g), 2) as avg_body_mass_g
FROM polaris_catalog.wildlife.penguins
GROUP BY species
ORDER BY species;

.print ''

.print 'Island Distribution:'
SELECT 
    island,
    COUNT(*) as penguin_count
FROM polaris_catalog.wildlife.penguins
GROUP BY island
ORDER BY penguin_count DESC;

.print ''

-- ============================================================================
-- Step 5: Plantae Namespace Analysis (optional - only if loaded separately)
-- ============================================================================
.print 'Checking for additional namespaces...'
.print ''

-- Note: plantae.fruits table is available for separate testing
-- Load with: python scripts/pyiceberg_data_loader.py --config-file datasets/plantae.toml
.print 'Plantae namespace: Available for separate multi-namespace testing'
.print ''

-- ============================================================================
-- Step 6: Iceberg Metadata Analysis (Read-Only)
-- ============================================================================
.print 'Iceberg Metadata Analysis...'
.print ''

.print 'Wildlife Penguins Metadata:'
SELECT 
    file_path,
    file_format,
    record_count
FROM iceberg_metadata('polaris_catalog.wildlife.penguins')
LIMIT 5;

.print ''

.print 'Wildlife Penguins Snapshots:'
SELECT 
    snapshot_id,
    sequence_number,
    timestamp_ms,
    operation
FROM iceberg_snapshots('polaris_catalog.wildlife.penguins')
ORDER BY timestamp_ms DESC
LIMIT 3;

.print ''

.print 'Additional metadata available for other namespaces when loaded separately.'
.print ''

-- ============================================================================
-- Step 7: Cross-Namespace Analysis
-- ============================================================================
.print 'Cross-Namespace Summary:'
.print ''

SELECT 
    'wildlife' as namespace,
    'penguins' as table_name,
    COUNT(*) as record_count,
    'animals' as category
FROM polaris_catalog.wildlife.penguins;

.print ''

-- ============================================================================
-- Completion
-- ============================================================================
.print '============================================================================'
.print 'Read-Only Analysis Complete!'
.print '============================================================================'
.print ''
.print 'This analysis used only SELECT queries to avoid DuckDB UUID corruption.'
.print 'For data loading, use: python scripts/pyiceberg_data_loader.py'
.print ''
.print 'Available commands for further exploration:'
.print ''
.print '  -- List all tables'
.print '  SHOW ALL TABLES;'
.print ''
.print '  -- Query specific tables'
.print '  SELECT * FROM polaris_catalog.wildlife.penguins LIMIT 10;'
.print '  SELECT * FROM polaris_catalog.plantae.fruits;'
.print ''
.print '  -- Explore metadata (read-only)'
.print '  SELECT * FROM iceberg_metadata('\''polaris_catalog.wildlife.penguins'\'');'
.print '  SELECT * FROM iceberg_snapshots('\''polaris_catalog.wildlife.penguins'\'');'
.print ''
.print '============================================================================'