-- Raw staging table (keeps full API payload for traceability)
CREATE TABLE IF NOT EXISTS stg_countries_raw (
  load_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source       TEXT NOT NULL DEFAULT 'restcountries',
  payload      JSONB NOT NULL
);

-- Curated dimension table (analytics-ready)
CREATE TABLE IF NOT EXISTS dim_country (
  country_code   TEXT PRIMARY KEY,
  name_common    TEXT,
  name_official  TEXT,
  region         TEXT,
  subregion      TEXT,
  capital        TEXT,
  population     BIGINT,
  area           DOUBLE PRECISION,
  independent    BOOLEAN,
  landlocked     BOOLEAN,
  flag_png       TEXT,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
