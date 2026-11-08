-- ============================================================
-- DIMENSIONS
-- ============================================================
 -- Core anime metadata — things that rarely change
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.dim_anime (anime_id SERIAL PRIMARY KEY,
                                        anilist_id INT NOT NULL UNIQUE, -- AniList's own ID
 title_romaji TEXT, title_english TEXT, title_native TEXT, format TEXT CHECK (format IN ('tv',
                                                                                         'tv_short',
                                                                                         'movie', 
                                                                                         'ova', 
                                                                                         'ona', 
                                                                                         'special', 
                                                                                         'music')), status TEXT CHECK (status IN ('finished', 
                                                                                                                                  'releasing', 
                                                                                                                                  'not_yet_released', 
                                                                                                                                  'cancelled', 
                                                                                                                                  'hiatus')), source_material TEXT CHECK (source_material IN ('original', 
                                                                                                                                                                                              'manga', 
                                                                                                                                                                                              'light_novel', 
                                                                                                                                                                                              'visual_novel', 
                                                                                                                                                                                              'video_game', 
                                                                                                                                                                                              'novel', 
                                                                                                                                                                                              'doujinshi', 
                                                                                                                                                                                              'anime', 
                                                                                                                                                                                              'web_novel', 
                                                                                                                                                                                              'live_action', 
                                                                                                                                                                                              'game', 
                                                                                                                                                                                              'comic', 
                                                                                                                                                                                              'multimedia_project', 
                                                                                                                                                                                              'other')), episodes INT, duration_min INT, is_adult BOOLEAN DEFAULT FALSE, -- When we first saw this anime in our pipeline
 created_at TIMESTAMPTZ DEFAULT NOW(),
                                updated_at TIMESTAMPTZ DEFAULT NOW());

-- ──────────────────────────────────────────────────────────────
 -- Genre lookup — Action, Romance, Comedy, etc.
-- Separate table because one anime has many genres

CREATE TABLE gold.dim_genre (genre_id SERIAL PRIMARY KEY,
                                        genre_name TEXT NOT NULL UNIQUE);

-- Studio lookup — Mappa, Ufotable, A-1 Pictures, etc.

CREATE TABLE gold.dim_studio (studio_id SERIAL PRIMARY KEY,
                                          studio_name TEXT NOT NULL UNIQUE,
                                                                    is_animation_studio BOOLEAN DEFAULT TRUE);

-- Season lookup — WINTER 2024, SPRING 2024, etc.

CREATE TABLE gold.dim_season (season_id SERIAL PRIMARY KEY,
                                          season TEXT NOT NULL CHECK (season IN ('winter',
                                                                                 'spring',
                                                                                 'summer',
                                                                                 'fall')), year INT NOT NULL,
                                                                                                    UNIQUE (season,
                                                                                                            year));

-- ============================================================
-- BRIDGE TABLES (many-to-many)
-- ============================================================
 -- One anime → many genres, one genre → many anime

CREATE TABLE gold.bridge_anime_genre
    (anime_id INT NOT NULL REFERENCES gold.dim_anime(anime_id) ON DELETE CASCADE,
                                                                    genre_id INT NOT NULL REFERENCES gold.dim_genre(genre_id) ON DELETE CASCADE,
                                                                                                                                   PRIMARY KEY (anime_id,
                                                                                                                                                genre_id));

-- One anime → many studios, one studio → many anime

CREATE TABLE gold.bridge_anime_studio
    (anime_id INT NOT NULL REFERENCES gold.dim_anime(anime_id) ON DELETE CASCADE,
                                                                    studio_id INT NOT NULL REFERENCES gold.dim_studio(studio_id) ON DELETE CASCADE,
                                                                                                                                      is_main_studio BOOLEAN DEFAULT FALSE, -- flag the animation studio
 PRIMARY KEY (anime_id,
              studio_id));

-- ============================================================
-- FACT TABLES
-- ============================================================
 -- Community scores snapshot — one row per anime per pipeline run
-- This lets you track how scores change over time

CREATE TABLE gold.fact_anime_snapshot (snapshot_id SERIAL PRIMARY KEY,
                                                     anime_id INT NOT NULL REFERENCES gold.dim_anime(anime_id),
                                                                                      season_id INT REFERENCES gold.dim_season(season_id), -- Scores (can change each pull)
 average_score NUMERIC(5, 2), -- AniList score out of 100
 mean_score NUMERIC(5, 2),
            popularity INT, -- number of users with it on list
 favourites INT, -- When this snapshot was taken
 snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
                                     ingested_at TIMESTAMPTZ DEFAULT NOW(), -- Prevent duplicate snapshots on same day for same anime
 UNIQUE (anime_id,
         snapshot_date));

-- Your personal watch list — one row per anime you've interacted with

CREATE TABLE gold.fact_user_watchlist (entry_id SERIAL PRIMARY KEY, 
                                                  anime_id INT NOT NULL REFERENCES gold.dim_anime(anime_id), 
                                                                                   platform TEXT NOT NULL CHECK (platform IN ('anilist', 
                                                                                                                              'mal')), username TEXT NOT NULL, -- Your personal data
 my_status TEXT CHECK (my_status IN ('current', 
                                     'completed', 
                                     'paused', 
                                     'dropped', 
                                     'planning')), my_score NUMERIC(4, 1), -- 0–10 (AniList default)
 my_progress INT, -- episodes watched
 my_repeat INT DEFAULT 0, -- times rewatched
 my_notes TEXT, -- Dates
 my_started_at TIMESTAMPTZ,
 my_completed_at TIMESTAMPTZ,
 my_updated_at TIMESTAMPTZ,
 ingested_at TIMESTAMPTZ DEFAULT NOW(),
                                 updated_at TIMESTAMPTZ DEFAULT NOW(), -- One entry per anime per platform per user
 UNIQUE (anime_id,
         platform,
         username));

-- ============================================================
-- INDEXES (speeds up your most common queries)
-- ============================================================
 -- dim_anime lookups by AniList ID — used in every join

CREATE INDEX idx_dim_anime_anilist_id ON gold.dim_anime(anilist_id);

-- Filter anime by format/status often

CREATE INDEX idx_dim_anime_format ON gold.dim_anime(format);


CREATE INDEX idx_dim_anime_status ON gold.dim_anime(status);

-- Snapshot queries — "scores this month", "trend over time"

CREATE INDEX idx_snapshot_date ON gold.fact_anime_snapshot(snapshot_date);


CREATE INDEX idx_snapshot_anime ON gold.fact_anime_snapshot(anime_id);

-- Watchlist queries — "my completed anime", "what I scored 8+"

CREATE INDEX idx_watchlist_status ON gold.fact_user_watchlist(my_status);


CREATE INDEX idx_watchlist_score ON gold.fact_user_watchlist(my_score);


CREATE INDEX idx_watchlist_anime ON gold.fact_user_watchlist(anime_id);
