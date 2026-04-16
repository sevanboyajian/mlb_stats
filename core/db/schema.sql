-- ============================================================
--  LTCWBB MLB BACKTESTING DATABASE
--  schema.sql  ·  SQLite 3.x  ·  compatible with DuckDB
--
--  PURPOSE:
--    Store 10 years of MLB game/player stats alongside Vegas
--    odds (game lines + player props) to backtest predictive
--    models against oddsmakers.
--
--  DATA SOURCES:
--    Stats  → statsapi.mlb.com  (daily 6am pull)
--    Odds   → the-odds-api.com  (daily pull + historical backfill)
--
--  DESIGN PRINCIPLES:
--    1. Star schema — facts tables join to dimension tables
--    2. Odds and results are ALWAYS decoupled by timestamp
--       (never use outcome data in a pre-game prediction query)
--    3. ingest_log tracks every pull — script is always resumable
--    4. model_predictions must be written BEFORE game_start_utc

--    5. backtest_results is written AFTER final score is known
--
--  TABLE ORDER (dependency safe):
--    1. Reference     : seasons, venues, teams, players
--    2. Schedule      : games
--    3. Stats         : player_game_stats, play_by_play, standings
--    4. Odds          : game_odds, player_props, line_movement
--    5. Backtesting   : model_predictions, backtest_results
--    6. Operations    : ingest_log, odds_ingest_log
-- ============================================================
-- # CHANGE LOG (latest first)
-- # -------------------------
-- # 2026-04-16  pipeline_job_runs: per-execution audit log; duration_seconds on finish
-- # 2026-04-15  team_rolling_stats: pre-game rolling team metrics (builder-populated)
-- # 2026-04-14 09:15 ET  
-- Eastern Time normalized date for all daily logic and reporting
-- MUST be used instead of game_date for filtering
-- ============================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;     -- allows reads during writes
PRAGMA synchronous  = NORMAL;  -- safe + faster than FULL for bulk inserts


-- ============================================================
-- 1. REFERENCE TABLES
-- ============================================================

-- ------------------------------------------------------------
-- seasons
-- One row per MLB season. Used to scope queries and validate
-- date ranges during ingestion.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS seasons (
    season          INTEGER PRIMARY KEY,   -- e.g. 2024
    sport_id        INTEGER NOT NULL DEFAULT 1,  -- 1 = MLB
    season_start    DATE    NOT NULL,
    season_end      DATE    NOT NULL,
    postseason_start DATE,
    postseason_end   DATE,
    regular_games    INTEGER,              -- expected games in regular season
    notes           TEXT                  -- e.g. 'COVID shortened 60-game season'
);


-- ------------------------------------------------------------
-- venues
-- MLB stadiums. Rarely changes — new parks open every decade.
--
-- COLUMN GROUPS:
--   Core identity  : venue_id, name, city, state, country
--   Physical       : capacity, surface, roof_type, field dimensions
--   Geography      : elevation_ft, latitude, longitude
--   Wind/weather   : wind_effect, wind_note, orientation_hp, cf_direction
--                    Populated by add_stadium_data.py (one-time seed).
--                    NOT written by load_mlb_stats.py — safe across re-pulls.
--   Park factors   : park_factor_runs, park_factor_hr (3-yr rolling avg)
--   Metadata       : opened_year, last_updated
--
-- wind_effect values:
--   HIGH       — open air, meaningfully wind-exposed (Wrigley, Fenway, Coors)
--   MODERATE   — open air but sheltered or low avg wind (PNC, Camden, Dodger)
--   LOW        — retractable roof; wind applies only when roof is open
--   SUPPRESSED — architecture neutralises wind OR fixed dome (Oracle, Tropicana,
--                loanDepot). Wind signals NEVER apply at SUPPRESSED venues.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS venues (
    venue_id        INTEGER PRIMARY KEY,  -- MLB's own venue ID
    name            TEXT    NOT NULL,
    city            TEXT    NOT NULL,
    state           TEXT,
    country         TEXT    NOT NULL DEFAULT 'USA',
    capacity        INTEGER,
    surface         TEXT    CHECK (surface IN ('Grass','Turf','Mixed')),
    roof_type       TEXT    CHECK (roof_type IN ('Open','Dome','Retractable','Fixed Dome')),
    left_line_ft    INTEGER,              -- ballpark dimensions
    left_center_ft  INTEGER,
    center_ft       INTEGER,
    right_center_ft INTEGER,
    right_line_ft   INTEGER,
    elevation_ft    INTEGER,             -- affects ball flight (feet above sea level)
    latitude        REAL,               -- for weather API cross-reference
    longitude       REAL,

    -- ── Wind / weather signal fields (populated by add_stadium_data.py) ──
    wind_effect     TEXT    CHECK (wind_effect IN ('HIGH','MODERATE','LOW','SUPPRESSED')),
    wind_note       TEXT,               -- human-readable commentary for brief output
    orientation_hp  TEXT,               -- compass direction home plate faces (NE, SE, E …)
    cf_direction    TEXT,               -- compass direction of CF from home plate

    -- ── Park factors (3-year rolling, updated annually) ──────────────────
    park_factor_runs  INTEGER DEFAULT 100,  -- 100 = league avg; >100 = hitter-friendly
    park_factor_hr    INTEGER DEFAULT 100,

    -- ── Metadata ─────────────────────────────────────────────────────────
    altitude_note   TEXT,               -- narrative for notable-altitude parks only
    opened_year     INTEGER,
    last_updated    TEXT                -- ISO date this row was last verified
);


-- ------------------------------------------------------------
-- teams
-- 30 MLB teams. Stable — only changes on franchise relocation.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS teams (
    team_id         INTEGER PRIMARY KEY,  -- MLB's own team ID
    name            TEXT    NOT NULL,     -- e.g. 'New York Yankees'
    abbreviation    TEXT    NOT NULL,     -- e.g. 'NYY'
    short_name      TEXT,                 -- e.g. 'Yankees'
    league          TEXT    NOT NULL CHECK (league IN ('AL','NL')),
    division        TEXT    NOT NULL CHECK (division IN ('East','Central','West')),
    venue_id        INTEGER REFERENCES venues (venue_id),
    first_year      INTEGER,
    active          INTEGER NOT NULL DEFAULT 1  -- 1=active, 0=relocated/defunct
);


-- ------------------------------------------------------------
-- players
-- Every player who appeared in a game 2015-present.
-- Slowly changing — position and team change frequently,
-- biographical data is static.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS players (
    player_id       INTEGER PRIMARY KEY,  -- MLB person ID (personId)
    full_name       TEXT    NOT NULL,
    first_name      TEXT,
    last_name       TEXT    NOT NULL,
    birth_date      DATE,
    birth_city      TEXT,
    birth_country   TEXT,
    height_inches   INTEGER,
    weight_lbs      INTEGER,
    bats            TEXT    CHECK (bats    IN ('L','R','S')),   -- S = Switch
    throws          TEXT    CHECK (throws  IN ('L','R','S')),
    primary_position TEXT,               -- e.g. 'P', 'C', '1B', 'OF'
    debut_date      DATE,
    active          INTEGER NOT NULL DEFAULT 1,
    last_updated    DATETIME
);


-- ============================================================
-- 2. SCHEDULE / GAMES
-- ============================================================

-- ------------------------------------------------------------
-- games
-- One row per game. The universal anchor for all other tables.
-- game_start_utc is CRITICAL — all odds must be captured before
-- this timestamp to be valid for backtesting.
-- Added date_et column to store the game date in EST.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS games (
    game_pk             INTEGER PRIMARY KEY,  -- MLB's gamePk — used in all API calls
    season              INTEGER NOT NULL REFERENCES seasons (season),
    game_date           DATE    NOT NULL,
    game_date_et        DATE,            -- Game date in US/Eastern (official local date); use for ET-based daily grouping/reporting
    game_type           TEXT    NOT NULL
                            CHECK (game_type IN ('R','S','P','A','E','D')),
                            -- R=Regular, S=Spring, P=Postseason
                            -- A=AllStar, E=Exhibition, D=Division Series
    series_description  TEXT,            -- e.g. 'World Series', 'ALDS'
    series_game_number  INTEGER,

    home_team_id        INTEGER NOT NULL REFERENCES teams (team_id),
    away_team_id        INTEGER NOT NULL REFERENCES teams (team_id),
    venue_id            INTEGER REFERENCES venues (venue_id),

    -- Schedule
    game_start_utc      DATETIME,        -- scheduled first pitch (UTC)
    game_end_utc        DATETIME,        -- actual end time
    game_duration_min   INTEGER,

    -- Score (NULL until final)
    home_score          INTEGER,
    away_score          INTEGER,
    innings_played      INTEGER,
    extra_innings       INTEGER NOT NULL DEFAULT 0,

    -- Game status
    status              TEXT    NOT NULL DEFAULT 'Scheduled'
                            CHECK (status IN (
                                'Scheduled','Pre-Game','In Progress',
                                'Final','Postponed','Suspended','Cancelled'
                            )),
    postpone_reason     TEXT,            -- e.g. 'Rain', 'Snow'

    -- Weather at first pitch
    temp_f              INTEGER,
    wind_mph            INTEGER,
    wind_direction      TEXT,
    sky_condition       TEXT,            -- 'Clear', 'Cloudy', 'Roof Closed', etc.

    -- Attendance
    attendance          INTEGER,

    -- Double header
    double_header       TEXT    CHECK (double_header IN ('N','Y','S')),
                            -- N=No, Y=Yes, S=Split doubleheader
    game_number         INTEGER NOT NULL DEFAULT 1,
    game_date_est       DATE
);

CREATE INDEX IF NOT EXISTS idx_games_date    ON games (game_date);
CREATE INDEX IF NOT EXISTS idx_games_season  ON games (season);
CREATE INDEX IF NOT EXISTS idx_games_home    ON games (home_team_id, season);
CREATE INDEX IF NOT EXISTS idx_games_away    ON games (away_team_id, season);
CREATE INDEX IF NOT EXISTS idx_games_status  ON games (status);


-- ============================================================
-- 3. STATS TABLES
-- ============================================================

-- ------------------------------------------------------------
-- player_game_stats
-- One row per player per game per role (batter or pitcher).
-- A two-way player like Shohei Ohtani has TWO rows per game.
-- ~1.1 million rows for 10 years.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_game_stats (
    id              INTEGER PRIMARY KEY,
    game_pk         INTEGER NOT NULL REFERENCES games (game_pk),
    player_id       INTEGER NOT NULL REFERENCES players (player_id),
    team_id         INTEGER NOT NULL REFERENCES teams (team_id),
    player_role     TEXT    NOT NULL CHECK (player_role IN ('batter','pitcher')),
    batting_order   INTEGER,             -- 1-9, NULL for pitchers not batting
    position        TEXT,                -- actual position played in this game

    -- ── BATTING ──────────────────────────────────────────────
    at_bats         INTEGER,
    plate_appearances INTEGER,
    runs            INTEGER,
    hits            INTEGER,
    doubles         INTEGER,
    triples         INTEGER,
    home_runs       INTEGER,
    rbi             INTEGER,
    walks           INTEGER,
    intentional_walks INTEGER,
    strikeouts_bat  INTEGER,
    stolen_bases    INTEGER,
    caught_stealing INTEGER,
    hit_by_pitch    INTEGER,
    sac_flies       INTEGER,
    sac_bunts       INTEGER,
    left_on_base    INTEGER,
    ground_into_dp  INTEGER,
    batting_avg     REAL,                -- stored from API for convenience
    obp             REAL,
    slg             REAL,
    ops             REAL,

    -- ── PITCHING ─────────────────────────────────────────────
    innings_pitched REAL,                -- stored as decimal e.g. 6.333
    pitches_thrown  INTEGER,
    strikes_thrown  INTEGER,
    earned_runs     INTEGER,
    runs_allowed    INTEGER,
    hits_allowed    INTEGER,
    doubles_allowed INTEGER,
    triples_allowed INTEGER,
    hr_allowed      INTEGER,
    walks_allowed   INTEGER,
    ibb_allowed     INTEGER,
    strikeouts_pit  INTEGER,
    hit_batters     INTEGER,
    wild_pitches    INTEGER,
    balks           INTEGER,
    ground_outs     INTEGER,
    air_outs        INTEGER,
    win             INTEGER,             -- 1 or 0
    loss            INTEGER,
    save            INTEGER,
    blown_save      INTEGER,
    hold            INTEGER,
    complete_game   INTEGER,
    shutout         INTEGER,
    quality_start   INTEGER,             -- computed: 6+ IP, 3 or fewer ER
    era             REAL,
    whip            REAL,
    k_per_9         REAL,
    bb_per_9        REAL,

    UNIQUE (game_pk, player_id, player_role)
);

CREATE INDEX IF NOT EXISTS idx_pgs_game     ON player_game_stats (game_pk);
CREATE INDEX IF NOT EXISTS idx_pgs_player   ON player_game_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_pgs_team     ON player_game_stats (team_id);
CREATE INDEX IF NOT EXISTS idx_pgs_role     ON player_game_stats (player_role);
CREATE INDEX IF NOT EXISTS idx_pgs_player_season
    ON player_game_stats (player_id, game_pk);


-- ------------------------------------------------------------
-- play_by_play
-- Every pitch and play in every game.
-- ~7.5 million rows for 10 years. Most granular table.
-- Used for Statcast-style analysis and pitcher/batter matchups.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS play_by_play (
    id              INTEGER PRIMARY KEY,
    game_pk         INTEGER NOT NULL REFERENCES games (game_pk),
    inning          INTEGER NOT NULL,
    inning_half     TEXT    NOT NULL CHECK (inning_half IN ('top','bottom')),
    at_bat_index    INTEGER NOT NULL,    -- sequential AB number in game
    play_index      INTEGER NOT NULL,    -- pitch number within the at-bat

    -- Participants
    batter_id       INTEGER REFERENCES players (player_id),
    pitcher_id      INTEGER REFERENCES players (player_id),
    fielder_ids     TEXT,                -- JSON array of player IDs involved

    -- Play result
    event_type      TEXT,                -- 'strikeout','walk','home_run','single',etc.
    event_code      TEXT,                -- MLB event code
    description     TEXT,               -- human readable play description
    is_scoring_play INTEGER NOT NULL DEFAULT 0,
    runs_scored     INTEGER NOT NULL DEFAULT 0,
    rbi_on_play     INTEGER NOT NULL DEFAULT 0,

    -- Count before pitch
    outs_before     INTEGER,
    balls_before    INTEGER,
    strikes_before  INTEGER,
    on_first        INTEGER,             -- player_id or NULL
    on_second       INTEGER,
    on_third        INTEGER,

    -- Pitch data (when applicable)
    pitch_type      TEXT,                -- 'FF'=4-seam, 'SL'=slider, 'CH'=changeup
    pitch_type_desc TEXT,                -- human readable
    pitch_speed_mph REAL,
    pitch_zone      INTEGER,            -- strike zone location 1-14

    -- Statcast hit data (when applicable)
    hit_trajectory  TEXT,               -- 'fly_ball','ground_ball','line_drive','popup'
    exit_velocity   REAL,
    launch_angle    REAL,
    hit_distance_ft REAL,
    hit_coord_x     REAL,               -- spray chart coordinates
    hit_coord_y     REAL,
    is_barrel       INTEGER,            -- 1 if exit velo 98+ and launch angle 26-30

    UNIQUE (game_pk, at_bat_index, play_index)
);

CREATE INDEX IF NOT EXISTS idx_pbp_game     ON play_by_play (game_pk);
CREATE INDEX IF NOT EXISTS idx_pbp_batter   ON play_by_play (batter_id);
CREATE INDEX IF NOT EXISTS idx_pbp_pitcher  ON play_by_play (pitcher_id);
CREATE INDEX IF NOT EXISTS idx_pbp_event    ON play_by_play (event_type);
CREATE INDEX IF NOT EXISTS idx_pbp_inning   ON play_by_play (game_pk, inning);


-- ------------------------------------------------------------
-- standings
-- Daily snapshot of division standings during the season.
-- Useful feature for predictive model (team momentum, GB, etc.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS standings (
    id              INTEGER PRIMARY KEY,
    snapshot_date   DATE    NOT NULL,
    team_id         INTEGER NOT NULL REFERENCES teams (team_id),
    season          INTEGER NOT NULL REFERENCES seasons (season),

    wins            INTEGER NOT NULL,
    losses          INTEGER NOT NULL,
    win_pct         REAL    NOT NULL,
    games_back      REAL    NOT NULL DEFAULT 0,
    wild_card_gb    REAL,
    division_rank   INTEGER,
    league_rank     INTEGER,
    wild_card_rank  INTEGER,

    -- Last 10 games
    last_10_wins    INTEGER,
    last_10_losses  INTEGER,
    streak          TEXT,                -- e.g. 'W3', 'L2'
    streak_type     TEXT    CHECK (streak_type IN ('W','L')),
    streak_length   INTEGER,

    -- Run differential
    runs_scored     INTEGER,
    runs_allowed    INTEGER,
    run_diff        INTEGER,             -- computed: runs_scored - runs_allowed
    pythag_win_pct  REAL,               -- Pythagorean expected win %

    -- Home / Away splits
    home_wins       INTEGER,
    home_losses     INTEGER,
    away_wins       INTEGER,
    away_losses     INTEGER,

    UNIQUE (snapshot_date, team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_date ON standings (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_standings_team ON standings (team_id, season);


-- ============================================================
-- 4. ODDS TABLES
-- ============================================================

-- ------------------------------------------------------------
-- game_odds
-- Pre-game lines captured per bookmaker per game.
-- Multiple snapshots per game — opening line through close.
-- Key backtest rule: only use rows where
--   captured_at_utc < games.game_start_utc
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS game_odds (
    id                  INTEGER PRIMARY KEY,
    game_pk             INTEGER NOT NULL REFERENCES games (game_pk),
    bookmaker           TEXT    NOT NULL,  -- 'draftkings','fanduel','betmgm',etc.
    data_source         TEXT    NOT NULL DEFAULT 'the-odds-api',
    captured_at_utc     DATETIME NOT NULL,
    hours_before_game   REAL,              -- computed on insert
    market_type         TEXT    NOT NULL
                            CHECK (market_type IN ('moneyline','runline','total')),

    -- ── MONEYLINE ────────────────────────────────────────────
    home_ml             INTEGER,           -- American odds e.g. -150
    away_ml             INTEGER,           -- e.g. +130

    -- ── RUN LINE (always ±1.5 in MLB) ────────────────────────
    home_rl_line        REAL,              -- -1.5 or +1.5
    home_rl_odds        INTEGER,
    away_rl_line        REAL,
    away_rl_odds        INTEGER,

    -- ── TOTAL (over/under) ───────────────────────────────────
    total_line          REAL,              -- e.g. 8.5
    over_odds           INTEGER,
    under_odds          INTEGER,

    -- ── LINE FLAGS ───────────────────────────────────────────
    is_opening_line     INTEGER NOT NULL DEFAULT 0,  -- first snapshot for this game+book
    is_closing_line     INTEGER NOT NULL DEFAULT 0,  -- last snapshot before first pitch

    UNIQUE (game_pk, bookmaker, market_type, captured_at_utc)
);

CREATE INDEX IF NOT EXISTS idx_godds_game     ON game_odds (game_pk);
CREATE INDEX IF NOT EXISTS idx_godds_book     ON game_odds (bookmaker);
CREATE INDEX IF NOT EXISTS idx_godds_market   ON game_odds (market_type);
CREATE INDEX IF NOT EXISTS idx_godds_closing  ON game_odds (game_pk, is_closing_line);
CREATE INDEX IF NOT EXISTS idx_godds_captured ON game_odds (captured_at_utc);


-- ------------------------------------------------------------
-- player_props
-- Pre-game player prop bets per bookmaker.
-- prop_type values match The Odds API market key names.
-- Same backtest timestamp rule: captured_at_utc < game_start_utc
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_props (
    id                  INTEGER PRIMARY KEY,
    game_pk             INTEGER NOT NULL REFERENCES games (game_pk),
    player_id           INTEGER NOT NULL REFERENCES players (player_id),
    bookmaker           TEXT    NOT NULL,
    data_source         TEXT    NOT NULL DEFAULT 'the-odds-api',
    captured_at_utc     DATETIME NOT NULL,
    hours_before_game   REAL,

    prop_type           TEXT    NOT NULL,
    -- BATTER props:
    --   batter_home_runs      batter_hits           batter_total_bases
    --   batter_rbis           batter_runs_scored    batter_stolen_bases
    --   batter_strikeouts     batter_singles        batter_doubles
    -- PITCHER props:
    --   pitcher_strikeouts    pitcher_hits_allowed  pitcher_walks
    --   pitcher_earned_runs   pitcher_outs          pitcher_record_a_win

    line                REAL    NOT NULL,  -- the over/under number e.g. 1.5
    over_odds           INTEGER,
    under_odds          INTEGER,

    is_closing_line     INTEGER NOT NULL DEFAULT 0,

    UNIQUE (game_pk, player_id, bookmaker, prop_type, captured_at_utc)
);

CREATE INDEX IF NOT EXISTS idx_props_game    ON player_props (game_pk);
CREATE INDEX IF NOT EXISTS idx_props_player  ON player_props (player_id);
CREATE INDEX IF NOT EXISTS idx_props_type    ON player_props (prop_type);
CREATE INDEX IF NOT EXISTS idx_props_closing ON player_props (game_pk, player_id, is_closing_line);


-- ------------------------------------------------------------
-- line_movement
-- Aggregated open-to-close line movement per game per book.
-- Derived from game_odds — populated by a daily compute job.
-- Steam moves (sharp money signals) are flagged here.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS line_movement (
    id                  INTEGER PRIMARY KEY,
    game_pk             INTEGER NOT NULL REFERENCES games (game_pk),
    bookmaker           TEXT    NOT NULL,
    market_type         TEXT    NOT NULL,

    -- Opening line (first capture)
    open_home_ml        INTEGER,
    open_away_ml        INTEGER,
    open_total          REAL,
    open_rl_home_odds   INTEGER,
    open_captured_utc   DATETIME,

    -- Closing line (last capture before first pitch)
    close_home_ml       INTEGER,
    close_away_ml       INTEGER,
    close_total         REAL,
    close_rl_home_odds  INTEGER,
    close_captured_utc  DATETIME,

    -- Movement summary (computed)
    ml_move_cents       INTEGER,          -- e.g. -150 → -165 = -15 cents toward home
    total_move          REAL,             -- e.g. 8.5 → 8.0 = -0.5
    move_direction      TEXT    CHECK (move_direction IN ('home','away','over','under','none')),
    steam_move          INTEGER NOT NULL DEFAULT 0,  -- 1 = rapid cross-book move
    reverse_line_move   INTEGER NOT NULL DEFAULT 0,  -- price moved opposite to ticket %

    -- Public betting % (if available from data source)
    home_ticket_pct     REAL,
    away_ticket_pct     REAL,
    over_ticket_pct     REAL,
    under_ticket_pct    REAL,

    UNIQUE (game_pk, bookmaker, market_type)
);

CREATE INDEX IF NOT EXISTS idx_linemove_game ON line_movement (game_pk);
CREATE INDEX IF NOT EXISTS idx_linemove_steam ON line_movement (steam_move);


-- ============================================================
-- 5. BACKTESTING TABLES
-- ============================================================

-- ------------------------------------------------------------
-- model_predictions
-- YOUR model's output written before the game starts.
-- CONSTRAINT: predicted_at_utc must be < games.game_start_utc
-- model_version lets you run multiple model iterations and
-- compare their performance in backtest_results.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_predictions (
    id                  INTEGER PRIMARY KEY,
    game_pk             INTEGER NOT NULL REFERENCES games (game_pk),
    player_id           INTEGER REFERENCES players (player_id),
                            -- NULL for game-level predictions (winner/total)

    -- What kind of prediction
    prediction_type     TEXT    NOT NULL
                            CHECK (prediction_type IN (
                                'game_winner',  -- moneyline pick
                                'run_line',     -- run line pick
                                'total',        -- over/under pick
                                'player_prop'   -- individual stat over/under
                            )),
    prop_type           TEXT,    -- if player_prop: matches player_props.prop_type

    -- The prediction itself
    predicted_side      TEXT    NOT NULL,
                            -- game_winner: 'home' or 'away'
                            -- run_line:    'home' or 'away'
                            -- total:       'over' or 'under'
                            -- player_prop: 'over' or 'under'
    predicted_value     REAL,   -- model's projected stat or probability
                            -- e.g. pitcher projected K = 7.2
                            --      win probability = 0.62

    -- Confidence
    confidence          REAL    CHECK (confidence BETWEEN 0.0 AND 1.0),
                            -- how confident the model is (0=no edge, 1=certain)
    edge_over_market    REAL,   -- model prob - implied prob from odds (key metric)

    -- Model metadata
    model_version       TEXT    NOT NULL DEFAULT 'v1',
    features_used       TEXT,   -- JSON list of features fed to model
    predicted_at_utc    DATETIME NOT NULL,
                            -- !! MUST be before games.game_start_utc !!

    -- Wagering decision
    bet_made            INTEGER NOT NULL DEFAULT 0,  -- 1 if we simulated a wager
    bet_odds_used       INTEGER,  -- American odds at time of bet (from game_odds)
    bet_size_units      REAL    DEFAULT 1.0,
                            -- 1.0 = 1 flat unit
                            -- Kelly sizing can vary this
    bookmaker_used      TEXT,

    -- Sanity check: ensure prediction was made before the game
    CHECK (predicted_at_utc IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_pred_game     ON model_predictions (game_pk);
CREATE INDEX IF NOT EXISTS idx_pred_player   ON model_predictions (player_id);
CREATE INDEX IF NOT EXISTS idx_pred_type     ON model_predictions (prediction_type);
CREATE INDEX IF NOT EXISTS idx_pred_model    ON model_predictions (model_version);
CREATE INDEX IF NOT EXISTS idx_pred_bet      ON model_predictions (bet_made);


-- ------------------------------------------------------------
-- backtest_results
-- Written AFTER the game is final.
-- The P&L ledger — the ultimate measure of model performance.
-- Join to model_predictions to get full picture.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS backtest_results (
    id                  INTEGER PRIMARY KEY,
    prediction_id       INTEGER NOT NULL UNIQUE
                            REFERENCES model_predictions (id),
    game_pk             INTEGER NOT NULL REFERENCES games (game_pk),
    player_id           INTEGER REFERENCES players (player_id),
    model_version       TEXT    NOT NULL,
    graded_at_utc       DATETIME NOT NULL,

    -- What actually happened
    actual_value        REAL,   -- actual stat or final score
    actual_side         TEXT,   -- 'home','away','over','under'
    prediction_correct  INTEGER,  -- 1=correct, 0=wrong, NULL=push

    -- Financial outcome
    bet_outcome         TEXT    CHECK (bet_outcome IN ('win','loss','push','no_action')),
    profit_loss_units   REAL,
                            -- win:  e.g. +0.91 (bet -110 odds)
                            -- loss: e.g. -1.0
                            -- push: 0.0

    -- Closing Line Value (CLV) — the gold standard backtest metric
    -- Did your model beat the number the market settled on?
    closing_line_odds   INTEGER,  -- what the closing line was
    closing_line_value  REAL,     -- your odds vs closing odds (positive = good)
                            -- CLV = implied_prob(closing) - implied_prob(bet_odds)

    -- Running totals (computed incrementally by the grader script)
    running_units_total REAL,    -- cumulative P&L at this point in time
    running_bets_count  INTEGER, -- number of bets made so far (this model version)
    running_roi_pct     REAL     -- running_units_total / running_bets_count * 100
);

CREATE INDEX IF NOT EXISTS idx_btr_prediction ON backtest_results (prediction_id);
CREATE INDEX IF NOT EXISTS idx_btr_game       ON backtest_results (game_pk);
CREATE INDEX IF NOT EXISTS idx_btr_model      ON backtest_results (model_version);
CREATE INDEX IF NOT EXISTS idx_btr_outcome    ON backtest_results (bet_outcome);
CREATE INDEX IF NOT EXISTS idx_btr_correct    ON backtest_results (prediction_correct);


-- ============================================================
-- 6. OPERATIONS / INGEST TRACKING
-- ============================================================

-- ------------------------------------------------------------
-- ingest_log
-- Tracks every MLB Stats API pull by game.
-- The daily script checks this before pulling — never double-pulls.
-- If a pull fails, status='error' and the script retries next run.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_log (
    game_pk             INTEGER PRIMARY KEY REFERENCES games (game_pk),
    first_attempted_utc DATETIME NOT NULL,
    last_attempted_utc  DATETIME NOT NULL,
    status              TEXT    NOT NULL
                            CHECK (status IN ('success','error','skipped','partial')),
    attempts            INTEGER NOT NULL DEFAULT 1,
    boxscore_rows       INTEGER,          -- rows inserted into player_game_stats
    pbp_rows            INTEGER,          -- rows inserted into play_by_play
    error_message       TEXT,
    mlb_api_version     TEXT DEFAULT 'v1'
);

CREATE INDEX IF NOT EXISTS idx_ingest_status ON ingest_log (status);
CREATE INDEX IF NOT EXISTS idx_ingest_date   ON ingest_log (last_attempted_utc);


-- ------------------------------------------------------------
-- odds_ingest_log
-- Tracks every Odds API pull.
-- Separate from ingest_log because odds are pulled on a
-- different schedule (multiple times per day during season).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS odds_ingest_log (
    id                  INTEGER PRIMARY KEY,
    pulled_at_utc       DATETIME NOT NULL,
    pull_type           TEXT    NOT NULL
                            CHECK (pull_type IN (
                                'historical_backfill',  -- Odds API backfill OR sbro_import
                                'daily_pregame',
                                'live_update',
                                'props_update'
                            )),
                            -- NOTE: SBRO flat-file imports use pull_type='historical_backfill'
                            -- with markets_pulled prefixed 'sbro:' to distinguish from API pulls
    sport               TEXT    NOT NULL DEFAULT 'baseball_mlb',
    markets_pulled      TEXT,            -- comma list e.g. 'h2h,totals,spreads'
    games_covered       INTEGER,
    odds_rows_inserted  INTEGER,
    props_rows_inserted INTEGER,
    api_requests_used   INTEGER,         -- The Odds API charges per request
    api_quota_remaining INTEGER,
    status              TEXT    NOT NULL
                            CHECK (status IN ('success','error','partial')),
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_oingest_date ON odds_ingest_log (pulled_at_utc);
CREATE INDEX IF NOT EXISTS idx_oingest_type ON odds_ingest_log (pull_type);

-- ------------------------------------------------------------
-- signal_state
-- Append-only ledger of every signal generated throughout the day
-- (top / next / avoid) so reports can be reconstructed exactly.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_state (
    id              INTEGER PRIMARY KEY,
    game_date        TEXT,
    game_pk          INTEGER,
    market_type      TEXT,    -- 'moneyline','spread','total'
    signal_type      TEXT,    -- 'top','next','avoid'
    bet             TEXT,
    odds            INTEGER,
    session         TEXT,     -- morning, early, primary, etc.
    recorded_at     TEXT
);

-- ------------------------------------------------------------
-- bet_ledger
-- Source of truth for P&L: append-only record of actual bets taken
-- (separate from signal generation).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bet_ledger (
    id              INTEGER PRIMARY KEY,
    game_date        TEXT,
    game_pk          INTEGER,
    market_type      TEXT,
    bet             TEXT,
    odds_taken       INTEGER,
    stake_units      REAL,
    signal_at_time   TEXT,    -- 'top','next','avoid'
    session         TEXT,
    placed_at        TEXT,
    result          TEXT,     -- 'win','loss','push'
    pnl_units        REAL
);

-- Enforce idempotent bet creation (one bet per game + market)
CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_ledger_game_market
    ON bet_ledger (game_pk, market_type);

-- ------------------------------------------------------------
-- pipeline_jobs
-- Scheduler queue for pipeline work (odds pulls, briefs, weather, etc.).
-- One row per job instance. Designed to be filled by a separate scheduler.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type        TEXT    NOT NULL,   -- e.g. 'odds_pull','brief','weather'
    job_date_et     TEXT    NOT NULL,   -- Eastern date YYYY-MM-DD (for filtering)
    scheduled_time_et TEXT   NOT NULL,  -- 'YYYY-MM-DD HH:MM ET' (human-readable primary)
    scheduled_time_utc DATETIME,        -- optional: UTC timestamp/ISO for machine scheduling
    window_start_et TEXT,               -- optional: start of intended execution window (ET)
    window_end_et   TEXT,               -- optional: end of intended execution window (ET)
    status          TEXT    NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','running','complete','failed')),
    game_group_id   INTEGER,            -- cluster id from game start grouping
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Idempotent scheduling (same job_type + time + group should not duplicate)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_jobs_unique
    ON pipeline_jobs (job_type, scheduled_time_et, game_group_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_time
    ON pipeline_jobs (status, scheduled_time_et);


-- ------------------------------------------------------------
-- pipeline_job_runs
-- One row per job execution attempt (started when runner claims the job).
-- duration_seconds is set only when the run finishes (new executions);
-- existing rows are never backfilled by the runner.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_job_runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id              INTEGER NOT NULL,
    job_type            TEXT,
    job_date_et         TEXT,
    started_at_utc      TEXT    NOT NULL,
    finished_at_utc     TEXT,
    duration_seconds    REAL,
    status              TEXT,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_job_id
    ON pipeline_job_runs (job_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_started
    ON pipeline_job_runs (started_at_utc);


-- ============================================================
-- USEFUL VIEWS
-- Pre-built queries for common backtesting operations.
-- These do NOT store data — they are computed on the fly.
-- ============================================================

-- ------------------------------------------------------------
-- v_closing_game_odds
-- The last odds snapshot before first pitch for each game.
-- Use this view for all backtesting — never the raw table.
--
-- BOOKMAKER PRIORITY (fixed Mar 2026):
--   Original view returned one row per bookmaker per market type,
--   causing 11x fan-out in load_games() due to triple JOIN.
--   Fixed by selecting exactly ONE bookmaker per game+market using
--   a priority ladder: live sharp books first, historical sources
--   as fallback. Guarantees exactly one row per game per market
--   regardless of how many bookmakers are loaded.
--
--   Priority: draftkings → fanduel → betmgm → betonlineag
--             → sbro → oddswarehouse → any other
-- ------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_closing_game_odds AS
SELECT
    go.game_pk,
    go.bookmaker,
    go.market_type,
    go.home_ml,
    go.away_ml,
    go.total_line,
    go.over_odds,
    go.under_odds,
    go.home_rl_line,
    go.home_rl_odds,
    go.away_rl_line,
    go.away_rl_odds,
    go.captured_at_utc,
    go.hours_before_game
FROM game_odds go
WHERE go.is_closing_line = 1
  AND go.bookmaker = (
      SELECT go2.bookmaker
      FROM game_odds go2
      WHERE go2.game_pk     = go.game_pk
        AND go2.market_type = go.market_type
        AND go2.is_closing_line = 1
        AND go2.bookmaker IN (
            'draftkings',
            'fanduel',
            'betmgm',
            'betonlineag',
            'sbro',
            'oddswarehouse'
        )
      ORDER BY
          CASE go2.bookmaker
              WHEN 'draftkings'    THEN 1
              WHEN 'fanduel'       THEN 2
              WHEN 'betmgm'        THEN 3
              WHEN 'betonlineag'   THEN 4
              WHEN 'sbro'          THEN 5
              WHEN 'oddswarehouse' THEN 6
              ELSE                      7
          END
      LIMIT 1
  );


-- ------------------------------------------------------------
-- v_closing_player_props
-- Closing props only — use for backtesting prop predictions.
-- ------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_closing_player_props AS
SELECT
    pp.game_pk,
    pp.player_id,
    p.full_name,
    pp.bookmaker,
    pp.prop_type,
    pp.line,
    pp.over_odds,
    pp.under_odds,
    pp.captured_at_utc,
    pp.hours_before_game
FROM player_props pp
JOIN players p ON p.player_id = pp.player_id
WHERE pp.is_closing_line = 1;


-- ------------------------------------------------------------
-- v_prediction_summary
-- Joins predictions to results for quick model evaluation.
-- Filter by model_version to compare models head-to-head.
-- ------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_prediction_summary AS
SELECT
    mp.model_version,
    mp.prediction_type,
    mp.prop_type,
    g.season,
    g.game_date,
    g.game_pk,
    t_home.abbreviation    AS home_team,
    t_away.abbreviation    AS away_team,
    pl.full_name           AS player_name,
    mp.predicted_side,
    mp.predicted_value,
    mp.confidence,
    mp.edge_over_market,
    mp.bet_made,
    mp.bet_odds_used,
    mp.bet_size_units,
    br.actual_value,
    br.actual_side,
    br.prediction_correct,
    br.bet_outcome,
    br.profit_loss_units,
    br.closing_line_value,
    br.running_units_total,
    br.running_roi_pct
FROM model_predictions mp
JOIN games   g      ON g.game_pk      = mp.game_pk
JOIN teams   t_home ON t_home.team_id = g.home_team_id
JOIN teams   t_away ON t_away.team_id = g.away_team_id
LEFT JOIN players pl ON pl.player_id  = mp.player_id
LEFT JOIN backtest_results br ON br.prediction_id = mp.id;


-- ------------------------------------------------------------
-- v_model_performance
-- Aggregate ROI stats per model version and prediction type.
-- The top-level scorecard for "are we smarter than Vegas?"
-- ------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_model_performance AS
SELECT
    mp.model_version,
    mp.prediction_type,
    mp.prop_type,
    COUNT(*)                                        AS total_predictions,
    SUM(mp.bet_made)                                AS bets_placed,
    SUM(CASE WHEN br.prediction_correct = 1
             THEN 1 ELSE 0 END)                     AS correct,
    ROUND(
        100.0 * SUM(CASE WHEN br.prediction_correct = 1
                         THEN 1 ELSE 0 END)
        / NULLIF(COUNT(br.id), 0), 1)               AS hit_rate_pct,
    ROUND(SUM(COALESCE(br.profit_loss_units, 0)), 2) AS total_units,
    ROUND(
        100.0 * SUM(COALESCE(br.profit_loss_units, 0))
        / NULLIF(SUM(mp.bet_made), 0), 2)           AS roi_pct,
    ROUND(AVG(COALESCE(br.closing_line_value, 0)), 4) AS avg_clv
FROM model_predictions mp
LEFT JOIN backtest_results br ON br.prediction_id = mp.id
GROUP BY mp.model_version, mp.prediction_type, mp.prop_type
ORDER BY roi_pct DESC;


-- ------------------------------------------------------------
-- team_rolling_stats
-- Pre-game rolling team metrics per (game_pk, team_id). Populated by a
-- separate builder job (not core ingestion). One row per team per game.
-- join key for briefs/signals: (game_pk, team_id) == batting team.
-- See batch/analysis/backtesting/backtest_team_vs_pitcher.py optional join.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS team_rolling_stats (
    game_pk         INTEGER NOT NULL REFERENCES games (game_pk),
    team_id         INTEGER NOT NULL REFERENCES teams (team_id),
    game_date       TEXT    NOT NULL,
    season          INTEGER NOT NULL,

    games_in_window INTEGER NOT NULL,

    rolling_runs_scored_pg   REAL,
    rolling_runs_allowed_pg  REAL,
    rolling_run_diff_pg      REAL,

    rolling_obp             REAL,
    rolling_slg             REAL,
    rolling_ops             REAL,
    rolling_iso             REAL,

    rolling_k_pct          REAL,
    rolling_bb_pct         REAL,

    rolling_hr_pg          REAL,

    rolling_sp_era         REAL,
    rolling_sp_k9          REAL,
    rolling_sp_whip        REAL,
    sp_starts_in_window    INTEGER,

    rolling_runs_scored_home_pg REAL,
    rolling_runs_scored_road_pg REAL,
    rolling_ops_home            REAL,
    rolling_ops_road             REAL,
    home_games_in_window         INTEGER,
    road_games_in_window         INTEGER,

    computed_at     TEXT    NOT NULL,

    PRIMARY KEY (game_pk, team_id)
);

CREATE INDEX IF NOT EXISTS idx_trs_team_date
    ON team_rolling_stats (team_id, game_date);

CREATE INDEX IF NOT EXISTS idx_trs_game
    ON team_rolling_stats (game_pk);

CREATE INDEX IF NOT EXISTS idx_trs_season
    ON team_rolling_stats (season, team_id);


-- ============================================================
-- SEED DATA — static reference rows
-- ============================================================

-- MLB Leagues (for reference in queries)
-- AL = 103, NL = 104 in MLB API
-- Divisions: ALE=200, ALC=202, ALW=200, NLE=204, NLC=205, NLW=203

INSERT OR IGNORE INTO seasons (season, season_start, season_end, regular_games) VALUES
    (2015, '2015-04-05', '2015-10-04', 162),
    (2016, '2016-04-03', '2016-10-02', 162),
    (2017, '2017-04-02', '2017-10-01', 162),
    (2018, '2018-03-29', '2018-09-30', 162),
    (2019, '2019-03-28', '2019-09-29', 162),
    (2020, '2020-07-23', '2020-09-27', 60),   -- COVID shortened season
    (2021, '2021-04-01', '2021-10-03', 162),
    (2022, '2022-04-07', '2022-10-05', 162),
    (2023, '2023-03-30', '2023-10-01', 162),
    (2024, '2024-03-20', '2024-09-29', 162),
    (2025, '2025-03-27', '2025-09-28', 162);

-- ============================================================
-- END OF SCHEMA
-- ============================================================
