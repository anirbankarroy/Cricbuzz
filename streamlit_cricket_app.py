"""
Streamlit Cricket Learning App
GitHub-ready version with relative paths

Features:
1. Home Page - project description and navigation
2. Live Match Page - fetches live data from a user-provided Cricbuzz-like API endpoint (or shows mock data)
3. Top Player Stats Page - shows top batting/bowling stats from the local DB
4. SQL Queries & Analytics Page - runs prepared SQL queries against local sqlite DB
5. CRUD Operations Page - forms to Create, Read, Update, Delete players and matches
"""

import streamlit as st
import pandas as pd
import sqlite3
import requests
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# ---------------------- Paths ----------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "cricket.db")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})

SAMPLE_CSV = os.path.join(DATA_DIR, "bat.csv")

# ---------------------- Database helpers ----------------------
def init_db(engine):
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            country TEXT,
            playing_role TEXT,
            batting_style TEXT,
            bowling_style TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY,
            name TEXT,
            country TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS venues (
            id INTEGER PRIMARY KEY,
            name TEXT,
            city TEXT,
            country TEXT,
            capacity INTEGER
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY,
            description TEXT,
            team1_id INTEGER,
            team2_id INTEGER,
            venue_id INTEGER,
            match_date TEXT,
            match_type TEXT,
            status TEXT,
            winning_team_id INTEGER,
            win_margin INTEGER,
            win_type TEXT,
            toss_winner_id INTEGER,
            toss_decision TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS player_stats (
            player_id INTEGER,
            format TEXT,
            matches_played INTEGER,
            runs INTEGER,
            wickets INTEGER,
            centuries INTEGER,
            batting_average REAL,
            bowling_average REAL,
            strike_rate REAL,
            economy REAL
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS performances (
            match_id INTEGER,
            player_id INTEGER,
            team_id INTEGER,
            runs_scored INTEGER,
            balls_faced INTEGER,
            batting_position INTEGER,
            overs_bowled REAL,
            runs_conceded INTEGER,
            wickets_taken INTEGER,
            catches INTEGER,
            stumpings INTEGER,
            innings_number INTEGER
        );
        """))

init_db(ENGINE)

# ---------------------- Load sample CSV ----------------------
if os.path.exists(SAMPLE_CSV):
    try:
        df_sample = pd.read_csv(SAMPLE_CSV)
        cols = [c.lower() for c in df_sample.columns]
        insert_df = pd.DataFrame()

        if "player" in cols or "name" in cols:
            name_col = df_sample.columns[[c.lower() in ("player","name","player_name") for c in df_sample.columns]][0]
            insert_df["first_name"] = df_sample[name_col].astype(str).apply(lambda x: x.split()[0] if len(x.split())>0 else x)
            insert_df["last_name"] = df_sample[name_col].astype(str).apply(lambda x: " ".join(x.split()[1:]) if len(x.split())>1 else "")
        if "country" in cols:
            insert_df["country"] = df_sample[[c for c in df_sample.columns if c.lower()=="country"][0]]
        if "role" in cols or "playing_role" in cols:
            insert_df["playing_role"] = df_sample[[c for c in df_sample.columns if c.lower() in ("role","playing_role")][0]]

        if not insert_df.empty:
            insert_df.drop_duplicates(subset=["first_name","last_name"], inplace=True)
            insert_df.to_sql("players", ENGINE, if_exists="append", index=False)
    except Exception:
        pass

# ---------------------- SQL Queries (examples) ----------------------
SQL_QUERIES = {
    1: """
    SELECT id, first_name || ' ' || last_name AS full_name, playing_role, batting_style, bowling_style
    FROM players WHERE country = 'India';
    """,
    2: """
    SELECT m.description, t1.name AS team1, t2.name AS team2, v.name AS venue_name, v.city, m.match_date
    FROM matches m
    JOIN teams t1 ON m.team1_id = t1.id
    JOIN teams t2 ON m.team2_id = t2.id
    JOIN venues v ON m.venue_id = v.id
    WHERE DATE(m.match_date) >= DATE('now','-30 days') ORDER BY m.match_date DESC;
    """
    # Add more up to 25 queries...
}

def run_query(sql):
    with ENGINE.connect() as conn:
        try:
            df = pd.read_sql(text(sql), conn)
            return df
        except Exception as e:
            return pd.DataFrame({'error': [str(e)]})

# ---------------------- Live match fetcher ----------------------
def fetch_live_matches(api_url: str):
    if not api_url:
        return [
            {
                'match_id': 1,
                'description': 'India vs Australia - 2nd ODI',
                'team1': 'India',
                'team2': 'Australia',
                'venue': 'Eden Gardens, Kolkata',
                'status': 'In Progress',
                'score': 'India 220/3 (36.4 ov)'
            },
            {
                'match_id': 2,
                'description': 'England vs Pakistan - T20I',
                'team1': 'England',
                'team2': 'Pakistan',
                'venue': 'Lords, London',
                'status': 'Delayed',
                'score': None
            }
        ]
    try:
        resp = requests.get(api_url, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {'error': str(e)}

# ---------------------- Streamlit UI ----------------------
st.set_page_config(page_title='Cricket Learning App', layout='wide')
st.title('Cricket Learning & SQL Practice App')

PAGES = ['Home', 'Live Matches', 'Top Player Stats', 'SQL Queries & Analytics', 'CRUD Operations']
page = st.sidebar.radio('Navigate', PAGES)

# ---------------------- Home ----------------------
if page == 'Home':
    st.header('Project Overview')
    st.markdown("""
    This Streamlit app is an educational tool for cricket analytics and SQL practice.
    """)
    if os.path.exists(SAMPLE_CSV):
        st.success(f'Sample CSV found at {SAMPLE_CSV} and loaded (if mapping matched).')

# ---------------------- Live Matches ----------------------
elif page == 'Live Matches':
    st.header('Live Matches')
    api_url = st.text_input('Live API endpoint (optional)')
    if st.button('Fetch Live Matches'):
        data = fetch_live_matches(api_url.strip())
        if isinstance(data, dict) and data.get('error'):
            st.error('Error fetching: ' + data.get('error'))
        else:
            df = pd.DataFrame(data)
            st.table(df)
            if not df.empty:
                sel = st.selectbox('Select match', df.index.tolist(), format_func=lambda i: df.loc[i,'description'])
                st.json(df.loc[sel].to_dict())

# ---------------------- Top Player Stats ----------------------
elif page == 'Top Player Stats':
    st.header('Top Player Stats')
    with ENGINE.connect() as conn:
        try:
            top_runs = pd.read_sql(text("""
                SELECT p.first_name || ' ' || p.last_name AS player, SUM(ps.runs) AS total_runs
                FROM player_stats ps JOIN players p ON ps.player_id = p.id
                GROUP BY ps.player_id ORDER BY total_runs DESC LIMIT 10;
            """), conn)
            top_wickets = pd.read_sql(text("""
                SELECT p.first_name || ' ' || p.last_name AS player, SUM(ps.wickets) AS total_wickets
                FROM player_stats ps JOIN players p ON ps.player_id = p.id
                GROUP BY ps.player_id ORDER BY total_wickets DESC LIMIT 10;
            """), conn)
        except Exception as e:
            top_runs = pd.DataFrame({'error':[str(e)]})
            top_wickets = pd.DataFrame({'error':[str(e)]})

    st.subheader('Top 10 Run-scorers')
    st.table(top_runs)
    st.subheader('Top 10 Wicket-takers')
    st.table(top_wickets)

    if not top_runs.empty and 'total_runs' in top_runs.columns:
        st.line_chart(top_runs.set_index('player')['total_runs'])
    if not top_wickets.empty and 'total_wickets' in top_wickets.columns:
        st.bar_chart(top_wickets.set_index('player')['total_wickets'])

# ---------------------- SQL Queries ----------------------
elif page == 'SQL Queries & Analytics':
    st.header('SQL Queries & Analytics')
    query_id = st.selectbox('Choose query', list(SQL_QUERIES.keys()), format_func=lambda x: f'Query {x}')
    st.code(SQL_QUERIES[query_id])
    if st.button('Run Query'):
        df = run_query(SQL_QUERIES[query_id])
        if 'error' in df.columns:
            st.error(df['error'].iloc[0])
        else:
            st.dataframe(df)
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button('Download CSV', data=csv, file_name=f'query_{query_id}_results.csv')

# ---------------------- CRUD ----------------------
elif page == 'CRUD Operations':
    st.header('CRUD Operations')
    entity = st.selectbox('Entity', ['players','matches'])

    if entity == 'players':
        st.subheader('Create new player')
        with st.form('create_player'):
            fn = st.text_input('First name')
            ln = st.text_input('Last name')
            country = st.text_input('Country')
            role = st.text_input('Playing role')
            bat_style = st.text_input('Batting style')
            bowl_style = st.text_input('Bowling style')
            submitted = st.form_submit_button('Create')
            if submitted:
                with ENGINE.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO players (first_name,last_name,country,playing_role,batting_style,bowling_style)
                        VALUES (:fn,:ln,:country,:role,:bat,:bowl)
                    """), {'fn':fn,'ln':ln,'country':country,'role':role,'bat':bat_style,'bowl':bowl_style})
                st.success('Player created')

        st.subheader('View players')
        df_players = pd.read_sql(text('SELECT * FROM players ORDER BY id DESC LIMIT 500'), ENGINE)
        st.dataframe(df_players)

    elif entity == 'matches':
        st.subheader('Create new match')
        with st.form('create_match'):
            desc = st.text_input('Description')
            t1 = st.number_input('Team 1 ID', min_value=1, value=1)
            t2 = st.number_input('Team 2 ID', min_value=1, value=2)
            vid = st.number_input('Venue ID', min_value=1, value=1)
            mdate = st.date_input('Match date', value=datetime.utcnow().date())
            mtype = st.selectbox('Match type', ['Test','ODI','T20I'])
            status = st.selectbox('Status', ['scheduled','in_progress','completed'])
            submitted = st.form_submit_button('Create')
            if submitted:
                with ENGINE.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO matches (description,team1_id,team2_id,venue_id,match_date,match_type,status)
                        VALUES (:desc,:t1,:t2,:vid,:mdate,:mtype,:status)
                    """), {'desc':desc,'t1':t1,'t2':t2,'vid':vid,'mdate':mdate.isoformat(),'mtype':mtype,'status':status})
                st.success('Match created')

        st.subheader('View matches')
        df_matches = pd.read_sql(text('SELECT * FROM matches ORDER BY id DESC LIMIT 500'), ENGINE)
        st.dataframe(df_matches)

# ---------------------- Sidebar ----------------------
st.sidebar.markdown('---')
st.sidebar.markdown('Need help? See the README.md in the repo.')
