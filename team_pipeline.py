import requests
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

# Create mapping for Team Name to Team ID
team_mapping = {'Atlanta Hawks': 'ATL', 'Brooklyn Nets': 'BRK', 'Boston Celtics': 'BOS', 'Charlotte Hornets': 'CHO',
 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE', 'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN',
  'Detroit Pistons': 'DET', 'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
   'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM', 'Miami Heat': 'MIA',
    'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN', 'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK',
     'Oklahoma City Thunder': 'OKC', 'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHO',
      'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS', 'Toronto Raptors': 'TOR',
       'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'}

# Extract Data
url = 'https://www.basketball-reference.com/leagues/NBA_2022_standings.html'
html = requests.get(url).content
df_list = pd.read_html(html)

# Transform Data
east = df_list[0] # Eastern Conference Data
east.rename(columns={'Eastern Conference':'Team Name'}, inplace=True )
east.insert(0, 'Conference', ['East'] * east.shape[0]) # Add column to identify as East Team

west = df_list[1] # Western Conference Data
west.rename(columns={'Western Conference':'Team Name'}, inplace=True )
west.insert(0, 'Conference', ['West'] * west.shape[0]) # Add column to identiy as West Team

teams = pd.concat([east, west]) # Merge dataframes to load into Postgres
teams = teams.reset_index(drop = False)
teams.rename(columns={'index':'Ranking'}, inplace = True) # Extracts index to get team's rank in conference
teams['Team Name'] = teams['Team Name'].str.replace('*', '', regex = False) # Removes asterisk used to identify playoff qualifying team on basketball-reference.com
teams = teams.replace('—', 0.0)
teams.insert(0, 'Team ID', [team_mapping[name] for name in teams['Team Name'].to_list()]) # Maps team name to Team ID and creates new column
teams.rename(columns = {'Team ID' : 'team_id', 'Ranking' : 'ranking', 'Conference' : 'conference', 'Team Name' : 'team_name',
 'W' : 'wins', 'L' : 'losses', 'W/L%' : 'win_loss_pct', 'GB' : 'gb', 'PS/G' : 'ppg', 'PA/G' : 'opponent_ppg', 
 'SRS' : 'ppg_difference'}, inplace = True)

print(teams)

# Load to PostgreSQL Database

# Create a .txt file in directory where first line is password (protection for public remote repository)
f = open("account.txt","r")
lines = f.readlines()
password = lines[0]
f.close()

conn_string = f'postgresql://postgres:{password}@127.0.0.1/nba'
  
db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(database = "nba", user = 'postgres', password = password, host='127.0.0.1', port= '5432')
  
conn1.autocommit = True
cursor = conn1.cursor()
  
# Drop both tables if already exist (teams table is foreign key to mvp_candidates table, so we must drop both)
cursor.execute('drop table if exists mvp_candidates, teams')
  
# Create Teams Table
sql = '''CREATE TABLE IF NOT EXISTS teams (
    team_id VARCHAR(3) PRIMARY KEY,
    ranking INT NOT NULL,
    conference VARCHAR(4) NOT NULL,
    team_name VARCHAR(50) NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    win_loss_pct NUMERIC NOT NULL,
    gb NUMERIC,
    ppg NUMERIC NOT NULL,
    opponent_ppg NUMERIC NOT NULL,
    ppg_difference NUMERIC NOT NULL
);'''
  
cursor.execute(sql)

teams.to_sql('teams', conn, index = False ,if_exists = 'append')

conn1.commit()
conn1.close()

print('Loaded to Postgres')