import requests
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

# All teams in the nba
teams = ['ATL', 'BRK', 'BOS', 'CHO', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA',
 'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']

# Testing - Use to run code on a single team
# teams = ['ATL'] 

# These values list the minimum values for various stats of previous players who have won an MVP award
min_mvp_stats = {'G' : [49], 'PTS/G' : [13.8], 'FGA' : [10.9], 'TRB' : [3.3], 'AST' : [1.3], 'FG%' : [0.378], 'MP' : [30.4]}
mvp_candidates = pd.DataFrame()

#Extract Data
for team in teams:
	url = f'https://www.basketball-reference.com/teams/{team}/2022.html'
	html = requests.get(url).content
	df_list = pd.read_html(html)
	df = df_list[1]

	# Transform Data - Filter data based on minimum season stats from previous MVP winners
	df.rename(columns={'Unnamed: 1':'Name'}, inplace=True )
	df = df.drop(['Rk'], axis=1) # Removes Rank Column

	for key, val in min_mvp_stats.items():
		df = df[df[key] > val[0]]

	df.insert(0, 'Team', [team] * df.shape[0]) # adds column 'Team' that will be used as foreign key for Teams tables
	mvp_candidates = pd.concat([mvp_candidates, df])

	print(f'{team} done') # testing - prints message to show when code is done running on team

print()
mvp_candidates = mvp_candidates.sort_values(by=['PTS/G'], ascending = False)
mvp_candidates = mvp_candidates.reset_index(drop = True) # Resets index
mvp_candidates = mvp_candidates.reset_index(drop = False) # Adds index column
mvp_candidates.rename(columns={'index':'player_id'}, inplace = True) # Extracts index to get player id as primary key for SQL table
mvp_candidates.rename(columns = {'player_id' : 'player_id', 'Team' : 'team', 'Name' : 'player_name', 'Age' : 'age',
 'G' : 'gp', 'GS' : 'gs', 'MP' : 'mp', 'FG' : 'fg', 'FGA' : 'fga', 'FG%' : 'fg_pct', '3P' : 'three_point', 
 '3PA' : 'three_pa', '3P%' : 'three_pct', '2P' : 'two_point', '2PA' : 'two_pa', '2P%' : 'two_pct', 'eFG%' : 'efg_pct', 
 'FT' : 'ft', 'FTA' : 'fta', 'FT%' : 'ft_pct', 'ORB' : 'orb', 'DRB' : 'drb', 'TRB' : 'trb', 'AST' : 'ast', 'STL' : 'stl', 
 'BLK' : 'blk', 'TOV' : 'tov', 'PF' : 'pf', 'PTS/G' : 'ppg'}, inplace = True)
print(mvp_candidates.to_string())

# Load Data to PostgreSQL Database

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
  
# Drop table if it already exists
cursor.execute('drop table if exists mvp_candidates')
  
# Create mvp_candidates Table
sql = '''CREATE TABLE IF NOT EXISTS mvp_candidates (
	player_id INT  PRIMARY KEY,
	team VARCHAR(3) NOT NULL,
	player_name VARCHAR(50) NOT NULL,
	age INT NOT NULL,
	gp INT NOT NULL,
	gs INT NOT NULL,
	mp NUMERIC NOT NULL,
	fg NUMERIC NOT NULL,
	fga NUMERIC NOT NULL,
	fg_pct NUMERIC NOT NULL,
	three_point NUMERIC NOT NULL,
	three_pa NUMERIC NOT NULL,
	three_pct NUMERIC NOT NULL,
	two_point NUMERIC NOT NULL,
	two_pa NUMERIC NOT NULL,
	two_pct NUMERIC NOT NULL,
	eFG_pct NUMERIC NOT NULL,
	ft NUMERIC NOT NULL,
	fta NUMERIC NOT NULL,
	ft_pct NUMERIC NOT NULL,
	orb NUMERIC NOT NULL,
	drb NUMERIC NOT NULL,
	trb NUMERIC NOT NULL,
	ast NUMERIC NOT NULL,
	stl NUMERIC NOT NULL,
	blk NUMERIC NOT NULL,
	tov NUMERIC NOT NULL,
	pf NUMERIC NOT NULL,
	ppg NUMERIC NOT NULL,
	CONSTRAINT fk_team
		FOREIGN KEY(team) 
			REFERENCES teams (team_id)
);'''
  
cursor.execute(sql)

mvp_candidates.to_sql('mvp_candidates', conn, index = False ,if_exists = 'append')

conn1.commit()
conn1.close()

print('Loaded to Postgres')


