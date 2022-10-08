import pandas as pd
import requests
import numpy as np

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
print(mvp_candidates)

# Load Data to PostgreSQL Database


