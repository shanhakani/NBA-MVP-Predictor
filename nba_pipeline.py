import pandas as pd
import requests
import numpy as np

# All teams in the nba
teams = ['ATL', 'BRK', 'BOS', 'CHO', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA',
 'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']

# teams = ['ATL']

# These values list the minimum stats of previous players who have won an MVP award
min_mvp_stats = {'G' : [49], 'PTS/G' : [13.8], 'FGA' : [10.9], 'TRB' : [3.3], 'AST' : [1.3], 'FG%' : [0.378], 'MP' : [30.4]}
mvp_candidates = pd.DataFrame()

#Extract
for team in teams:
	url = f'https://www.basketball-reference.com/teams/{team}/2022.html'
	html = requests.get(url).content
	df_list = pd.read_html(html)
	df = df_list[1]

	# Transform Data - Filter data based on 
	df.rename(columns={'Unnamed: 1':'Name'}, inplace=True )

	for key, val in min_mvp_stats.items():
		df = df[df[key] > val[0]]

	df.insert(0, 'Team', [team] * df.shape[0])
	mvp_candidates = pd.concat([mvp_candidates, df])

	print(f'{team} done')

print()
mvp_candidates = mvp_candidates.sort_values(by=['PTS/G'], ascending = False)
mvp_candidates = mvp_candidates.reset_index(drop = True)
print(mvp_candidates)