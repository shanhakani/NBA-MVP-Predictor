import pandas as pd
import requests
import numpy as np

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
teams.insert(0, 'Team ID', [team_mapping[name] for name in teams['Team Name'].to_list()]) # Maps team name to Team ID and creates new column

print(teams)

# Load to Postgres
