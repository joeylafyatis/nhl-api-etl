import json
import numpy as np
import pandas as pd
import requests
import sqlite3

NHL_API = 'https://statsapi.web.nhl.com/api/v1/{}'
[CURRENT_SEASON] = requests.get(NHL_API.format('seasons/current')).json()['seasons']
NHL_DB = sqlite3.connect('nhl.db')

def refresh_table(df_source,db_table):
    with open('table_specs.json') as f:
        data = json.load(f)[db_table]
    df_source = df_source[data['select_columns']]
    df_source.columns = data['rename_columns']
    df_source.to_sql(name=db_table,con=NHL_DB,if_exists='replace',index=0,dtype=data['cast_dtypes'])

def standard_refresh(endpoint):
    r = requests.get(NHL_API.format(endpoint)).json()[endpoint]
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=1)
    refresh_table(df,endpoint[:-1])

def refresh_players():
    # BEGIN GARBAGE
    c = NHL_DB.cursor().execute('SELECT team_id FROM team;')
    rosters = [ 'teams/{}?expand=team.roster'.format(x[0]) for x in c.fetchall() ]
    get_rosters = [ requests.get(NHL_API.format(x)).json()['teams'][0]['roster']['roster'] for x in rosters ]
    df_rosters = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in get_rosters ],sort=1)

    player_ids = df_rosters['person_id'].tolist()
    players = [ 'people/{}'.format(x) for x in player_ids ]
    get_players = [ requests.get(NHL_API.format(x)).json()['people'] for x in players ]
    df_players = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in get_players ],sort=1)
    # END GARBAGE
    refresh_table(df_players,'player')

def refresh_games():
    endpoint = 'schedule?startDate={regularSeasonStartDate}&endDate={regularSeasonEndDate}'.format( **CURRENT_SEASON)
    get_games = requests.get(NHL_API.format(endpoint)).json()['dates']
    df_games = pd.concat([ pd.io.json.json_normalize(x['games'],sep='_') for x in get_games ],sort=1)
    refresh_table(df_games,'game')

def refresh_gamelogs():
    # BEGIN GARBAGE
    c = NHL_DB.cursor().execute('SELECT player_id FROM player;')
    df_gamelogs = pd.DataFrame()
    for x in c.fetchall():
        player_id = x[0]
        api = 'people/{}/stats?stats=gameLog&season=20192020'.format(player_id)
        get_stats = requests.get(NHL_API.format(api)).json()['stats'][0]['splits']
        df_stats = pd.io.json.json_normalize(get_stats,sep='_')
        df_stats['player_id'] = player_id
        df_gamelogs = df_gamelogs.append(df_stats,sort=1)
    # END GARBAGE
    refresh_table(df_gamelogs,'gamelog')

def refresh_standings():
    # BEGIN GARBAGE
    get_standings = requests.get(NHL_API.format('standings')).json()['records']
    df_standings = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in get_standings ],sort=1)
    df_standings = df_standings[['division_id','teamRecords']]

    flatten_records = np.dstack((np.repeat(df_standings.division_id.values,list(map(len,df_standings.teamRecords.values))),np.concatenate(df_standings.teamRecords.values)))
    df_standings = pd.DataFrame(data=flatten_records[0],columns=df_standings.columns)
    for x in ['teamRecords','team','leagueRecord','streak']:
        df_standings = pd.concat([df_standings,df_standings[x].apply(pd.Series)],axis=1)
    # END GARBAGE
    refresh_table(df_standings,'standings')

def main():
    [ standard_refresh(x) for x in ['conferences', 'divisions', 'teams'] ]
    refresh_players()
    refresh_games()
    refresh_gamelogs()
    refresh_standings()

if __name__ == '__main__':
    main()
