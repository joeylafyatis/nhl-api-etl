import json
import numpy as np
import pandas as pd
import requests
import sqlite3

NHL_DB = sqlite3.connect('nhl.db')
NHL_API = 'https://statsapi.web.nhl.com/api/v1/{}'

# BEGIN WIP
standard_get = lambda x: requests.get(NHL_API.format(x)).json()
# def standard_get(base,modifier):
#     r = requests.get(NHL_API.format(base)).json()
#     #incorporate modifier?
# 
# concat_df = lambda x: pd.concat([ pd.io.json.json_normalize(x)
# END WIP

def refresh_table(df_source,db_table):
    with open('table_specs.json') as f:
        spec = json.load(f)[db_table]
    df_source = df_source[spec['select_col']]
    df_source.columns = spec['rename_col']
    df_source.to_sql(
        name=db_table,
        con=NHL_DB,
        if_exists='replace',
        index=0,
        dtype=spec['cast_dtypes']
    )

def standard_refresh(api_endpoint):
    r = standard_get(api_endpoint)[api_endpoint]
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=1)
    refresh_table(df,api_endpoint[:-1])

def refresh_players():
    team_ids = NHL_DB.cursor().execute('SELECT team_id FROM team;')
    api_template = 'teams/{}?expand=team.roster'
    rosters = [ api_template.format(t[0]) for t in team_ids.fetchall() ]
    get_rosters = [ standard_get(x)['teams'][0]['roster']['roster'] for x in rosters ]
    df_rosters = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in get_rosters ],sort=1)

    player_ids = df_rosters['person_id'].tolist()
    players = [ 'people/{}'.format(x) for x in player_ids ]
    get_players = [ standard_get(x)['people'] for x in players ]
    df_players = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in get_players ],sort=1)
    refresh_table(df_players,'player')

def refresh_games():
    current_season = standard_get('seasons/current')['seasons']
    endpoint = 'schedule?\
        startDate={regularSeasonStartDate}&\
        endDate={regularSeasonEndDate}'.format( **current_season)
    get_games = standard_get(endpoint)['dates']
    df_games = pd.concat([ pd.io.json.json_normalize(x['games'],sep='_') for x in get_games ],sort=1)
    refresh_table(df_games,'game')

def refresh_gamelogs():
    c = NHL_DB.cursor().execute('SELECT player_id FROM player;')
    df_gamelogs = pd.DataFrame()
    for x in c.fetchall():
        player_id = x[0]
        api = 'people/{}/stats?stats=gameLog&season=20192020'.format(player_id)
        get_stats = standard_get(api)['stats'][0]['splits']
        df_stats = pd.io.json.json_normalize(get_stats,sep='_')
        df_stats['player_id'] = player_id
        df_gamelogs = df_gamelogs.append(df_stats,sort=1)
    refresh_table(df_gamelogs,'gamelog')

def refresh_standings():
    get_standings = standard_get('standings')['records']
    df_standings = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in get_standings ],sort=1)
    df_standings = df_standings[['division_id','teamRecords']]

    flatten_records = np.dstack((np.repeat(df_standings.division_id.values,list(map(len,df_standings.teamRecords.values))),np.concatenate(df_standings.teamRecords.values)))
    df_standings = pd.DataFrame(data=flatten_records[0],columns=df_standings.columns)
    for x in ['teamRecords','team','leagueRecord','streak']:
        df_standings = pd.concat([df_standings,df_standings[x].apply(pd.Series)],axis=1)
    refresh_table(df_standings,'standings')

def main():
    [ standard_refresh(x) for x in ['conferences', 'divisions', 'teams'] ] #push "refresh_type" into table_specs?
    refresh_players()
    refresh_games()
    refresh_gamelogs()
    refresh_standings()

if __name__ == '__main__':
    main()
