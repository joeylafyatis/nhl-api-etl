import json
import numpy as np
import pandas as pd
import requests
import sqlite3

NHL_API = 'https://statsapi.web.nhl.com/api/v1/{}'
NHL_DB = sqlite3.connect('nhl.db')

lambda_get = lambda x: requests.get(NHL_API.format(x)).json()
lambda_df = lambda x: pd.concat([ pd.io.json.json_normalize(y,sep='_') for y in x ],sort=1)

def refresh_table(table,spec,df):
    print('Starting table refresh for {}...'.format(table))
    df = df[spec['select_col']]
    df.columns = spec['rename_col']
    df.to_sql(
        name=table,
        con=NHL_DB,
        if_exists='replace',
        index=0,
        dtype=spec['cast_dtypes']
    )
    print('Finished table refresh for {}.'.format(table))

def request_api(table,spec):
    print('Accessing NHL API data for {}...'.format(table))
    if spec['standard_refresh']:
        endpoint = spec['api_endpoint']
        get = lambda_get(endpoint)[endpoint]
        df = lambda_df(get)
    else:
        func_switch = {
            'player': refresh_player,
            'game': refresh_game,
            'gamelog': refresh_gamelog,
            'standings': refresh_standings
        }
        df = func_switch[table](table,spec)
    return { 'table': table, 'spec': spec, 'df': df }

def refresh_player(table,spec):
    sql = 'SELECT team_id FROM team;'
    team_ids = [ t[0] for t in NHL_DB.cursor().execute(sql).fetchall() ]
    endpoints = [ 'teams/{}?expand=team.roster'.format(t) for t in team_ids ]
    rosters = [ lambda_get(e)['teams'][0]['roster']['roster'] for e in endpoints ]
    df_rosters = lambda_df(rosters)

    player_ids = df_rosters['person_id'].tolist()
    endpoints = [ spec['api_endpoint'].format(p) for p in player_ids ]
    players = [ lambda_get(e)['people'] for e in endpoints ]
    return lambda_df(players)

def refresh_game(table,spec):
    [current_season] = lambda_get('seasons/current')['seasons']
    endpoint = spec['api_endpoint'].format( **current_season)
    games = [ e['games'] for e in lambda_get(endpoint)['dates'] ]
    return lambda_df(games)

def refresh_gamelog(table,spec):
    sql = 'SELECT player_id FROM player;'
    player_ids = [ p[0] for p in NHL_DB.cursor().execute(sql).fetchall() ]
    df_gamelogs = pd.DataFrame()
    for p in player_ids:
        api = spec['api_endpoint'].format(p)
        get_stats = lambda_get(api)['stats'][0]['splits']
        df_stats = pd.io.json.json_normalize(get_stats,sep='_')
        df_stats['player_id'] = p
        df_gamelogs = df_gamelogs.append(df_stats,sort=1)
    return df_gamelogs

    # sql = 'SELECT player_id FROM player;'
    # player_ids = [ p[0] for p in NHL_DB.cursor().execute(sql).fetchall() ]
    # endpoints = [ spec['api_endpoint'].format(p) for p in player_ids ]
    # gamelogs = [ lambda_get(e)['stats'][0]['splits'] for e in endpoints ]
    # df_gamelogs = lambda_df(gamelogs)
    # ...

def refresh_standings(table,spec):
    standings = lambda_get(spec['api_endpoint'])['records']
    df_standings = pd.concat([ lambda_df(s['teamRecords']) for s in standings ])
    return df_standings

def main():
    with open('table_specs.json') as f:
        table_specs = json.load(f).items()
        full_refresh = [ refresh_table( **request_api(k,v)) for k,v in table_specs ]

if __name__ == '__main__':
    main()
