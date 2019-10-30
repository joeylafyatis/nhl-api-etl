import json
import numpy as np
import pandas as pd
import requests
import sqlite3

NHL_API = 'https://statsapi.web.nhl.com/api/v1/{}'
NHL_DB = sqlite3.connect('nhl.db')

lambda_get = lambda x: requests.get(NHL_API.format(x)).json()
lambda_df = lambda x: pd.concat([ pd.io.json.json_normalize(y,sep='_') for y in x ],sort=1)

def refresh_table(df,table,spec):
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

def iter_tables(table,spec):
    print('Accessing NHL API for {}...'.format(table))
    if spec['standard_refresh']:
        endpoint = spec['api_endpoint']
        get = lambda_get(endpoint)[endpoint]
        df = lambda_df(get)
        refresh_table(df,table,spec)
    else:
        func_switch = {
            'player': refresh_player,
            'game': refresh_game,
            'gamelog': refresh_gamelog,
            'standings': refresh_standings
        }
        func_switch[table](table,spec)

def refresh_player(table,spec):
    team_ids = NHL_DB.cursor().execute('SELECT team_id FROM team;').fetchall()
    endpoints = [ 'teams/{}?expand=team.roster'.format(t[0]) for t in team_ids ]
    rosters = [ lambda_get(e)['teams'][0]['roster']['roster'] for e in endpoints ]
    df_rosters = lambda_df(rosters)

    player_ids = df_rosters['person_id'].tolist()
    endpoints = [ spec['api_endpoint'].format(p) for p in player_ids ]
    players = [ lambda_get(e)['people'] for e in endpoints ]
    df_players = lambda_df(players)
    refresh_table(df_players,table,spec)

def refresh_game(table,spec):
    [current_season] = lambda_get('seasons/current')['seasons']
    endpoint = spec['api_endpoint'].format( **current_season)
    games = [ g['games'] for g in lambda_get(endpoint)['dates'] ]
    df_games = lambda_df(games)
    refresh_table(df_games,table,spec)

def refresh_gamelog(table,spec):
    player_ids = NHL_DB.cursor().execute('SELECT player_id FROM player;')
    df_gamelogs = pd.DataFrame()
    for x in player_ids.fetchall():
        player_id = x[0]
        api = spec['api_endpoint'].format(player_id)
        get_stats = lambda_get(api)['stats'][0]['splits']
        df_stats = pd.io.json.json_normalize(get_stats,sep='_')
        df_stats['player_id'] = player_id
        df_gamelogs = df_gamelogs.append(df_stats,sort=1)
    refresh_table(df_gamelogs,table,spec)

    # endpoints = [ 'people/{}/stats?stats=gameLog&season=20192020'.format(p[0]) for p in player_ids.fetchall() ]
    # gamelogs = [ lambda_get(e)['stats'][0]['splits'] for e in endpoints ]
    # df_gamelogs = lambda_df(stats)
    # ...

def refresh_standings(table,spec):
    standings = lambda_get(spec['api_endpoint'])['records']
    df_standings = lambda_df(standings)[['division_id','teamRecords']]

    # BEGIN GARBAGE
    flatten_records = np.dstack((np.repeat(df_standings.division_id.values,list(map(len,df_standings.teamRecords.values))),np.concatenate(df_standings.teamRecords.values)))
    df_standings = pd.DataFrame(data=flatten_records[0],columns=df_standings.columns)
    for x in ['teamRecords','team','leagueRecord','streak']:
        df_standings = pd.concat([df_standings,df_standings[x].apply(pd.Series)],axis=1)
    # END GARBAGE
    refresh_table(df_standings,table,spec)

def main():
    with open('table_specs.json') as f:
        [ iter_tables(k,v) for k,v in json.load(f).items() ]

if __name__ == '__main__':
    main()

# NOTE TO SELF: move endpoint strings into table_specs.json
