import json
import numpy as np
import pandas as pd
import requests
import sqlite3

NHL_API = 'https://statsapi.web.nhl.com/api/v1/{}'
[CURRENT_SEASON] = requests.get(NHL_API.format('seasons/current')).json()['seasons']
NHL_DB = sqlite3.connect('nhl.db')

def refresh_table(df,table_spec):
    with open('table_specs.json') as f:
        data = json.load(f)[table_spec]
    df = df[data['select_columns']]
    df.columns = data['rename_columns']
    df.to_sql(name=table_spec,con=NHL_DB,if_exists='replace',index=False,dtype=data['cast_columns'])

def refresh_conferences():
    r = requests.get(NHL_API.format('conferences')).json()['conferences']
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    refresh_table(df,'conference')

def refresh_divisions():
    r = requests.get(NHL_API.format('divisions')).json()['divisions']
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    refresh_table(df,'division')

def refresh_teams():
    r = requests.get(NHL_API.format('teams')).json()['teams']
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    refresh_table(df,'team')

def refresh_players():
    c = NHL_DB.cursor().execute('SELECT team_id FROM team;')
    rosters = [ 'teams/{}?expand=team.roster'.format(x[0]) for x in c.fetchall() ]
    r1 = [ requests.get(NHL_API.format(x)).json()['teams'][0]['roster']['roster'] for x in rosters ]
    df_rosters = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r1 ],sort=True)
    player_ids = df_rosters['person_id'].tolist()
    players = [ 'people/{}'.format(x) for x in player_ids ]
    r2 = [ requests.get(NHL_API.format(x)).json()['people'] for x in players ]
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r2 ],sort=True)
    refresh_table(df,'player')

def refresh_games():
    endpoint = 'schedule?startDate={regularSeasonStartDate}&endDate={regularSeasonEndDate}'.format( **CURRENT_SEASON)
    r = requests.get(NHL_API.format(endpoint)).json()['dates']
    df = pd.concat([ pd.io.json.json_normalize(x['games'],sep='_') for x in r ],sort=True)
    refresh_table(df,'game')

def refresh_gamelogs():
    c = NHL_DB.cursor().execute('SELECT player_id FROM player;')
    df = pd.DataFrame()
    for x in c.fetchall():
        player_id = x[0]
        api = 'people/{}/stats?stats=gameLog&season=20192020'.format(player_id)
        r = requests.get(NHL_API.format(api)).json()['stats'][0]['splits']
        df_playerstats = pd.io.json.json_normalize(r,sep='_')
        df_playerstats['player_id'] = player_id
        df = df.append(df_playerstats,sort=True)
    refresh_table(df,'gamelog')

def refresh_standings():
    r = requests.get(NHL_API.format('standings')).json()['records']
    df = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    df = df[['division_id','teamRecords']]

    flatten_records = np.dstack((np.repeat(df.division_id.values,list(map(len,df.teamRecords.values))),np.concatenate(df.teamRecords.values)))
    df = pd.DataFrame(data=flatten_records[0],columns=df.columns)
    for x in ['teamRecords','team','leagueRecord','streak']:
        df = pd.concat([df,df[x].apply(pd.Series)],axis=1)
    refresh_table(df,'standings')

def main():
    refresh_conferences()
    refresh_divisions()
    refresh_teams()
    refresh_players()
    refresh_games()
    refresh_gamelogs()
    refresh_standings()

if __name__ == '__main__':
    main()
