import numpy as np
import pandas as pd
import requests
import sqlite3

API_URL = 'https://statsapi.web.nhl.com/api/v1/{}'
CURRENT_SEASON = requests.get(API_URL.format('seasons/current')).json()['seasons'][0]
NHL_DB = sqlite3.connect('nhl.db')

def refresh_conferences():
    r = requests.get(API_URL.format('conferences')).json()['conferences']
    df_conferences = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    df_conferences = df_conferences[[
        'id',
        'name',
        'shortName',
        'abbreviation',
        'link',
        'active'
    ]]
    df_conferences.columns = [
        'conference_id',
        'conference_name',
        'conference_shortname',
        'conference_abbr',
        'api_link',
        'is_active'
    ]
    df_conferences.to_sql(name='conference',con=NHL_DB,if_exists='replace',index=False)

def refresh_divisions():
    r = requests.get(API_URL.format('divisions')).json()['divisions']
    df_divisions = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    df_divisions = df_divisions[[
        'id',
        'name',
        'nameShort',
        'abbreviation',
        'conference_id',
        'link',
        'active'
    ]]
    df_divisions.columns = [
        'division_id',
        'division_name',
        'division_shortname',
        'division_abbr',
        'conference_id',
        'api_link',
        'is_active'
    ]
    df_divisions.to_sql(name='division',con=NHL_DB,if_exists='replace',index=False)

def refresh_teams():
    r = requests.get(API_URL.format('teams')).json()['teams']
    df_teams = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    df_teams = df_teams[[
        'id',
        'name',
        'shortName',
        'teamName',
        'abbreviation',
        'division_id',
        'firstYearOfPlay',
        'locationName',
        'venue_name',
        'venue_city',
        'officialSiteUrl',
        'link',
        'active'
    ]]
    df_teams.columns = [
        'team_id',
        'team_fullname',
        'team_shortname',
        'team_name',
        'team_abbr',
        'division_id',
        'first_year_of_play',
        'location_name',
        'venue_name',
        'venue_city',
        'official_site_url',
        'api_link',
        'is_active'
    ]
    df_teams.to_sql(name='team',con=NHL_DB,if_exists='replace',index=False,
        dtype={'first_year_of_play': 'integer'})

def refresh_players():
    c = NHL_DB.cursor().execute('SELECT team_id FROM team;')
    rosters = [ 'teams/{}?expand=team.roster'.format(x[0]) for x in c.fetchall() ]
    r1 = [ requests.get(API_URL.format(x)).json()['teams'][0]['roster']['roster'] for x in rosters ]
    df_rosters = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r1 ],sort=True)

    player_ids = df_rosters['person_id'].tolist()
    players = [ 'people/{}'.format(x) for x in player_ids ]
    r2 = [ requests.get(API_URL.format(x)).json()['people'] for x in players ]
    df_players = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r2 ],sort=True)
    df_players = df_players[[
        'id',
        'firstName',
        'lastName',
        'fullName',
        'currentAge',
        'birthDate',
        'birthCity',
        'birthStateProvince',
        'birthCountry',
        'nationality',
        'height',
        'weight',
        'shootsCatches',
        'primaryPosition_type',
        'primaryPosition_name',
        'primaryPosition_code',
        'primaryPosition_abbreviation',
        'currentTeam_id',
        'primaryNumber',
        'captain',
        'alternateCaptain',
        'rookie',
        'rosterStatus',
        'link',
        'active'
    ]]
    df_players.columns = [
        'player_id',
        'player_firstname',
        'player_lastname',
        'player_fullname',
        'age',
        'birth_date',
        'birth_city',
        'birth_stateprovince',
        'birth_country',
        'nationality',
        'height',
        'weight',
        'shoots_catches',
        'position_type',
        'position_name',
        'position_code',
        'position_abbr',
        'current_team_id',
        'primary_number',
        'is_captain',
        'is_alternate_captain',
        'is_rookie',
        'roster_status',
        'api_link',
        'is_active'
    ]
    df_players.to_sql(name='player',con=NHL_DB,if_exists='replace',index=False,
        dtype={'primary_number': 'integer'})

def refresh_games():
    endpoint = 'schedule?startDate={regularSeasonStartDate}&endDate={regularSeasonEndDate}'.format( **CURRENT_SEASON)
    r = requests.get(API_URL.format(endpoint)).json()['dates']
    df_games = pd.concat([ pd.io.json.json_normalize(x['games'],sep='_') for x in r ],sort=True)
    df_games = df_games[[
        'gamePk',
        'gameDate',
        'season',
        'gameType',
        'status_statusCode',
        'status_detailedState',
        'teams_home_team_id',
        'teams_home_score',
        'teams_home_leagueRecord_wins',
        'teams_home_leagueRecord_losses',
        'teams_home_leagueRecord_ot',
        'teams_away_team_id',
        'teams_away_score',
        'teams_away_leagueRecord_wins',
        'teams_away_leagueRecord_losses',
        'teams_away_leagueRecord_ot',
        'venue_id',
        'venue_name',
        'venue_link',
        'link',
        'content_link'
    ]]
    df_games.columns = [
        'game_id',
        'game_datetime',
        'season',
        'game_type',
        'status_code',
        'status_name',
        'home_team_id',
        'home_team_score',
        'home_team_record_wins',
        'home_team_record_losses',
        'home_team_record_ot',
        'away_team_id',
        'away_team_score',
        'away_team_record_wins',
        'away_team_record_losses',
        'away_team_record_ot',
        'venue_id',
        'venue_name',
        'venue_link',
        'api_link',
        'content_link'
    ]
    df_games.to_sql(name='game',con=NHL_DB,if_exists='replace',index=False,
        dtype={
            'season': 'integer',
            'status_code': 'integer',
            'venue_id': 'integer'
        }
    )

def refresh_gamelogs():
    c = NHL_DB.cursor().execute('SELECT player_id FROM player;')
    df_gamelogs = pd.DataFrame()
    for x in c.fetchall():
        player_id = x[0]
        api = 'people/{}/stats?stats=gameLog&season=20192020'.format(player_id)
        r = requests.get(API_URL.format(api)).json()['stats'][0]['splits']
        df_playerstats = pd.io.json.json_normalize(r,sep='_')
        df_playerstats['player_id'] = player_id
        df_gamelogs = df_gamelogs.append(df_playerstats,sort=True)

    df_gamelogs = df_gamelogs[[
        'player_id',
        'game_gamePk',
        'stat_assists',
        'stat_blocked',
        'stat_decision',
        'stat_evenSaves',
        'stat_evenShots',
        'stat_evenStrengthSavePercentage',
        'stat_evenTimeOnIce',
        'stat_faceOffPct',
        'stat_gameWinningGoals',
        'stat_games',
        'stat_gamesStarted',
        'stat_goals',
        'stat_goalsAgainst',
        'stat_hits',
        'stat_ot',
        'stat_overTimeGoals',
        'stat_penaltyMinutes',
        'stat_pim',
        'stat_plusMinus',
        'stat_points',
        'stat_powerPlayGoals',
        'stat_powerPlayPoints',
        'stat_powerPlaySavePercentage',
        'stat_powerPlaySaves',
        'stat_powerPlayShots',
        'stat_powerPlayTimeOnIce',
        'stat_savePercentage',
        'stat_saves',
        'stat_shifts',
        'stat_shortHandedGoals',
        'stat_shortHandedPoints',
        'stat_shortHandedSavePercentage',
        'stat_shortHandedSaves',
        'stat_shortHandedShots',
        'stat_shortHandedTimeOnIce',
        'stat_shotPct',
        'stat_shots',
        'stat_shotsAgainst',
        'stat_shutouts',
        'stat_timeOnIce'
    ]]
    df_gamelogs.columns = [
        'player_id',
        'game_id',
        'assists',
        'blocked',
        'decision',
        'even_saves',
        'even_shots',
        'even_strength_save_percentage',
        'even_time_on_ince',
        'face_off_pct',
        'game_winning_goals',
        'games',
        'games_started',
        'goals',
        'goals_against',
        'hits',
        'ot',
        'over_time_goals',
        'penalty_minutes',
        'pim',
        'plus_minus',
        'points',
        'power_play_goals',
        'power_play_points',
        'power_play_save_percentage',
        'power_play_saves',
        'power_play_shots',
        'power_play_time_on_ice',
        'save_percentage',
        'saves',
        'shifts',
        'short_handed_goals',
        'short_handed_points',
        'short_handed_save_percentage',
        'short_handed_saves',
        'short_handed_shots',
        'short_handed_time_on_ice',
        'shot_pct',
        'shots',
        'shots_against',
        'shutouts',
        'time_on_ice'
    ]
    df_gamelogs.to_sql(name='gamelog',con=NHL_DB,if_exists='replace',index=False,
        dtype={
            'game_id': 'integer',
            'points': 'integer',
            'goals': 'integer',
            'assists': 'integer',
            'plus_minus': 'integer',
            'shots': 'integer'
        }
    )

def refresh_standings():
    r = requests.get(API_URL.format('standings')).json()['records']
    df_standings = pd.concat([ pd.io.json.json_normalize(x,sep='_') for x in r ],sort=True)
    df_standings = df_standings[['division_id','teamRecords']]

    flatten_records = np.dstack((np.repeat(df_standings.division_id.values,list(map(len,df_standings.teamRecords.values))),np.concatenate(df_standings.teamRecords.values)))
    df_standings = pd.DataFrame(data=flatten_records[0],columns=df_standings.columns)
    for x in ['teamRecords','team','leagueRecord','streak']:
        df_standings = pd.concat([df_standings,df_standings[x].apply(pd.Series)],axis=1)
    df_standings = df_standings[[
        'division_id',
        'divisionRank',
        'id',
        'points',
        'wins',
        'losses',
        'ot',
        'streakType',
        'streakNumber',
        'streakCode',
        'gamesPlayed',
        'goalsScored',
        'goalsAgainst',
        'divisionL10Rank',
        'divisionRoadRank',
        'divisionHomeRank',
        'conferenceRank',
        'conferenceL10Rank',
        'conferenceRoadRank',
        'conferenceHomeRank',
        'leagueRank',
        'leagueL10Rank',
        'leagueRoadRank',
        'leagueHomeRank',
        'wildCardRank',
        'lastUpdated'
    ]]
    df_standings.columns = [
        'division_id',
        'division_rank',
        'team_id',
        'points',
        'wins',
        'losses',
        'ot',
        'streak_type',
        'streak_number',
        'streak_code',
        'games_played',
        'goals_scored',
        'goals_against',
        'division_l10_rank',
        'division_road_rank',
        'division_home_rank',
        'conference_rank',
        'conference_l10_rank',
        'conference_road_rank',
        'conference_home_rank',
        'league_rank',
        'league_l10_rank',
        'league_road_rank',
        'league_home_rank',
        'wild_card_rank',
        'last_updated'
    ]
    df_standings.to_sql(name='standings',con=NHL_DB,if_exists='replace',index=False,
        dtype={
            'division_rank': 'integer',
            'division_l10_rank': 'integer',
            'division_road_rank': 'integer',
            'division_home_rank': 'integer',
            'conference_rank': 'integer',
            'conference_l10_rank': 'integer',
            'conference_road_rank': 'integer',
            'conference_home_rank': 'integer',
            'league_rank': 'integer',
            'league_l10_rank': 'integer',
            'league_road_rank': 'integer',
            'league_home_rank': 'integer',
            'wild_card_rank': 'integer'
        }
    )

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
