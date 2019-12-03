import json
import pandas as pd
import spotipy
import spotipy.util as util

lambda_df = lambda y: pd.concat([ pd.io.json.json_normalize(z,sep='_') for z in y ],sort=1)

class Playlist_Builder():
    def __init__(self,credentials):
        with open(credentials) as f:
            self.crd = json.load(f)
            self.token = util.prompt_for_user_token( **self.crd)
            self.cnx = spotipy.Spotify(auth=self.token)

        self.playlists = self.get_playlists()
        self.tracks = self.get_tracks()
        if self.validate_decades():
            self.rebuild_playlists()
            print('Success!' )
        else:
            print("Error! One or more tracks are in the wrong playlist!")

    def get_playlists(self):
        print('Accessing Spotify API for users\' decades playlists...')
        playlists = self.cnx.current_user_playlists()['items']
        df_playlists = lambda_df(playlists)
        df_playlists = df_playlists.loc[df_playlists['name'].str.match('albums_'),['id','name']]
        df_playlists.index = list(df_playlists.name.apply(lambda x: x[7:10]))
        return df_playlists

    def get_tracks(self):
        print('Accessing Spotify API for decades playlists\' tracklists...')
        df_tracks = pd.DataFrame()
        for index,row in self.playlists.iterrows():
            id, name = row
            tracklist = self.cnx.user_playlist_tracks(self.crd['username'],playlist_id=id)['items']
            df_tracklist = lambda_df(tracklist)
            df_tracklist['playlist_key'] = index
            df_tracklist['playlist_id'] = id
            df_tracklist['playlist_name'] = name
            df_tracks = df_tracks.append(df_tracklist,sort=1)

        print('Considering manual overrides of album release dates...')
        overrides = pd.read_csv('release_date_overrides.csv',index_col=0)
        df_tracks = df_tracks.join(overrides,on='track_uri')
        df_tracks['release_date'] = df_tracks.release_date.combine_first(df_tracks.track_album_release_date)
        df_tracks = df_tracks.sort_values('release_date')
        df_tracks = df_tracks[[
            'playlist_key'
            , 'playlist_id'
            , 'playlist_name'
            , 'track_uri'
            , 'track_album_name'
            , 'release_date'
        ]]
        print('Generating \'playlist_tracks.csv\' output...')
        df_tracks.to_csv('playlist_tracks.csv',index=False)
        return df_tracks

    def rebuild_playlists(self):
        print('Accessing Spotify API to rebuild users\' decades playlists...')
        df_tracks = self.tracks
        playlist_ids = df_tracks.playlist_id.unique()
        tracklist = [ list(df_tracks.loc[df_tracks['playlist_id'] == p,'track_uri']) for p in playlist_ids ]
        playlist_metadata = zip(playlist_ids,tracklist)
        for p in playlist_metadata:
            self.cnx.user_playlist_replace_tracks(self.crd['username'],p[0],p[1])

    def validate_decades(self):
        print('Validating album release dates against decades playlists...')
        df_tracks = self.tracks
        release_decade = df_tracks.release_date.apply(lambda x: x[:3])
        df_failure = df_tracks[ df_tracks.playlist_key != release_decade ]
        return df_failure.empty

def main():
    sp = Playlist_Builder('credentials.json')

if __name__ == '__main__':
    main()
