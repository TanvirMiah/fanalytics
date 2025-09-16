import requests
import pandas as pd
import sqlite3
from typing import Dict, Optional, List

class FPLDataCollection:
    def __init__(self, base_url: str = "https://fantasy.premierleague.com/api/"):
        self.base_url = base_url
        self.endpoints = {
            "main": "bootstrap-static/",
            "player_stats": "element-summary/",
            "manager_info": "entry/",
            "manager_history": "history/",
            "leagues": "leagues-classic/"
        }
        self.main_data = get_fpl_data()
        self.current_gameweek = self._get_current_gameweek()

        def _make_request(self, url: str) -> Optional[Dict]:
            """Helper method to make API requests"""
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return None
        
        def _get_current_gameweek(self) -> int:
            """Helper method to get the current gameweek"""
            if self.main_data:
                game_weeks_df = self.main_data['game_weeks']
                return game_weeks_df[game_weeks_df['is_current']].iloc[0]['id']
            return 0

        def get_fpl_data(self) -> Optional[Dict[str, pd.DataFrame]]:
            """Get main FPL data and return as DataFrames"""
            url = f"{self.base_url}{self.endpoints['main']}"
            data = self._make_request(url)

            if data:
                return{
                    'chips': pd.DataFrame(data['chips']), # gamechips
                    'months': pd.DataFrame(data['phases']),
                    'football_players': pd.DataFrame(data['elements']),
                    'football_teams': pd.DataFrame(data['teams']),
                    'game_weeks': pd.DataFrame(data['events']),  # gameweeks
                    'element_types': pd.DataFrame(data['element_types']),  # player positions
                    'element_stats': pd.DataFrame(data['element_stats'])
                }
            return None
        
    def get_manager_gameweek_data(self, manager_id: int, event_id: int) -> Optional[Dict[str, pd.DataFrame]]:
        """Get manager's gameweek data"""
        url = f"{self.base_url}{self.endpoints['manager_info']}{manager_id}/event/{event_id}/picks/"
        data = self._make_request(url)
        
        if data:
            gameweek_data = {}
            data_mapping = {
                'active_chips': 'active_chip',
                'automatic_subs': 'automatic_subs',
                'event_history': 'entry_history',
                'player_picks': 'picks'
            }

            for df_key, data_key in data_mapping.items():
                try:
                    if data_key in data and data[data_key] is not None:
                        if df_key == 'event_history':
                            gameweek_data[df_key] = pd.DataFrame([data[data_key]])
                        elif isinstance(data[data_key], (list, dict)):
                            gameweek_data[df_key] = pd.DataFrame(data[data_key] if isinstance(data[data_key], list) else [data[data_key]])
                        else:
                            gameweek_data[df_key] = pd.DataFrame([[data[data_key]]])
                    else:
                        gameweek_data[df_key] = None
                except Exception as e:
                    print(f"Error processing {df_key}: {str(e)}")
                    gameweek_data[df_key] = None
            return gameweek_data
        return None
    
    def get_league_standings(self, league_id: int) -> Optional[Dict[str, pd.DataFrame]]:
        """Get league standings"""
        url = f"{self.base_url}{self.endpoints['leagues']}{league_id}/standings/"
        data = self._make_request(url)
        
        if data:
            return {
                'league_info': pd.DataFrame([data['league']]),
                'standings': pd.DataFrame(data['standings']['results'])
            }
        return None
    
    def get_manager_history(self, manager_id: int) -> Optional[Dict[str, pd.DataFrame]]:
        """Get manager history"""
        url = f"{self.base_url}{self.endpoints['manager_history']}{manager_id}/{self.endpoints['manager_history']}"
        data = self._make_request(url)
        
        if data:
            return {
                'current_season': pd.DataFrame(data['history']),
                'past_seasons': pd.DataFrame(data['history_past']),
                'chips': pd.DataFrame(data['chips'])
            }
        return None

    def collect_player_data_from_league(self, league_id: int) -> Optional[pd.DataFrame]:
        """Collect player data from league"""
        league_data = self.get_league_standings(league_id)
        if league_data is None:
            print("No league data found.")
            return None

        league_player_data = []
        for manager in league_data['standings']['entry']:
            for week in range(1, self.current_gameweek + 1):
                manager_gameweek_data = self.get_manager_gameweek_data(manager, week)
                if manager_gameweek_data is not None:
                    event_history = manager_gameweek_data['event_history']
                    event_history['manager_id'] = manager
                    league_player_data.append(event_history)

        return pd.concat(league_player_data, ignore_index=True) if league_player_data else None

    #def save_to_database(self, dataframes: Dict[str, pd.DataFrame], db_name: str = "fpl_data.db", verbose: bool = False) -> None:
    #    """Save data to SQLite database"""
    #    try:
    #        with sqlite3.connect(f"./data/{db_name}") as conn:
    #            for table_name, df in dataframes.items():
    #                if verbose:
    #                    print(f"Processing table: {table_name}")
    #                
    #                for column in df.columns:
    #                    if df[column].dtype == 'object':
    #                        non_null_values = df[column].dropna()
    #                        if len(non_null_values) > 0:
    #                            first_value = non_null_values.iloc[0]
    #                            if isinstance(first_value, (dict, list)):
    #                                if verbose:
    #                                    print(f"Converting column {column} from {type(first_value)} to string")
    #                                df[column] = df[column].apply(lambda x: str(x) if x is not None else None)
    #                
    #                df.to_sql(table_name, conn, if_exists='replace', index=False)
    #                if verbose:
    #                    print(f"Successfully saved table: {table_name}")
    #    except Exception as e:
    #        print(f"Error saving to database: {str(e)}")
    #        raise

    def check_for_updates(self) -> Dict[str, bool]:
        """
        Check if there are any updates in the FPL data by comparing with local database
        Returns a dictionary indicating which tables need updates
        """
        updates_needed = {}
        
        try:
            with sqlite3.connect(f"./data/fpl_data.db") as conn:
                # Get latest API data
                api_data = self.get_fpl_data()
                if not api_data:
                    return {}

                for table_name, new_df in api_data.items():
                    # Check if table exists in database
                    table_exists = pd.read_sql_query(
                        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'",
                        conn
                    ).shape[0] > 0

                    if not table_exists:
                        updates_needed[table_name] = True
                        continue

                    # Get existing data
                    existing_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                    
                    # Compare row counts and last modified timestamps
                    if table_name == 'game_weeks':
                        # For game_weeks, check if any gameweek data has been updated
                        updates_needed[table_name] = not new_df.equals(existing_df)
                    elif table_name == 'football_players':
                        # For players, check if stats have been updated
                        updates_needed[table_name] = not new_df['total_points'].equals(existing_df['total_points'])
                    else:
                        # For other tables, simple row count comparison
                        updates_needed[table_name] = new_df.shape[0] != existing_df.shape[0]

            return updates_needed

        except Exception as e:
            print(f"Error checking for updates: {str(e)}")
            return {}

    def sync_database(self, specific_tables: Optional[List[str]] = None) -> None:
        """
        Sync the database with latest FPL data
        If specific_tables is provided, only sync those tables
        """
        updates = self.check_for_updates()
        
        if not updates:
            print("No updates available or error checking for updates")
            return

        tables_to_update = specific_tables if specific_tables else updates.keys()
        tables_needing_update = [table for table in tables_to_update if updates.get(table, False)]

        if not tables_needing_update:
            print("All specified tables are up to date")
            return

        print(f"Updating tables: {', '.join(tables_needing_update)}")
        
        # Get fresh data
        new_data = self.get_fpl_data()
        if not new_data:
            print("Failed to fetch new data")
            return

        # Create dictionary with only the tables that need updating
        update_dict = {table: new_data[table] for table in tables_needing_update}
        
        # Save updates to database
        #self.save_to_database(update_dict, verbose=True)
        #print("Database sync completed")

    def get_last_updated_gameweek(self) -> int:
        """Get the last updated gameweek from the database"""
        try:
            with sqlite3.connect(f"./data/fpl_data.db") as conn:
                df = pd.read_sql_query(
                    "SELECT MAX(id) as last_gw FROM game_weeks WHERE finished = 1",
                    conn
                )
                return df['last_gw'].iloc[0] or 0
        except Exception as e:
            print(f"Error getting last updated gameweek: {str(e)}")
            return 0