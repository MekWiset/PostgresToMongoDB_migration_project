import pandas as pd
from datetime import datetime

class timestampManager:

    '''
    Manage timestamp files for data processing.

    Args:
        timestamp_path (str): Path to the timestamp file.

    Methods:
        read_timestamp() -> datetime:
            Read and return the timestamp from the file.

        compare_timestamp(lastexe_df: pd.DataFrame, lastexe_col: str) -> pd.DataFrame: 
            Return DataFrame with data newer than the stored timestamp.
            
        update_timestamp() -> None:
            Update the timestamp file with the current time.
    '''

    def __init__(self, timestamp_path):
        self.timestamp_path = timestamp_path

    def read_timestamp(self) -> datetime:
        '''Read and return the timestamp from the file.'''
        try:
            with open(self.timestamp_path, 'r') as file:
                timestamp_str = file.read().strip()

                if timestamp_str:
                    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                else:
                    print('Timestamp file is empty')
                    return None

        except FileNotFoundError:
            print('Timestamp file cannot be found')
            return None

    def compare_timestamp(self, lastexe_df: pd.DataFrame, lastexe_col: str) -> pd.DataFrame:
        '''Return DataFrame with data newer than the stored timestamp.'''
        timestamp = self.read_timestamp()

        if timestamp is not None:
            lastexe_df[lastexe_col] = pd.to_datetime(lastexe_df[lastexe_col])
            newly_generated_data = lastexe_df[lastexe_df[lastexe_col] > timestamp]
        else:
            newly_generated_data = lastexe_df.copy()

        return newly_generated_data

    def update_timestamp(self) -> None:
        '''Update the timestamp file with the current time.'''
        with open(self.timestamp_path, 'w') as file:
            file.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))