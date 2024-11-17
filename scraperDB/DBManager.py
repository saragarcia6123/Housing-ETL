import os
import pandas as pd
import shutil
from datetime import datetime
import logging
import re

class DBM:
    
    def __init__(self):
        self.active_session = None
        self.active_table = None
        print('Initialized new instance of DBM')

    """Connects to a database"""
    def connect(self, db: str, force: bool = False):

        if db == self.active_session:
            print(f"Already connected to database '{db}'.")
            return

        if self.active_session is not None:
            if not force:
                conf = input(f"Session '{self.active_session}' is currently active. Disconnect? (y/N): ")
            else:
                conf = 'y'

            if conf != 'y':
                print("Operation cancelled")
                return

            self.disconnect(force)
            
        path = self.get_db_path(db)

        lock_file = os.path.join(path, ".lock")
        
        if os.path.exists(lock_file):
            print(f"Failed to connnect: Database '{db}' is locked by another session.")
            return
        
        if not os.path.exists(path):
            if not force:
                conf = input(f"Database '{db}' does not exist. Would you like to create it? (y/N): ")
            else:
                conf = 'y'

            if conf != 'y':
                print("Operation cancelled")
                return

            self.create_db(db, force=force)
        
        self.active_session = db
        locked = self._lock()
        if not locked:
            print(f'Failed to connnect: Error locking file')
            self.active_session = None
            return
        
        print(f"Connected to '{db}'.")

    """Disconnects active database"""
    def disconnect(self, force: bool = False):
        
        if self.active_session is None:
            print("Error: No session is currently active.")
            self.active_table = None
            return
            
        if self.active_table is not None:
            if not force:
                conf = input(f"Table '{self.active_table}' is currently open. Your changes will not be saved. Continue? (y/N): ")
            else:
                conf = 'y'
                
            if conf != 'y':
                print('Operation cancelled')
                return
                
            self.close_table()

        db = self.active_session
        self._unlock()
        self.active_session = None
        self.active_table = None
        print(f"Disconnected from '{db}'.")

    """Locks a database"""
    def _lock(self) -> bool:

        if self.active_session is None:
            print(f"Failed to lock: No session is currently active.")
            return False
            
        path = self.get_db_path(self.active_session)
        lock_file = os.path.join(path, ".lock")
        
        if os.path.exists(lock_file):
            print(f"Failed to lock: Database {self.active_session} is locked by another session.")
            return False
            
        self.lock_file = lock_file
        with open(self.lock_file, "w") as f:
            f.write("locked")
            
        print(f"Locked {self.active_session}")
        return True

    """Unlocks a database"""
    def _unlock(self):

        if self.active_session is None:
            print(f"Failed to unlock: No session is currently active.")
            return

        path = self.get_db_path(self.active_session)
        
        lock_file = os.path.join(path, ".lock")
        if not os.path.exists(lock_file):
            print(f"Failed to unlock: Database {self.active_session} is not locked.")
            return
            
        os.remove(self.lock_file)
        self.lock_file = None
        print(f"Unlocked {self.active_session}")

    """Creates a new database"""
    def create_db(self, db_name: str):
        
        if db_name is None or db_name == "":
            print(f"Failed to create database: No database name provided.")
            return

        if not self.is_valid_filename(db_name):
            print(f"Failed to create database: Invalid database name '{db_name}'.")

        path = self.get_db_path(db_name)
        if os.path.exists(path):
            print(f"Failed to create database: Datatabase '{db_name}' already exists.")
            return

        os.makedirs(path, exist_ok=True)
        print(f"Database {db_name} initialized.")

    """Deletes a database"""
    def delete_db(self, db_name: str, force: bool = False):
        
        if self.active_session == db_name:
            print(f"Failed to delete database: Database '{db_name}' is currently active.")
            return

        path = self.get_db_path(self.active_session)
        
        if not os.path.exists(path):
            print(f"Failed to delete database: Database '{db_name}' does not exist.")
            return

        lock_file = os.path.join(path, ".lock")
        
        if os.path.exists(lock_file):
            print(f"Failed to delete database: Database {db_name} is locked by another session.")
            return False
        
        if not force:
            conf = input(f"Are you sure you want to delete '{db_name}'? (y/N): ")
        else:
            conf = 'y'
            
        if conf.lower() != 'y':
            print('Operation cancelled.')
            return
            
        try:
            os.rmdirs(path)
            print(f"Database '{db_name}' deleted.")
            
        except Exception as e:
            print(f"Failed to delete Database '{db_name}': {e}")
    
    """Creates a new table"""
    def create_table(self, table_name: str, df: pd.DataFrame = None, force: bool = False):
        
        if self.active_session is None:
            print("Error: No session is currently active.")
            return

        if not self.is_valid_filename(table_name):
            print(f"Failed to load: Invalid table name '{table_name}'.")
            return
        
        path = self.get_table_path(self.active_session, table_name)
        
        if os.path.exists(path):
            if not force:
                conf = input(f"Table '{table_name}' already exists. Would you like to overwrite? (y/N): ")
            else:
                conf = 'y'
                
            if conf.lower() != 'y':
                print('Operation cancelled.')
                return
        
        print(f"Table '{table_name}' created.")
        
        if df is not None:
            self.write_df(table_name, df)

    """Deletes a table"""
    def delete_table(self, table_name: str, force: bool = False):

        if self.active_session is None:
            print("Error: No session is currently active.")
            return
        
        if self.active_table == table_name:
            print(f"Failed to delete table: Table '{table_name}' is currently active.")
            return

        path = self.get_table_path(self.active_session, table_name)
        
        if not os.path.exists(path):
            print(f"Failed to delete table: Table '{table_name}' does not exist.")
            return
        
        if not force:
            conf = input(f"Are you sure you want to delete '{table_name}'? (y/N): ")
        else:
            conf = 'y'
            
        if conf.lower() != 'y':
            print('Operation cancelled.')
            return
            
        try:
            os.remove(path)
            print(f"Table '{table_name}' deleted.")
            
        except Exception as e:
            print(f"Failed to delete table '{table_name}': {e}")

    """Opens a table for editing"""
    def open_table(self, table_name, force: bool = False) -> pd.DataFrame:
        
        if self.active_session  is just is None:
            print("Error: No session is currently active.")
            return None

        path = self.get_current_path(self.active_session, table_name)

        if not os.path.exists(path):
            print(f"Failed to open: Table '{table_name}' does not exist.")
            self.active_table = None
            return None

        if self.active_table is not None:
            if not force:
                conf = input (f"Table '{self.active_table}' is already open. Changes will not be saved. Continue? (y/N): ")
            else:
                conf = 'y'

            if conf.lower() != 'y':
                print('Operation cancelled.')
                return
                
            self.close_table(force=True)

        try:
            df = pd.read_csv(path)

            os.chmod(path, 0o666)
            self.active_table = table_name
        except Exception as e:
            print(f"Failed to open: {e}")

    """Closes the active table"""
    def close_table(self, force: bool = False):
        
        if self.active_session is None:
            print("Error: No session is currently active.")
            self.active_table = None
            return

        if not force:
            conf = input("The active table will be closed. Any unsaved changes will be lost. Proceed? (y/N):")
        else:
            conf = 'y'

        if conf.lower() != 'y':
            print('Operation cancelled.')
            return
        
        path = self.get_table_path(self.active_session, self.active_table)

        if not os.path.exists(path):
            print(f"Failed to close: Table '{self.active_table}' does not exist.")
            self.active_table = None
            return

        os.chmod(path, 0o444)
        self.active_table = None

    """Saves a DataFrame into a table"""
    def write_df(self, table_name: str, df: pd.DataFrame, force: bool = False):

        if self.active_session is None:
            print("Error: No session is currently active.")
            return

        if df is None:
            print(f"Failed to load: No DataFrame provided.")
            return
            
        if not self.is_valid_filename(table_name):
            print(f"Failed to load: Invalid table name '{table_name}'.")
            return

        if table_name == None or table_name == '':
            if not force:
                conf = input(f"No table name was provided. The current table will be overwritten. Continue? (y/N): ")
            else:
                conf = 'y'

            if conf != 'y':
                print("Operation cancelled")
                return
        
        table_name = table_name if table_name else self.active_table
        
        path = self.get_table_path(self.active_session, table_name)
        
        if os.path.exists(path):
            if not force:
                conf = input(f"Table '{table_name}' already exists. Overwrite? (y/N)")
            else:
                conf = 'y'
                
            if conf != 'y':
                print("Operation cancelled.")
                return
        
        try:
            df.to_csv(path, index=False)
            print(f"Loaded DataFrame into table '{table_name}' at '{path}'.")
        except Exception as e:
            print(f"Failed to load DataFrame into table: {e}")

    @staticmethod
    def get_db_path(db_name: str) -> str:
        return f'scraperDB/databases/{db_name}'

    @staticmethod
    def get_table_path(db_name: str, table_name: str) -> str:
        return f'scraperDB/databases/{db_name}/{table_name}.csv'

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        pattern = r'/^[a-zA-Z0-9](?:[a-zA-Z0-9 ._-]*[a-zA-Z0-9])?\.[a-zA-Z0-9_-]+$/'
        return not re.search(pattern, filename)
