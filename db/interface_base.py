import sqlite3
from datetime import datetime
from tools import manuwriter


class DatabaseInterfaceBase:
    '''A Helper class for database, you can implement your own interface from scratch, or use this helper class as parent class.'''
    _instance = None
    TABLE_USERS = "users"
    USER_ID = 'id'
    USER_LAST_INTERACTION = 'last_interaction'
    USER_LANGUAGE = 'language'
    
    DATE_FORMAT = '%Y-%m-%d'
    @staticmethod
    def Get():
        if not DatabaseInterfaceBase._instance:
            DatabaseInterfaceBase._instance = DatabaseInterfaceBase()
        return DatabaseInterfaceBase._instance

    def setup(self):
        '''Implement the structure of database here'''
        pass


    def add(self, user, log_category_prefix=''):
        '''Add user method'''

    def get(self, chat_id):
        rows = self.execute(True, f"SELECT * FROM {DatabaseInterfaceBase.TABLE_USERS} WHERE {DatabaseInterfaceBase.USER_ID}=? LIMIT 1", chat_id)
        return rows[0] if rows else None

    def get_users(self) -> list:
        return self.execute(True, f"SELECT * FROM {DatabaseInterfaceBase.TABLE_USERS}")

    def get_users_by_column(self, column: str=USER_ID) -> list:
        rows = self.execute(True, f"SELECT ({column}) FROM {DatabaseInterfaceBase.TABLE_USERS}")
        if column == DatabaseInterfaceBase.USER_LAST_INTERACTION:
            return [datetime.strptime(row[0], DatabaseInterfaceBase.DATE_FORMAT) if row[0] else None for row in rows]
        return [row[0] for row in rows] # just return a list of ids

    def execute(self, is_fetch_query: bool, query: str, *params):
        '''Execute queries that doesnt return result such as insert or delete'''
        rows = None
        connection = sqlite3.connect(self._name)
        cursor = connection.cursor()
        cursor.execute(query, (*params, ))
        if is_fetch_query:
            rows = cursor.fetchall()
        else:  # its a change and needs to be saved
            connection.commit()
        cursor.close()
        connection.close()
        return rows
    
    def __init__(self, name="data.db"):
        self._name = name
        self.setup()