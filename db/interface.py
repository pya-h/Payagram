from db.interface_base import DatabaseInterfaceBase
import sqlite3
from tools import manuwriter


class DatabaseInterface(DatabaseInterfaceBase):
    '''Implement your actual database here. This is the Actual one used in bot.'''
    _instance = None

    @staticmethod
    def Get():
        if not DatabaseInterface._instance:
            DatabaseInterface._instance = DatabaseInterface()
        return DatabaseInterface._instance

    def setup(self):
        connection = None
        try:
            connection = sqlite3.connect(self._name, detect_types=sqlite3.PARSE_DECLTYPES)
            cursor = connection.cursor()

            # check if the table users was created
            if not cursor.execute(f"SELECT name from sqlite_master WHERE name='{DatabaseInterfaceBase.TABLE_USERS}'").fetchone():
                query = f"CREATE TABLE {DatabaseInterfaceBase.TABLE_USERS} ({DatabaseInterfaceBase.USER_ID} INTEGER PRIMARY KEY," +\
                    f"{DatabaseInterfaceBase.USER_LANGUAGE} TEXT, {DatabaseInterfaceBase.USER_LAST_INTERACTION} DATE)"
                # create table user
                cursor.execute(query)
                manuwriter.log(f"{DatabaseInterfaceBase.TABLE_USERS} table created successfuly.", category_name='info')
            manuwriter.log("Database setup completed.", category_name='info')
            cursor.close()
            connection.close()
        except Exception as ex:
            if connection:
                connection.close()
            raise ex  # create custom exception for this
        
    def add(self, user, log_category_prefix=''):
        connection = None
        if not user:
            raise Exception("You must provide an user to save")
        try:
            USER_ALL_FIELDS = f'({DatabaseInterfaceBase.USER_ID}, {DatabaseInterfaceBase.USER_LAST_INTERACTION}, {DatabaseInterfaceBase.USER_LANGUAGE})'
            query = f"INSERT INTO {DatabaseInterfaceBase.TABLE_USERS} {USER_ALL_FIELDS} VALUES (?, ?, ?)"
            self.execute(False, query, user.chat_id, user.last_interaction.strftime(DatabaseInterfaceBase.DATE_FORMAT), user.language)

        except Exception as ex:
            manuwriter.log(f"Cannot save this user:{user}", ex, category_name=f'{log_category_prefix}database')
            if connection:
                connection.close()
            raise ex  # custom ex needed here too
        
    def update(self, user, log_category_prefix=''):
        connection = sqlite3.connect(self._name)
        cursor = connection.cursor()
        FIELDS_TO_SET = ','.join(
            f'{column}=?' for column in (DatabaseInterfaceBase.USER_LANGUAGE, DatabaseInterfaceBase.USER_LAST_INTERACTION)
        )
        result = cursor.execute(f'UPDATE {DatabaseInterfaceBase.TABLE_USERS} SET {FIELDS_TO_SET} WHERE {DatabaseInterfaceBase.USER_ID}=?', \
            (user.language, user.last_interaction.strftime(DatabaseInterfaceBase.DATE_FORMAT) , user.chat_id))

        if not result.rowcount: # if user with his chat id has been saved before in the database
            try:
                USER_ALL_FIELDS = f'({DatabaseInterfaceBase.USER_ID}, {DatabaseInterfaceBase.USER_LAST_INTERACTION}, {DatabaseInterfaceBase.USER_LANGUAGE})'
                cursor.execute(f"INSERT INTO {DatabaseInterfaceBase.TABLE_USERS} {USER_ALL_FIELDS} VALUES (?, ?, ?)", \
                    (user.chat_id, user.last_interaction.strftime(DatabaseInterfaceBase.DATE_FORMAT), user.language))
                manuwriter.log("New user started using this bot with chat_id=: " + user.__str__(), category_name=f'{log_category_prefix}info')
            except:
                pass
        connection.commit()
        cursor.close()
        connection.close()