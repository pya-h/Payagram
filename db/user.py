from db.user_base import UserBase
from tools.mathematix import tz_today
from enum import Enum

class UserStates(Enum):
    '''Custom states of the user can be defined here'''

class User(UserBase):
    '''Extended version of the user model, varies by developer's need'''

    def __init__(self, chat_id, language: str = 'fa') -> None:
        super().__init__(chat_id, language)

    
    @staticmethod
    def Get(chat_id):
        if chat_id in User.Instances:
            User.Instances[chat_id].last_interaction = tz_today()
            return User.Instances[chat_id]
        row = User.Database().get(chat_id)
        if row:
            '''load database and create User from that and'''
            # return User(...)
        return User(chat_id=chat_id).save()

    # continue overriding methods or defining new ones if required...