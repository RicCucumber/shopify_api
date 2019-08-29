import mysql.connector
import configparser
from pathlib import Path


class Mysql():

    def __init__(self, account):
        self.base_path = Path(__file__).parent
        self.account = account
        self.config = configparser.ConfigParser()
        self.config.read(str(self.base_path / 'mysql.ini'))


    def __enter__(self):
        self.db = mysql.connector.connect(
            user = self.config[self.account]['user'],
            password = self.config[self.account]['password'],
            host = self.config[self.account]['host'])
        return self.db

    def __exit__(self, *exc_info):
        self.db.close()
