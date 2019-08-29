import configparser
import requests
import base64
from pathlib import Path

from google_cloud_sql.database_mysql import Mysql

class Shopify:

    def __init__(self, user):

        def generate_token():
            #base64 token generation
            api_key, password = self.config[self.user]['api_key'], self.config[user]['password']
            return base64.b64encode(f'{api_key}:{password}'.encode()).decode()

        self.base_path = Path(__file__).parent
        self.user = user
        self.config = configparser.ConfigParser()
        self.config.read(str(self.base_path / 'shopify.ini'))
        self.url = self.config[user]['url']
        self.headers = {
            'Authorization': f'Basic {generate_token()}'
        }

        #self.db = Mysql(self.user)


    def send_request(self, params=''):
        response = requests.get(url=self.url+self.api_call,
                                headers=self.headers, params=params)
        return response


    def mysql_delete_by_in(self, field, param):
        query = f"""
            delete from {self.database}.{self.table}
            where {field} in {param}
        """

        with Mysql(self.user) as db_con:
            cursor = db_con.cursor()
            cursor.execute(query)
            db_con.commit()
            print(f'{self.__class__.__name__}: {cursor.rowcount} rows have been deleted')

        return True


    def mysql_add(self, data_to_add):

        def get_columns():
            query = f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = "{self.table}"
                AND table_schema = "{self.database}"
            """

            with Mysql(self.user) as db_con:
                cursor = db_con.cursor()
                cursor.execute(query)
                response = cursor.fetchall()

            return ', '.join(x[0] for x in response)

        data_to_add = str([tuple(line) for line in data_to_add]).strip('[]')
        columns = get_columns()

        query = f"""
            INSERT IGNORE INTO {self.database}.{self.table} ({columns})
            values {data_to_add}
        """

        with Mysql(self.user) as db_con:
            cursor = db_con.cursor()
            cursor.execute(query)
            db_con.commit()
            print(f'{self.__class__.__name__}: updated succesfully. {cursor.rowcount} rows have been added to database')

        return True
