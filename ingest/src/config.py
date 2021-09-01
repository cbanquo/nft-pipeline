import os
from dotenv import load_dotenv


load_dotenv()


config = {
    "account": os.environ['snow_account'],
    "user": os.environ['snow_user'],
    "password": os.environ['snow_password'],
    "database": os.environ['snow_database'], 
    "schema": os.environ['snow_schema'], 
}
