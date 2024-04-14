from flask_pymongo import PyMongo
from dotenv import load_dotenv
mongo = None

class MongoDb():
    def __int__(self, app):
        self.mongo = PyMongo(app)
