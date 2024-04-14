import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'a-very-secret-key'
    CELERY_BROKER_URL = 'redis://localhost:6379/0'
    CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/moviesDB")
    UPLOAD_FOLDER = '/Users/praghatish.minnalraj/PycharmProjects/pythonProject3/uploads'
