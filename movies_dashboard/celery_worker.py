# celery_worker.py
from app import create_app  # Ensure Celery is correctly imported

app, celery = create_app()

if __name__ == '__main__':
    celery.worker_main(['worker', '--loglevel=info'])
