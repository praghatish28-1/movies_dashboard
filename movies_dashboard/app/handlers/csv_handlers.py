from flask import render_template, request, redirect, url_for, session, jsonify
from app.movies.movies_fetcher import MoviesFetcher
from werkzeug.utils import secure_filename
import os
import datetime
from app.db import mongo
from app import celery
import csv
import time

class RouteHandler:
    def __init__(self, app):
        self.app = app

    def movies(self):
        if 'username' not in session:
            return redirect(url_for('login'))

        page = request.args.get('page', 1, type=int)
        sort_by = request.args.get('sort_by', 'date_added')
        sort_order = request.args.get('sort_order', 'asc')

        fetcher = MoviesFetcher(page=page, sort_by=sort_by, sort_order=sort_order)
        movies_list, total_pages = fetcher.fetch_movies()
        return movies_list, page, total_pages, sort_by, sort_order

    def upload_csv(self):
        try:
            cron_id = int(datetime.datetime.now().timestamp() * 1000)
            file = request.files['file']
            filename = secure_filename(file.filename)
            filepath = os.path.join(self.app.config['UPLOAD_FOLDER'], filename)
            username = session.get('username', 'Anonymous')
            if file and file.filename.endswith('.csv'):
                file.save(filepath)
                mongo.db.uploads.insert_one({
                    'cron_id': cron_id,
                    'filename': filename,
                    'filepath': filepath,
                    'status': 'Uploaded',
                    'username': username,
                    'remark' : "CSV upload started"
                })
                self.process_csv_task.delay(cron_id)
                return jsonify({'message': 'CSV upload started.'}), 202
            else:
                raise Exception('Invalid file format')
        except Exception as e:
            mongo.db.uploads.insert_one({
                'cron_id': cron_id,
                'filename': filename,
                'filepath': filepath,
                'status': 'Failed',
                'username': username,
                'remark': e.args[0]
            })
            return jsonify({'error': f'{e.args[0]}'}), 400



    def view_uploads(self):
        username = session.get('username')
        if not username:
            return redirect(url_for('auth.login'))  # Redirect to login if no user is logged in
        uploads = mongo.db.uploads.find({'username': username})
        return uploads

    @celery.task(bind=True)
    def process_csv_task(self, cron_id):
        try:
            print("came inside")
            db = mongo.db
            collection = db.movies
            upload_obj = mongo.db.uploads.find_one({'cron_id': cron_id})
            filename = upload_obj['filename']
            filepath = upload_obj['filepath']
            mongo.db.uploads.update_one({'cron_id': cron_id}, {'$set': {'status': 'In-progress'}})
            time.sleep(60)
            with open(filepath, newline='') as file:
                reader = csv.DictReader(file)
                collection.insert_many(reader)
            os.remove(filepath)
            mongo.db.uploads.update_one({'cron_id': cron_id},
                                        {'$set': {'status': 'Completed', 'remark': 'Added successfully'}})
            return {'status': 'Completed', 'filename': filename, 'cron_id': cron_id}
        except Exception as e:
            mongo.db.uploads.update_one({'cron_id': cron_id}, {'$set': {'status': 'Completed', 'remark': str(e)}})


