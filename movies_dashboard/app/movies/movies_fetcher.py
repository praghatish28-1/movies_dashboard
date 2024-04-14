from app.db import mongo

class MoviesFetcher:
    def __init__(self, page=1, per_page=5, sort_by='date_added', sort_order='asc'):
        self.page = page
        self.per_page = per_page
        self.sort_by = sort_by
        self.sort_order = sort_order

    def fetch_movies(self):
        sort_mapping = {
            'date_added': 'date_added',
            'release_date': 'release_year',
            'duration': 'duration'
        }
        order = 1 if self.sort_order == 'asc' else -1
        movies_list = mongo.db.movies.find().sort(sort_mapping[self.sort_by], order).skip((self.page - 1) * self.per_page).limit(self.per_page)
        total = mongo.db.movies.count_documents({})
        total_pages = total // self.per_page + (1 if total % self.per_page > 0 else 0)
        return movies_list, total_pages
