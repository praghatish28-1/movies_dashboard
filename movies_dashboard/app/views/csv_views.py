from flask import Blueprint, current_app, render_template, session, redirect, url_for
from app.handlers.csv_handlers import RouteHandler

bp = Blueprint('csv', __name__, url_prefix='/csv')
handler = RouteHandler(current_app)

@bp.route('/movies')
def movies():
    movies_list, page, total_pages, sort_by, sort_order = handler.movies()
    return render_template('movies.html', movies=movies_list, page=page, total_pages=total_pages, sort_by=sort_by, sort_order=sort_order)

@bp.route('/upload', methods=['POST'])
def upload_csv():
    message, error_code = handler.upload_csv()
    return redirect(url_for('csv.view_uploads'))

@bp.route('/uploads', methods=['GET'])
def view_uploads():
    uploads = handler.view_uploads()
    return render_template('uploads.html', uploads=uploads)


