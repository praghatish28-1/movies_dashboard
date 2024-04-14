from flask import Blueprint, request, render_template, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
from app.db import mongo  # Ensure that this import aligns with how you've configured PyMongo in your app

bp = Blueprint('auth', __name__, url_prefix='/auth')

@bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = mongo.db.users.find_one({'username': username})

        if user:
            flash('Username already exists.', 'error')
            return redirect(url_for('auth.register'))

        # Password hashing
        hash_pass = generate_password_hash(password, method='sha256')

        # Insert new user into the database
        mongo.db.users.insert_one({'username': username, 'password': hash_pass})
        flash('User registered successfully!', 'success')
        return redirect(url_for('auth.login'))

    return render_template('register.html')

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = mongo.db.users.find_one({'username': username})

        if not user or not check_password_hash(user['password'], password):
            flash('Invalid username or password.', 'error')
            return redirect(url_for('auth.login'))

        session['username'] = user['username']
        flash('You have been logged in!', 'success')
        return redirect(url_for('auth.dashboard'))  # Ensure you have a 'dashboard' route defined

    return render_template('login.html')

@bp.route('/logout')
def logout():
    session.pop('username', None)
    flash('You have been logged out.', 'success')
    return redirect(url_for('auth.login'))

@bp.route('/')
def index():
    if 'username' in session:
        return render_template('dashboard.html', username=session['username'])
    return render_template('index.html')

@bp.route('/dashboard')
def dashboard():
    if 'username' not in session:
        return redirect(url_for('auth.login'))
    return render_template('dashboard.html', username=session['username'])
