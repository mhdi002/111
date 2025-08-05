import os
import pandas as pd
from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app, session
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename

from app import db
from app.models import User, Role, Log
from app.forms import LoginForm, RegistrationForm
from app.processing import run_report_processing
from app.charts import create_charts
from app.logger import record_log


bp = Blueprint('main', __name__)

# ... (other routes remain the same) ...

@bp.route('/')
@bp.route('/index')
def index():
    return render_template('index.html', title='Home')

@bp.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', title='Dashboard')

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password', 'danger')
            return redirect(url_for('main.login'))
        login_user(user, remember=form.remember_me.data)
        record_log('user_login')
        return redirect(url_for('main.dashboard'))
    return render_template('login.html', title='Sign In', form=form)

@bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    form = RegistrationForm()
    if form.validate_on_submit():
        viewer_role = Role.query.filter_by(name='Viewer').first()
        if not viewer_role:
            flash('System error: User roles not configured.', 'danger')
            return redirect(url_for('main.register'))

        user = User(username=form.username.data, email=form.email.data, role=viewer_role)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Congratulations, you are now a registered user!', 'success')
        return redirect(url_for('main.login'))
    return render_template('register.html', title='Register', form=form)


@bp.route('/logout')
@login_required
def logout():
    record_log('user_logout')
    logout_user()
    return redirect(url_for('main.index'))

@bp.route('/upload', methods=['GET', 'POST'])
@login_required
def upload_file():
    if request.method == 'POST':
        if 'deals_csv' not in request.files or 'ex_csv' not in request.files or 'vip_csv' not in request.files:
            flash('All three files are required', 'warning')
            return redirect(request.url)

        # ... (rest of upload logic is the same)
        deals_file = request.files['deals_csv']
        ex_file = request.files['ex_csv']
        vip_file = request.files['vip_csv']

        if deals_file.filename == '' or ex_file.filename == '' or vip_file.filename == '':
            flash('One or more files were not selected', 'warning')
            return redirect(request.url)

        upload_folder = current_app.config['UPLOAD_FOLDER']
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)

        deals_file.save(os.path.join(upload_folder, secure_filename('deals.csv')))
        ex_file.save(os.path.join(upload_folder, secure_filename('excluded.csv')))
        vip_file.save(os.path.join(upload_folder, secure_filename('vip.csv')))

        record_log('files_uploaded')
        flash('Files successfully uploaded. You can now generate the report.', 'success')
        session['files_uploaded'] = True
        return redirect(url_for('main.dashboard'))

    return render_template('upload.html', title='Upload Files')

@bp.route('/report/generate')
@login_required
def generate_report():
    if not session.get('files_uploaded'):
        flash('Please upload the report files first.', 'warning')
        return redirect(url_for('main.upload_file'))

    upload_folder = current_app.config['UPLOAD_FOLDER']
    deals_path = os.path.join(upload_folder, 'deals.csv')
    excluded_path = os.path.join(upload_folder, 'excluded.csv')
    vip_path = os.path.join(upload_folder, 'vip.csv')

    try:
        deals_df = pd.read_csv(deals_path)
        excluded_df = pd.read_csv(excluded_path, header=None)
        vip_df = pd.read_csv(vip_path, header=None)

        results = run_report_processing(deals_df, excluded_df, vip_df)

        # Convert all result tables to HTML
        report_tables = {
            key: df.to_html(classes='table table-striped table-hover', index=False)
            for key, df in results.items() if isinstance(df, pd.DataFrame)
        }

        # Generate charts
        report_charts = create_charts(results)

        record_log('report_generated')

        # Render the results template directly
        return render_template('results.html', title='Report Results', tables=report_tables, charts=report_charts)

    except FileNotFoundError:
        flash('Could not find uploaded files. Please upload again.', 'danger')
        return redirect(url_for('main.upload_file'))
    except Exception as e:
        flash(f'An error occurred during report generation: {e}', 'danger')
        return redirect(url_for('main.dashboard'))

@bp.route('/admin')
@login_required
def admin():
    if not current_user.has_role('Owner'):
        flash('You do not have permission to access the admin panel.', 'danger')
        return redirect(url_for('main.dashboard'))

    logs = Log.query.order_by(Log.timestamp.desc()).all()
    return render_template('admin.html', title='Admin Panel', logs=logs)
