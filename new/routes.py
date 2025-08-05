import os
import pandas as pd
from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app, session
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime

from app import db
from app.models import User, Role, Log, CRMDeposit, M2pDeposit
from app.forms import LoginForm, RegistrationForm
from app.processing import run_report_processing  # Keep for backwards compatibility
from app.charts import create_charts
from app.logger import record_log
from app.unified_processing import (
    process_ib_rebate_file,
    process_crm_withdrawals_file,
    process_crm_deposit_file,
    process_account_list_file,
    process_payment_file,
    generate_advanced_final_report,
    compare_crm_and_client_deposits,
    get_date_range_from_data,
    run_original_report_processing,
    read_file
)


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
        upload_folder = current_app.config['UPLOAD_FOLDER']
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)

        processed_files = []

        def handle_file_upload(file_key, process_func):
            if file_key in request.files:
                file = request.files[file_key]
                if file.filename:
                    file_path = os.path.join(upload_folder, secure_filename(file.filename))
                    file.save(file_path)
                    try:
                        result = process_func(file_path)
                        processed_files.append(f"{file_key}: {result} rows added.")
                    except Exception as e:
                        flash(f"Error processing {file_key}: {e}", "danger")
                    finally:
                        os.remove(file_path)

        # Original files (now optional)
        if 'deals_csv' in request.files and request.files['deals_csv'].filename:
            # The original logic saved these files for later processing.
            # I will maintain this for now, but it could be refactored.
            request.files['deals_csv'].save(os.path.join(upload_folder, secure_filename('deals.csv')))
            processed_files.append("deals.csv")
        if 'ex_csv' in request.files and request.files['ex_csv'].filename:
            request.files['ex_csv'].save(os.path.join(upload_folder, secure_filename('excluded.csv')))
            processed_files.append("excluded.csv")
        if 'vip_csv' in request.files and request.files['vip_csv'].filename:
            request.files['vip_csv'].save(os.path.join(upload_folder, secure_filename('vip.csv')))
            processed_files.append("vip.csv")


        # New optional files
        handle_file_upload('ib_rebate_csv', process_ib_rebate_file)
        handle_file_upload('crm_withdrawals_csv', process_crm_withdrawals_file)
        handle_file_upload('crm_deposit_csv', process_crm_deposit_file)
        handle_file_upload('account_list_csv', process_account_list_file)
        handle_file_upload('payment_csv', process_payment_file)

        if processed_files:
            flash(f'Successfully processed: {", ".join(processed_files)}', 'success')
            record_log('files_uploaded', details=f"Files: {', '.join(processed_files)}")
            session['files_uploaded'] = True # For original report compatibility
        else:
            flash('No files were selected for upload.', 'warning')

        return redirect(url_for('main.upload_file'))

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


@bp.route('/reports/discrepancies')
@login_required
def discrepancies_report():
    discrepancies_df = compare_crm_and_client_deposits()
    return render_template('discrepancies.html', title='Deposit Discrepancies', report_df=discrepancies_df)

@bp.route('/reports/discrepancies/confirm/<source>/<int:row_id>')
@login_required
def confirm_discrepancy(source, row_id):
    if source == 'CRM Deposit':
        item = CRMDeposit.query.get(row_id)
    elif source == 'M2p Deposit':
        item = M2pDeposit.query.get(row_id)
    else:
        flash('Invalid source for discrepancy.', 'danger')
        return redirect(url_for('main.discrepancies_report'))

    if item:
        db.session.delete(item)
        db.session.commit()
        flash(f'Discrepancy from {source} (ID: {row_id}) has been confirmed and removed.', 'success')
    else:
        flash('Discrepancy not found.', 'danger')

    return redirect(url_for('main.discrepancies_report'))

@bp.route('/reports/final')
@login_required
def final_report():
    report_df = generate_advanced_final_report()
    report_html = report_df.to_html(classes='table table-striped table-hover')
    return render_template('final_report.html', title='Final Report', report_html=report_html, filtered=False)

@bp.route('/reports/filtered', methods=['GET', 'POST'])
@login_required
def filtered_report():
    if request.method == 'POST':
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')

        # Basic validation
        if not start_date_str or not end_date_str:
            flash('Both start and end dates are required.', 'warning')
            return render_template('filtered_report_form.html', title='Filtered Report')

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            flash('Invalid date format. Please use YYYY-MM-DD.', 'warning')
            return render_template('filtered_report_form.html', title='Filtered Report')

        report_df = generate_advanced_final_report(start_date, end_date)
        report_html = report_df.to_html(classes='table table-striped table-hover')
        return render_template('final_report.html', title='Filtered Report', report_html=report_html, filtered=True, start_date=start_date_str, end_date=end_date_str)

    return render_template('filtered_report_form.html', title='Filtered Report')
