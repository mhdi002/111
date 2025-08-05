import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import re
from sqlalchemy import and_, or_

# Import models
from app.models import (
    IBRebate, CRMWithdrawal, CRMDeposit, AccountList, WelcomeBonusAccount,
    M2pDeposit, SettlementDeposit, M2pWithdraw, SettlementWithdraw
)
from app import db

# ─── Helpers ────────────────────────────────────────────────────────────────

def round4(x):
    """Safely round a value to 4 decimal places."""
    try:
        return round(float(x), 4)
    except (ValueError, TypeError):
        return 0.0

def parse_custom_datetime(s: str):
    """Parse datetime in multiple formats including dd.mm.yyyy hh:mm:ss"""
    if not s or pd.isna(s):
        return pd.NaT
    
    try:
        # Try dd.mm.yyyy hh:mm:ss format first
        return pd.to_datetime(s, format="%d.%m.%Y %H:%M:%S", utc=True)
    except (ValueError, TypeError):
        try:
            # Try standard ISO format
            return pd.to_datetime(s, utc=True)
        except (ValueError, TypeError):
            return pd.NaT

def sanitize_numeric_series(sr: pd.Series) -> pd.Series:
    """Clean a pandas Series to ensure it contains only numeric values."""
    return (
        sr.astype(str)
          .str.replace(r"[^\d\.\-]", "", regex=True)
          .replace(r"^\s*$", "0", regex=True)
          .astype(float)
          .fillna(0.0)
    )

def detect_separator(line: str) -> str:
    """Detect CSV separator from a line."""
    tab_count = line.count('\t')
    comma_count = line.count(',')
    semicolon_count = line.count(';')
    
    if tab_count >= comma_count and tab_count >= semicolon_count:
        return '\t'
    if semicolon_count >= comma_count:
        return ';'
    return ','

def filter_by_date_range(df: pd.DataFrame, start_date, end_date, datetime_col="Date & Time (UTC)"):
    """Filter a DataFrame by a given date range."""
    if df.empty or datetime_col not in df.columns:
        return df

    if start_date and end_date:
        mask = pd.Series([True] * len(df))

        start_dt = parse_custom_datetime(start_date) if isinstance(start_date, str) else start_date
        end_dt = parse_custom_datetime(end_date) if isinstance(end_date, str) else end_date

        if pd.isna(start_dt) or pd.isna(end_dt):
             raise ValueError("Invalid start or end date format. Please use 'dd.mm.yyyy hh:mm:ss'")

        # This is more efficient than iterating row-by-row
        parsed_dts = df[datetime_col].apply(lambda x: parse_custom_datetime(str(x)))
        mask = (parsed_dts >= start_dt) & (parsed_dts <= end_dt)

        return df[mask].copy()
    return df

# ─── New Processing Functions for Individual Data Sources ─────────────────────

def process_ib_rebate_csv(file_path: str) -> int:
    """Process IB Rebate CSV and save to database."""
    try:
        # Read CSV with BOM handling
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read().strip()
        
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        if df.empty:
            return 0
        
        # Clean headers
        df.columns = df.columns.str.strip()
        headers_upper = [col.upper() for col in df.columns]
        
        # Find required columns
        id_idx = next((i for i, h in enumerate(headers_upper) if 'TRANSACTION ID' in h), -1)
        rebate_time_idx = next((i for i, h in enumerate(headers_upper) if 'REBATE TIME' in h), -1)
        rebate_idx = next((i for i, h in enumerate(headers_upper) if h == 'REBATE'), -1)
        
        if id_idx == -1 or rebate_time_idx == -1:
            raise ValueError("Required columns 'Transaction ID' and 'Rebate Time' not found")
        
        added_rows = 0
        
        for _, row in df.iterrows():
            transaction_id = str(row.iloc[id_idx]).strip()
            rebate_time_str = str(row.iloc[rebate_time_idx]).strip()
            rebate_amount = float(row.iloc[rebate_idx]) if rebate_idx != -1 and pd.notna(row.iloc[rebate_idx]) else 0.0
            
            if not transaction_id or transaction_id.upper() == 'NAN':
                continue
                
            # Check if already exists
            existing = IBRebate.query.filter_by(transaction_id=transaction_id).first()
            if existing:
                continue
            
            # Parse rebate time
            rebate_time = parse_custom_datetime(rebate_time_str)
            if pd.isna(rebate_time):
                continue
            
            # Create new record
            rebate_record = IBRebate(
                transaction_id=transaction_id,
                rebate_time=rebate_time.to_pydatetime(),
                rebate=rebate_amount
            )
            
            db.session.add(rebate_record)
            added_rows += 1
        
        db.session.commit()
        return added_rows
        
    except Exception as e:
        db.session.rollback()
        raise e

def process_crm_withdrawals_csv(file_path: str) -> int:
    """Process CRM Withdrawals CSV and save to database."""
    try:
        # Read file and detect separator
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            separator = detect_separator(first_line)
        
        df = pd.read_csv(file_path, sep=separator, encoding='utf-8-sig')
        if df.empty:
            return 0
        
        # Clean headers
        df.columns = df.columns.str.replace('\ufeff', '').str.strip()
        headers_upper = [col.upper() for col in df.columns]
        
        # Find required columns
        def find_col_idx(possible_names):
            return next((i for i, h in enumerate(headers_upper) if any(name.upper() in h for name in possible_names)), -1)
        
        review_time_idx = find_col_idx(['REVIEW TIME'])
        trading_account_idx = find_col_idx(['TRADING ACCOUNT'])
        amount_idx = find_col_idx(['WITHDRAWAL AMOUNT'])
        request_id_idx = find_col_idx(['REQUEST ID'])
        
        if review_time_idx == -1 or trading_account_idx == -1 or amount_idx == -1 or request_id_idx == -1:
            missing = []
            if review_time_idx == -1: missing.append("Review Time")
            if trading_account_idx == -1: missing.append("Trading Account")
            if amount_idx == -1: missing.append("Withdrawal Amount")
            if request_id_idx == -1: missing.append("Request ID")
            raise ValueError(f"Required columns not found: {', '.join(missing)}")
        
        added_rows = 0
        
        for _, row in df.iterrows():
            request_id = str(row.iloc[request_id_idx]).strip()
            review_time_str = str(row.iloc[review_time_idx]).strip()
            trading_account = str(row.iloc[trading_account_idx]).strip()
            amount_str = str(row.iloc[amount_idx]).strip().upper()
            
            if not request_id or request_id.upper() == 'NAN':
                continue
            
            # Check if already exists
            existing = CRMWithdrawal.query.filter_by(request_id=request_id).first()
            if existing:
                continue
            
            # Parse review time
            review_time = parse_custom_datetime(review_time_str)
            if pd.isna(review_time):
                continue
            
            # Convert withdrawal amount (handle USC -> USD)
            if 'USD' in amount_str:
                withdrawal_amount = float(re.sub(r'[^0-9.-]', '', amount_str))
            elif 'USC' in amount_str:
                raw_amount = float(re.sub(r'[^0-9.-]', '', amount_str))
                withdrawal_amount = raw_amount / 100  # Convert USC to USD
            else:
                withdrawal_amount = float(re.sub(r'[^0-9.-]', '', amount_str))
            
            # Create new record
            withdrawal_record = CRMWithdrawal(
                request_id=request_id,
                review_time=review_time.to_pydatetime(),
                trading_account=trading_account,
                withdrawal_amount=withdrawal_amount
            )
            
            db.session.add(withdrawal_record)
            added_rows += 1
        
        db.session.commit()
        return added_rows
        
    except Exception as e:
        db.session.rollback()
        raise e

def process_crm_deposit_csv(file_path: str) -> int:
    """Process CRM Deposit CSV and save to database."""
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        if df.empty:
            return 0
        
        # Clean headers
        df.columns = df.columns.str.strip()
        headers_upper = [col.upper() for col in df.columns]
        
        # Find required columns
        request_time_idx = next((i for i, h in enumerate(headers_upper) if 'REQUEST TIME' in h), -1)
        trading_account_idx = next((i for i, h in enumerate(headers_upper) if 'TRADING ACCOUNT' in h), -1)
        trading_amount_idx = next((i for i, h in enumerate(headers_upper) if 'TRADING AMOUNT' in h), -1)
        request_id_idx = next((i for i, h in enumerate(headers_upper) if 'REQUEST ID' in h), -1)
        payment_method_idx = next((i for i, h in enumerate(headers_upper) if 'PAYMENT METHOD' in h), -1)
        
        if any(idx == -1 for idx in [request_time_idx, trading_account_idx, trading_amount_idx]):
            raise ValueError("Required columns not found (Request Time, Trading Account, Trading Amount)")
        
        added_rows = 0
        
        for _, row in df.iterrows():
            request_id = str(row.iloc[request_id_idx]).strip() if request_id_idx != -1 else None
            request_time_str = str(row.iloc[request_time_idx]).strip()
            trading_account = str(row.iloc[trading_account_idx]).strip()
            trading_amount_str = str(row.iloc[trading_amount_idx]).strip()
            payment_method = str(row.iloc[payment_method_idx]).strip() if payment_method_idx != -1 else None
            
            if not request_id or request_id.upper() == 'NAN':
                continue
            
            # Check if already exists
            existing = CRMDeposit.query.filter_by(request_id=request_id).first()
            if existing:
                continue
            
            # Parse request time
            request_time = parse_custom_datetime(request_time_str)
            if pd.isna(request_time):
                continue
            
            # Parse trading amount (handle USC conversion)
            parts = trading_amount_str.split()
            if len(parts) >= 2:
                unit = parts[0].upper()
                amount_str = parts[1].replace(',', '').replace(/[^\d.-]/g, '')
                amount = float(amount_str) if amount_str else 0
                
                if unit == 'USC':
                    amount = amount / 100  # Convert USC to USD
            else:
                amount = float(re.sub(r'[^\d.-]', '', trading_amount_str))
            
            # Create new record
            deposit_record = CRMDeposit(
                request_id=request_id,
                request_time=request_time.to_pydatetime(),
                trading_account=trading_account,
                trading_amount=amount,
                payment_method=payment_method
            )
            
            db.session.add(deposit_record)
            added_rows += 1
        
        db.session.commit()
        return added_rows
        
    except Exception as e:
        db.session.rollback()
        raise e

def process_account_list_csv(file_path: str) -> Tuple[int, int]:
    """Process Account List CSV and save to database. Returns (accounts_added, welcome_bonus_added)."""
    try:
        # Read file content and handle MetaTrader header
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
        
        # Remove MetaTrader header if present
        if lines and 'METATRADER' in lines[0].upper():
            lines = lines[1:]
        
        content = ''.join(lines)
        df = pd.read_csv(pd.StringIO(content), sep=';')
        
        if df.empty:
            return 0, 0
        
        # Clean headers
        df.columns = df.columns.str.strip().str.upper()
        
        # Find required columns
        login_idx = next((i for i, col in enumerate(df.columns) if col == 'LOGIN'), -1)
        name_idx = next((i for i, col in enumerate(df.columns) if col == 'NAME'), -1)
        group_idx = next((i for i, col in enumerate(df.columns) if col == 'GROUP'), -1)
        
        if any(idx == -1 for idx in [login_idx, name_idx, group_idx]):
            raise ValueError("Required columns (Login, Name, Group) not found")
        
        # Clear existing data
        AccountList.query.delete()
        WelcomeBonusAccount.query.delete()
        
        accounts_added = 0
        welcome_added = 0
        
        for _, row in df.iterrows():
            login = str(row.iloc[login_idx]).strip()
            name = str(row.iloc[name_idx]).strip()
            group = str(row.iloc[group_idx]).strip()
            
            if not login:
                continue
            
            # Add to Account List
            account_record = AccountList(
                login=login,
                name=name,
                group=group
            )
            db.session.add(account_record)
            accounts_added += 1
            
            # Add to Welcome Bonus if applicable
            if group == "WELCOME\\Welcome BBOOK":
                welcome_record = WelcomeBonusAccount(login=login)
                db.session.add(welcome_record)
                welcome_added += 1
        
        db.session.commit()
        return accounts_added, welcome_added
        
    except Exception as e:
        db.session.rollback()
        raise e

def process_payment_csv(file_path: str) -> int:
    """Process Payment CSV and distribute data to appropriate tables."""
    try:
        # Read CSV with BOM handling
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read().replace('\ufeff', '').strip()
        
        df = pd.read_csv(pd.StringIO(content))
        if df.empty:
            return 0
        
        # Clean headers
        df.columns = df.columns.str.strip()
        
        # Column mapping
        column_map = {
            'confirmed': 'Confirmed',
            'txId': 'Transaction ID',
            'transactionAddress': 'Wallet address',
            'status': 'Status',
            'type': 'Type',
            'paymentGatewayName': 'Payment gateway',
            'finalAmount': 'Transaction amount',
            'finalCurrency': 'Transaction currency',
            'transactionAmount': 'Settlement amount',
            'transactionCurrencyDisplayName': 'Settlement currency',
            'processingFee': 'Processing fee',
            'price': 'Price',
            'comment': 'Comment',
            'paymentId': 'Payment ID',
            'created': 'Booked',
            'tradingAccount': 'Trading account',
            'correctCoinSent': 'correctCoinSent',
            'balanceAfterTransaction': 'Balance after',
            'txId_2': 'Transaction ID',
            'tierFee': 'Tier fee'
        }
        
        # Map CSV headers to expected headers
        csv_to_expected = {}
        for expected_key, csv_header in column_map.items():
            csv_idx = next((i for i, col in enumerate(df.columns) if col.strip() == csv_header), -1)
            if csv_idx != -1:
                csv_to_expected[expected_key] = csv_idx
        
        added_rows = 0
        
        for _, row in df.iterrows():
            # Extract key fields
            tx_id = str(row.iloc[csv_to_expected.get('txId', 0)]).strip() if 'txId' in csv_to_expected else ''
            status = str(row.iloc[csv_to_expected.get('status', 0)]).upper() if 'status' in csv_to_expected else ''
            pg_name = str(row.iloc[csv_to_expected.get('paymentGatewayName', 0)]).upper() if 'paymentGatewayName' in csv_to_expected else ''
            type_val = str(row.iloc[csv_to_expected.get('type', 0)]).upper() if 'type' in csv_to_expected else ''
            
            if not tx_id or pg_name == 'BALANCE' or status != 'DONE':
                continue
            
            # Parse common fields
            created_str = str(row.iloc[csv_to_expected.get('created', 0)]) if 'created' in csv_to_expected else ''
            created_time = parse_custom_datetime(created_str)
            if pd.isna(created_time):
                continue
            
            trading_account = str(row.iloc[csv_to_expected.get('tradingAccount', 0)]).strip() if 'tradingAccount' in csv_to_expected else ''
            final_amount = float(row.iloc[csv_to_expected.get('finalAmount', 0)]) if 'finalAmount' in csv_to_expected and pd.notna(row.iloc[csv_to_expected.get('finalAmount', 0)]) else 0.0
            tier_fee = float(row.iloc[csv_to_expected.get('tierFee', 0)]) if 'tierFee' in csv_to_expected and pd.notna(row.iloc[csv_to_expected.get('tierFee', 0)]) else 0.0
            
            # Determine target table and check for duplicates
            if type_val == 'DEPOSIT':
                if 'SETTLEMENT' in pg_name:
                    # Settlement Deposit
                    existing = SettlementDeposit.query.filter_by(tx_id=tx_id).first()
                    if not existing:
                        record = SettlementDeposit(
                            tx_id=tx_id,
                            created=created_time.to_pydatetime(),
                            trading_account=trading_account,
                            final_amount=final_amount,
                            tier_fee=tier_fee
                        )
                        db.session.add(record)
                        added_rows += 1
                else:
                    # M2p Deposit
                    existing = M2pDeposit.query.filter_by(tx_id=tx_id).first()
                    if not existing:
                        record = M2pDeposit(
                            tx_id=tx_id,
                            created=created_time.to_pydatetime(),
                            trading_account=trading_account,
                            final_amount=final_amount,
                            tier_fee=tier_fee
                        )
                        db.session.add(record)
                        added_rows += 1
            else:  # WITHDRAWAL
                if 'SETTLEMENT' in pg_name:
                    # Settlement Withdraw
                    existing = SettlementWithdraw.query.filter_by(tx_id=tx_id).first()
                    if not existing:
                        record = SettlementWithdraw(
                            tx_id=tx_id,
                            created=created_time.to_pydatetime(),
                            trading_account=trading_account,
                            final_amount=final_amount,
                            tier_fee=tier_fee
                        )
                        db.session.add(record)
                        added_rows += 1
                else:
                    # M2p Withdraw
                    existing = M2pWithdraw.query.filter_by(tx_id=tx_id).first()
                    if not existing:
                        record = M2pWithdraw(
                            tx_id=tx_id,
                            created=created_time.to_pydatetime(),
                            trading_account=trading_account,
                            final_amount=final_amount,
                            tier_fee=tier_fee
                        )
                        db.session.add(record)
                        added_rows += 1
        
        db.session.commit()
        return added_rows
        
    except Exception as e:
        db.session.rollback()
        raise e

# ─── Report Generation Functions ─────────────────────────────────────────────

def generate_final_report(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate the final report with all calculations."""
    try:
        # Helper function to sum filtered data
        def sum_filtered_data(model_class, amount_field, date_field, start_dt=None, end_dt=None):
            query = db.session.query(model_class)
            if start_dt and end_dt:
                query = query.filter(
                    getattr(model_class, date_field) >= start_dt,
                    getattr(model_class, date_field) <= end_dt
                )
            
            total = 0
            for record in query.all():
                amount = getattr(record, amount_field, 0)
                if amount:
                    total += float(amount)
            return total
        
        # Calculate individual components
        values = []
        
        # Add date range if provided
        if start_date and end_date:
            date_range = f"Filtered from {start_date.strftime('%d.%m.%Y %H:%M:%S')} to {end_date.strftime('%d.%m.%Y %H:%M:%S')}"
            values.append([date_range, ""])
            values.append(["", ""])
        
        # Total Rebate
        total_rebate = sum_filtered_data(IBRebate, 'rebate', 'rebate_time', start_date, end_date)
        values.append(['Total Rebate', total_rebate])
        
        # M2p and Settlement Deposits/Withdrawals
        m2p_deposit = sum_filtered_data(M2pDeposit, 'final_amount', 'created', start_date, end_date)
        settlement_deposit = sum_filtered_data(SettlementDeposit, 'final_amount', 'created', start_date, end_date)
        m2p_withdrawal = sum_filtered_data(M2pWithdraw, 'final_amount', 'created', start_date, end_date)
        settlement_withdrawal = sum_filtered_data(SettlementWithdraw, 'final_amount', 'created', start_date, end_date)
        
        values.extend([
            ['M2p Deposit', m2p_deposit],
            ['Settlement Deposit', settlement_deposit],
            ['M2p Withdrawal', m2p_withdrawal],
            ['Settlement Withdrawal', settlement_withdrawal]
        ])
        
        # CRM Deposit Total
        crm_deposit_total = sum_filtered_data(CRMDeposit, 'trading_amount', 'request_time', start_date, end_date)
        values.append(['CRM Deposit Total', crm_deposit_total])
        
        # Topchange Deposit Total (from CRM Deposit where payment_method = 'TOPCHANGE')
        topchange_query = db.session.query(CRMDeposit).filter(CRMDeposit.payment_method == 'TOPCHANGE')
        if start_date and end_date:
            topchange_query = topchange_query.filter(
                CRMDeposit.request_time >= start_date,
                CRMDeposit.request_time <= end_date
            )
        
        topchange_total = sum(float(record.trading_amount or 0) for record in topchange_query.all())
        values.append(['Topchange Deposit Total', topchange_total])
        
        # Tier Fees
        tier_fee_deposit = (
            sum_filtered_data(M2pDeposit, 'tier_fee', 'created', start_date, end_date) +
            sum_filtered_data(SettlementDeposit, 'tier_fee', 'created', start_date, end_date)
        )
        tier_fee_withdraw = (
            sum_filtered_data(M2pWithdraw, 'tier_fee', 'created', start_date, end_date) +
            sum_filtered_data(SettlementWithdraw, 'tier_fee', 'created', start_date, end_date)
        )
        
        values.extend([
            ['Tier Fee Deposit', tier_fee_deposit],
            ['Tier Fee Withdraw', tier_fee_withdraw]
        ])
        
        # Welcome Bonus Withdrawals
        welcome_bonus_withdrawals = calculate_welcome_bonus_withdrawals(start_date, end_date)
        values.append(['Welcome Bonus Withdrawals', welcome_bonus_withdrawals])
        
        # CRM Withdraw Total
        crm_withdraw_total = sum_filtered_data(CRMWithdrawal, 'withdrawal_amount', 'review_time', start_date, end_date)
        values.append(['CRM Withdraw Total', crm_withdraw_total])
        
        # Create DataFrame
        df = pd.DataFrame(values, columns=['Metric', 'Value'])
        return df
        
    except Exception as e:
        raise e

def calculate_welcome_bonus_withdrawals(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> float:
    """Calculate welcome bonus withdrawals by matching welcome bonus accounts with CRM withdrawals."""
    try:
        # Get welcome bonus account logins
        welcome_accounts = {str(account.login).strip() for account in WelcomeBonusAccount.query.all()}
        
        if not welcome_accounts:
            return 0.0
        
        # Query CRM withdrawals with date filtering
        query = db.session.query(CRMWithdrawal)
        if start_date and end_date:
            query = query.filter(
                CRMWithdrawal.review_time >= start_date,
                CRMWithdrawal.review_time <= end_date
            )
        
        total = 0.0
        for withdrawal in query.all():
            # Extract numeric login from trading account
            trading_account = str(withdrawal.trading_account).strip()
            login_match = re.search(r'\d+', trading_account)
            if login_match:
                login = login_match.group()
                if login in welcome_accounts:
                    total += float(withdrawal.withdrawal_amount or 0)
        
        return total
        
    except Exception as e:
        return 0.0

def compare_deposits(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> pd.DataFrame:
    """Compare CRM deposits with M2p deposits to find discrepancies."""
    try:
        # Get CRM deposits
        crm_query = db.session.query(CRMDeposit)
        if start_date and end_date:
            crm_query = crm_query.filter(
                CRMDeposit.request_time >= start_date,
                CRMDeposit.request_time <= end_date
            )
        
        # Get M2p deposits
        m2p_query = db.session.query(M2pDeposit)
        if start_date and end_date:
            m2p_query = m2p_query.filter(
                M2pDeposit.created >= start_date,
                M2pDeposit.created <= end_date
            )
        
        crm_deposits = crm_query.all()
        m2p_deposits = m2p_query.all()
        
        # Convert to normalized format for comparison
        crm_normalized = []
        for crm in crm_deposits:
            if crm.payment_method and crm.payment_method.upper() == 'TOPCHANGE':
                continue  # Skip Topchange deposits
            
            crm_normalized.append({
                'id': crm.id,
                'source': 'CRM Deposit',
                'date': crm.request_time,
                'client_id': crm.trading_account.lower() if crm.trading_account else '',
                'amount': float(crm.trading_amount or 0),
                'account': crm.trading_account,
                'name': getattr(crm, 'name', ''),  # If name field exists
            })
        
        m2p_normalized = []
        for m2p in m2p_deposits:
            m2p_normalized.append({
                'id': m2p.id,
                'source': 'M2p Deposit',
                'date': m2p.created,
                'client_id': '',  # M2p doesn't have client_id directly
                'amount': float(m2p.final_amount or 0),
                'account': m2p.trading_account.lower() if m2p.trading_account else '',
                'name': '',
            })
        
        # Find unmatched records
        matched_m2p = set()
        unmatched = []
        
        # Match CRM deposits with M2p deposits
        for crm_rec in crm_normalized:
            match_found = False
            for m2p_rec in m2p_normalized:
                if m2p_rec['id'] in matched_m2p:
                    continue
                
                # Check if dates are within 3.5 hours, account matches, and amounts are close
                time_diff = abs((crm_rec['date'] - m2p_rec['date']).total_seconds())
                if (time_diff <= 3.5 * 3600 and  # 3.5 hours
                    crm_rec['client_id'] in m2p_rec['account'] and
                    abs(crm_rec['amount'] - m2p_rec['amount']) <= 1):  # Amount tolerance
                    matched_m2p.add(m2p_rec['id'])
                    match_found = True
                    break
            
            if not match_found:
                unmatched.append([
                    crm_rec['source'],
                    crm_rec['date'].strftime('%Y-%m-%d %H:%M:%S'),
                    crm_rec['client_id'],
                    crm_rec['account'],
                    crm_rec['amount'],
                    crm_rec['name'],
                    '',  # Confirmed column
                    crm_rec['id']
                ])
        
        # Add unmatched M2p deposits
        for m2p_rec in m2p_normalized:
            if m2p_rec['id'] not in matched_m2p:
                # Try to find a match in CRM deposits
                match_found = False
                for crm_rec in crm_normalized:
                    time_diff = abs((m2p_rec['date'] - crm_rec['date']).total_seconds())
                    if (time_diff <= 3.5 * 3600 and
                        crm_rec['client_id'] in m2p_rec['account'] and
                        abs(crm_rec['amount'] - m2p_rec['amount']) <= 1):
                        match_found = True
                        break
                
                if not match_found:
                    unmatched.append([
                        m2p_rec['source'],
                        m2p_rec['date'].strftime('%Y-%m-%d %H:%M:%S'),
                        '',  # Client ID
                        m2p_rec['account'],
                        m2p_rec['amount'],
                        '',  # Name
                        '',  # Confirmed column
                        m2p_rec['id']
                    ])
        
        # Create DataFrame
        columns = ['Source', 'Date', 'Client ID', 'Trading Account', 'Amount', 'Client Name', 'Confirmed (Y/N)', 'Row ID']
        df = pd.DataFrame(unmatched, columns=columns)
        return df
        
    except Exception as e:
        raise e

def remove_confirmed_discrepancies(discrepancy_ids: List[Tuple[str, int]]) -> int:
    """Remove confirmed discrepancies from their respective tables."""
    try:
        removed_count = 0
        
        for source, row_id in discrepancy_ids:
            if source == 'CRM Deposit':
                record = CRMDeposit.query.get(row_id)
                if record:
                    db.session.delete(record)
                    removed_count += 1
            elif source == 'M2p Deposit':
                record = M2pDeposit.query.get(row_id)
                if record:
                    db.session.delete(record)
                    removed_count += 1
        
        db.session.commit()
        return removed_count
        
    except Exception as e:
        db.session.rollback()
        raise e

# ─── Original Deal Processing Functions (maintained for compatibility) ─────────

def process_and_split(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Convert USC to USD and split the DataFrame by 'Processing rule' into A/B/Multi books."""
    d = df.copy()
    # USC → USD conversion
    for col in d.select_dtypes(include="object"):
        d[col] = d[col].astype(str).str.replace(
            r"(?i)(\d[\d\.\-]*)\s*usc",
            lambda m: f"{round4(float(m.group(1)) / 100):.4f} USD",
            regex=True
        )

    if "Processing rule" not in d:
        raise ValueError("Missing 'Processing rule' column in the deals CSV.")

    books = {"A Book": [], "B Book": [], "Multi Book": []}
    for _, row in d.iterrows():
        rule = str(row["Processing rule"]).strip()
        bucket = (
            "A Book" if rule == "Pipwise"
            else "B Book" if rule == "Retail B-book"
            else "Multi Book"
        )
        books[bucket].append(row)
    return {name: pd.DataFrame(rows, columns=d.columns) for name, rows in books.items()}

def enrich_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated columns and remove duplicate deals based on the first column."""
    if df.empty:
        return df
    output, seen = [], set()
    for _, row in df.iterrows():
        deal = str(row.iloc[0]).strip()
        if deal in seen:
            continue
        seen.add(deal)
        raw = str(row.iloc[6] if len(row) > 6 else "")
        val = round4("".join(ch for ch in raw if ch.isdigit() or ch in ".-"))
        unit = "".join(ch for ch in raw if not (ch.isdigit() or ch in ".-")).strip().upper()
        dt_raw = str(row.iloc[7] if len(row) > 7 else "").strip()
        dt = parse_custom_datetime(dt_raw)
        date_str = dt.strftime("%Y-%m-%d") if not pd.isna(dt) else ""
        time_str = dt.strftime("%H:%M:%S") if not pd.isna(dt) else ""
        output.append(list(row) + [val, unit, date_str, time_str])
    headers = list(df.columns) + ["Profit Value", "Profit Unit", "Date", "Time"]
    return pd.DataFrame(output, columns=headers)

def aggregate_book(df: pd.DataFrame, excluded: set[str], book_type: str) -> pd.DataFrame:
    """Aggregate book data, applying specific exclusion logic based on book type."""
    if df.empty:
        return pd.DataFrame()

    required = ["Login", "Notional volume in USD", "Trader profit", "Swaps", "Commission", "TP broker profit", "Total broker profit"]
    for col in required:
        if col not in df:
            raise ValueError(f"Missing required column '{col}' in the deals CSV.")
        if col != "Login":
            df[col] = sanitize_numeric_series(df[col])

    rows = []
    for login, group in df.groupby("Login", dropna=False):
        if pd.isna(login):
            continue

        login_str = str(int(login)).strip() if pd.notna(login) else ""
        is_excluded = login_str in excluded

        if book_type == "B Book" and is_excluded:
            continue

        comm, tp, bk = (0, 0, 0) if is_excluded and book_type in ["A Book", "Multi Book"] else (group["Commission"].sum(), group["TP broker profit"].sum(), group["Total broker profit"].sum())

        rec = {
            "Login": login_str,
            "Total Volume": group["Notional volume in USD"].sum(),
            "Trader Profit": group["Trader profit"].sum(),
            "Swaps": group["Swaps"].sum(),
            "Commission": comm,
            "TP Profit": tp,
            "Broker Profit": bk
        }
        rec["Net"] = rec["Trader Profit"] + rec["Swaps"] - rec["Commission"]
        rows.append(rec)

    df_out = pd.DataFrame(rows)
    if not df_out.empty:
        summary = {c: round4(df_out[c].sum()) for c in df_out.columns if c != "Login"}
        summary["Login"] = "Summary"
        return pd.concat([df_out, pd.DataFrame([summary])], ignore_index=True)
    return df_out

def generate_chinese_clients(enriched_books: dict, excluded: set) -> pd.DataFrame:
    """Generate analysis for Chinese clients, excluding specified accounts."""
    chinese_prefixes = ['real\\Chines', 'BBOOK\\Chines']
    chinese_summary = {}

    for book_name, df in enriched_books.items():
        if df.empty:
            continue

        required_cols = ["Login", "Group", "Notional volume in USD", "Trader profit", "Swaps", "Commission", "TP broker profit", "Total broker profit"]
        if not all(col in df.columns for col in required_cols):
            continue

        for _, row in df.iterrows():
            login = str(int(row["Login"])).strip() if pd.notna(row["Login"]) else ""
            group = str(row["Group"]).strip()

            if not login or login in excluded or not any(group.startswith(prefix) for prefix in chinese_prefixes):
                continue

            if login not in chinese_summary:
                chinese_summary[login] = {"Total Volume": 0, "Trader Profit": 0, "Swaps": 0, "Commission": 0, "TP Profit": 0, "Broker Profit": 0}

            chinese_summary[login]["Total Volume"] += float(row["Notional volume in USD"] or 0)
            chinese_summary[login]["Trader Profit"] += float(row["Trader profit"] or 0)
            chinese_summary[login]["Swaps"] += float(row["Swaps"] or 0)
            chinese_summary[login]["Commission"] += float(row["Commission"] or 0)
            chinese_summary[login]["TP Profit"] += float(row["TP broker profit"] or 0)
            chinese_summary[login]["Broker Profit"] += float(row["Total broker profit"] or 0)

    if not chinese_summary:
        return pd.DataFrame(columns=["Login", "Total Volume", "Trader Profit", "Swaps", "Commission", "TP Profit", "Broker Profit", "Net"])

    rows = []
    for login, data in chinese_summary.items():
        net = data["Trader Profit"] + data["Swaps"] - data["Commission"]
        rows.append({"Login": login, **{k: round4(v) for k, v in data.items()}, "Net": round4(net)})

    df_chinese = pd.DataFrame(rows)

    if not df_chinese.empty:
        summary = {col: round4(df_chinese[col].sum()) for col in df_chinese.columns if col != "Login"}
        summary["Login"] = "Summary"
        df_chinese = pd.concat([df_chinese, pd.DataFrame([summary])], ignore_index=True)

    return df_chinese

def generate_client_summary(results: dict) -> pd.DataFrame:
    """Generate a consolidated client summary across all books."""
    all_clients = {}
    for book_name, df in results.items():
        if df.empty:
            continue
        client_data = df[df["Login"] != "Summary"].copy()
        for _, row in client_data.iterrows():
            login = row["Login"]
            if login not in all_clients:
                all_clients[login] = {"Total Volume": 0, "Trader Profit": 0, "Swaps": 0, "Commission": 0, "TP Profit": 0, "Broker Profit": 0, "Net": 0}
            for col in all_clients[login]:
                all_clients[login][col] += float(row.get(col, 0) or 0)

    if not all_clients:
        return pd.DataFrame()

    df_summary = pd.DataFrame([{ "Login": login, **{k: round4(v) for k, v in data.items()} } for login, data in all_clients.items()])

    if not df_summary.empty:
        summary = {col: round4(df_summary[col].sum()) for col in df_summary.columns if col != "Login"}
        summary["Login"] = "Summary"
        df_summary = pd.concat([df_summary, pd.DataFrame([summary])], ignore_index=True)

    return df_summary

def calculate_vip_volume(enriched_books: dict, vip_clients: set, excluded: set) -> float:
    """Calculate the total volume for VIP clients, excluding specified accounts."""
    total_vip_volume = 0
    for book_name, df in enriched_books.items():
        if df.empty or "Login" not in df.columns or "Notional volume in USD" not in df.columns:
            continue
        for _, row in df.iterrows():
            login = str(int(row["Login"])).strip() if pd.notna(row["Login"]) else ""
            if login and login in vip_clients and login not in excluded:
                total_vip_volume += float(row["Notional volume in USD"] or 0)
    return total_vip_volume

def generate_final_calculations(results: dict, chinese_df: pd.DataFrame, vip_volume: float, date_range: str = "") -> pd.DataFrame:
    """Generate the final summary calculations table."""
    def get_sum(book_name, column):
        if book_name not in results or results[book_name].empty: return 0
        summary_row = results[book_name][results[book_name]["Login"] == "Summary"]
        return float(summary_row[column].iloc[0] or 0) if not summary_row.empty else 0

    a_book_commission = get_sum("A Book", "Commission")
    a_book_tp = get_sum("A Book", "TP Profit")
    multi_commission = get_sum("Multi Book", "Commission")
    multi_tp = get_sum("Multi Book", "TP Profit")
    a_book_total = a_book_commission + a_book_tp + multi_commission + multi_tp

    b_book_tsm = get_sum("B Book", "Net") * -1
    multi_total_broker = get_sum("Multi Book", "Broker Profit")
    multi_tp_broker = get_sum("Multi Book", "TP Profit")
    b_book_extra = multi_total_broker - multi_tp_broker
    b_book_total = b_book_tsm + b_book_extra

    a_book_volume = get_sum("A Book", "Total Volume")
    b_book_volume = get_sum("B Book", "Total Volume")
    multi_volume = get_sum("Multi Book", "Total Volume")

    total_swaps = get_sum("A Book", "Swaps") + get_sum("Multi Book", "Swaps")

    a_book_lot = (a_book_volume + multi_volume) / 200000
    b_book_lot = b_book_volume / 200000

    chinese_volume = get_sum("Chinese Clients", "Total Volume") if not chinese_df.empty else 0
    chinese_lot = chinese_volume / 200000
    vip_lot = vip_volume / 200000
    retail_lot = a_book_lot + b_book_lot - chinese_lot - vip_lot
    total_lot = a_book_lot + b_book_lot

    calculations = []
    if date_range:
        calculations.extend([["DATE RANGE", "", date_range], ["", "", ""]])

    calculations.extend([
        ["A BOOK SUMMARY", "", ""], ["Source", "Description", "Value"],
        ["A Book Result", "Sum of TP Broker Profit + Commission", round4(a_book_tp + a_book_commission)],
        ["Multi Book Result", "Sum of TP Broker Profit + Commission", round4(multi_tp + multi_commission)],
        ["Total A Book", "Sum of above two values", round4(a_book_total)],
        ["", "", ""],
        ["B BOOK SUMMARY", "", ""], ["Source", "Description", "Value"],
        ["B Book Result", "(-1) * Sum of (Trader + Swaps - Commission)", round4(b_book_tsm)],
        ["Multi Book Result", "Total Broker Profit - TP Broker Profit", round4(b_book_extra)],
        ["Total B Book", "Sum of above two values", round4(b_book_total)],
        ["", "", ""],
        ["EXTRA SUMMARY DATA", "", ""],
        ["A Book", "Client's Spread (TP Broker Profit)", round4(a_book_tp + multi_tp)],
        ["A Book", "Client's Commission", round4(a_book_commission + multi_commission)],
        ["Total Swap", "Sum of all Swaps", round4(total_swaps)],
        ["A Book", "Volume (Lot)", round4(a_book_lot)],
        ["B Book", "Volume (Lot)", round4(b_book_lot)],
        ["Chinese Clients", "Volume (Lot)", round4(chinese_lot)],
        ["VIP Clients", "Volume (Lot)", round4(vip_lot)],
        ["Retail Clients", "Volume (Lot)", round4(retail_lot)],
        ["Total Volume", "A Book + B Book", round4(total_lot)]
    ])

    return pd.DataFrame(calculations, columns=["Source", "Description", "Value"])

def run_report_processing(deals_df: pd.DataFrame, excluded_df: pd.DataFrame, vip_df: pd.DataFrame, start_date: str = None, end_date: str = None):
    """
    Main orchestrator function to run the entire report generation process.
    """
    # 1. Load sets for excluded and vip clients
    excluded_logins = set(excluded_df.iloc[:, 0].astype(str).str.strip()) if not excluded_df.empty else set()
    vip_logins = set(vip_df.iloc[:, 0].astype(str).str.strip()) if not vip_df.empty else set()

    # 2. Process and split the main deals dataframe
    books = process_and_split(deals_df)
    enriched = {k: enrich_and_dedupe(v) for k, v in books.items()}

    # 3. Apply date filtering if enabled
    date_range_str = ""
    if start_date and end_date:
        date_range_str = f"From {start_date} to {end_date}"
        for k in enriched:
            enriched[k] = filter_by_date_range(enriched[k], start_date, end_date)

    # 4. Generate all analyses
    results = {
        book_name: aggregate_book(book_data, excluded_logins, book_name)
        for book_name, book_data in enriched.items()
    }

    chinese_clients = generate_chinese_clients(enriched, excluded_logins)
    client_summary = generate_client_summary(results)
    vip_volume = calculate_vip_volume(enriched, vip_logins, excluded_logins)
    final_calculations = generate_final_calculations(results, chinese_clients, vip_volume, date_range_str)

    return {
        "A Book Raw": enriched.get("A Book", pd.DataFrame()),
        "B Book Raw": enriched.get("B Book", pd.DataFrame()),
        "Multi Book Raw": enriched.get("Multi Book", pd.DataFrame()),
        "A Book Result": results.get("A Book", pd.DataFrame()),
        "B Book Result": results.get("B Book", pd.DataFrame()),
        "Multi Book Result": results.get("Multi Book", pd.DataFrame()),
        "Chinese Clients": chinese_clients,
        "Client Summary": client_summary,
        "Final Calculations": final_calculations,
        "VIP Volume": vip_volume
    }
