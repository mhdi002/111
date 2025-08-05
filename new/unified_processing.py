"""
Unified Processing Module - Combines original and new processing capabilities
Supports both CSV and XLSX formats, implements all stage2 features
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from app import db
from app.models import (
    IBRebate, CRMWithdrawal, CRMDeposit, AccountList, WelcomeBonusAccount,
    M2pDeposit, SettlementDeposit, M2pWithdraw, SettlementWithdraw
)
import os

# ═══════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def round4(x):
    """Safely round a value to 4 decimal places."""
    try:
        return round(float(x), 4)
    except (ValueError, TypeError):
        return 0.0

def parse_custom_datetime(s: str):
    """Parse datetime in multiple formats including dd.mm.yyyy hh:mm:ss"""
    if pd.isna(s) or s == '':
        return pd.NaT
    
    try:
        # Try various formats
        formats = [
            "%d.%m.%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S", 
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y"
        ]
        
        s_str = str(s).strip()
        for fmt in formats:
            try:
                return pd.to_datetime(s_str, format=fmt, utc=True)
            except ValueError:
                continue
        
        # Fallback to pandas automatic parsing
        return pd.to_datetime(s_str, utc=True)
    except:
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

def detect_separator(file_path):
    """Detect the separator used in a CSV file"""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            if first_line.count('\t') > first_line.count(','):
                return '\t'
            elif first_line.count(';') > first_line.count(','):
                return ';'
            else:
                return ','
    except:
        return ','

def read_file(file_path):
    """Read CSV or XLSX file with automatic format detection"""
    if not file_path or not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        # Check file extension
        _, ext = os.path.splitext(file_path.lower())
        
        if ext in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
        elif ext == '.csv':
            # Try to detect separator
            sep = detect_separator(file_path)
            # Try UTF-8 with BOM first, then fallback
            try:
                return pd.read_csv(file_path, sep=sep, encoding='utf-8-sig')
            except:
                return pd.read_csv(file_path, sep=sep, encoding='latin-1')
        else:
            # Assume CSV for unknown extensions
            sep = detect_separator(file_path)
            return pd.read_csv(file_path, sep=sep, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return pd.DataFrame()

def filter_by_date_range(df: pd.DataFrame, start_date, end_date, datetime_col="Date & Time (UTC)"):
    """Filter a DataFrame by a given date range with flexible date parsing"""
    if df.empty or datetime_col not in df.columns:
        return df

    if start_date and end_date:
        try:
            start_dt = parse_custom_datetime(start_date) if isinstance(start_date, str) else start_date
            end_dt = parse_custom_datetime(end_date) if isinstance(end_date, str) else end_date

            if pd.isna(start_dt) or pd.isna(end_dt):
                raise ValueError("Invalid start or end date format")

            # Parse datetime column with custom parser
            parsed_dts = df[datetime_col].apply(lambda x: parse_custom_datetime(str(x)))
            mask = (parsed_dts >= start_dt) & (parsed_dts <= end_dt)

            return df[mask].copy()
        except Exception as e:
            print(f"Error in date filtering: {e}")
            return df
    return df

def filter_unique_rows(existing_keys, new_rows, key_columns):
    """Filter unique rows based on specified key columns"""
    unique_rows = []
    
    for _, row in new_rows.iterrows():
        # Create key based on specified columns
        key_parts = []
        for col_idx in key_columns:
            if col_idx < len(row):
                val = str(row.iloc[col_idx] if pd.notna(row.iloc[col_idx]) else '').strip().upper()
                key_parts.append(val)
        
        key = '|'.join(key_parts)
        
        if key and key not in existing_keys:
            existing_keys.add(key)
            unique_rows.append(row)
    
    return pd.DataFrame(unique_rows)

# ═══════════════════════════════════════════════════════════════════
# ORIGINAL PROCESSING FUNCTIONS (Stage 1)
# ═══════════════════════════════════════════════════════════════════

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

def run_original_report_processing(deals_df: pd.DataFrame, excluded_df: pd.DataFrame, vip_df: pd.DataFrame, start_date: str = None, end_date: str = None):
    """
    Main orchestrator function to run the original report generation process.
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

# ═══════════════════════════════════════════════════════════════════
# NEW PROCESSING FUNCTIONS (Stage 2) - Database Operations
# ═══════════════════════════════════════════════════════════════════

def process_ib_rebate_file(file_path):
    """Process IB Rebate CSV/XLSX file and save to database"""
    try:
        df = read_file(file_path)
        if df.empty:
            return 0

        # Normalize column names
        df.columns = [col.strip().upper() for col in df.columns]

        if 'TRANSACTION ID' not in df.columns or 'REBATE TIME' not in df.columns:
            raise ValueError("Required columns 'Transaction ID' or 'Rebate Time' not found.")

        # Get existing transaction IDs from the database
        existing_ids = {item.transaction_id for item in IBRebate.query.all()}

        new_rebates = []
        for _, row in df.iterrows():
            tx_id = str(row['TRANSACTION ID']).strip()
            if tx_id and tx_id not in existing_ids:
                rebate_time_str = str(row['REBATE TIME']).strip()
                rebate_time = parse_custom_datetime(rebate_time_str)
                
                if pd.isna(rebate_time):
                    print(f"Skipping row with invalid date: {rebate_time_str}")
                    continue

                # Get additional columns if they exist
                rebate_amount = 0
                if 'REBATE' in df.columns:
                    try:
                        rebate_amount = float(row['REBATE'])
                    except:
                        rebate_amount = 0

                new_rebate = IBRebate(
                    transaction_id=tx_id,
                    rebate_time=rebate_time.to_pydatetime() if hasattr(rebate_time, 'to_pydatetime') else rebate_time
                )
                new_rebates.append(new_rebate)
                existing_ids.add(tx_id)

        if new_rebates:
            db.session.bulk_save_objects(new_rebates)
            db.session.commit()

        return len(new_rebates)

    except Exception as e:
        db.session.rollback()
        print(f"Error processing IB Rebate file: {e}")
        return 0

def process_crm_withdrawals_file(file_path):
    """Process CRM Withdrawals CSV/XLSX file and save to database"""
    try:
        df = read_file(file_path)
        if df.empty:
            return 0

        df.columns = [col.strip().upper().replace('"', '') for col in df.columns]

        required_cols = ['REVIEW TIME', 'TRADING ACCOUNT', 'WITHDRAWAL AMOUNT', 'REQUEST ID']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Missing one of the required columns: {required_cols}")

        existing_ids = {item.request_id for item in CRMWithdrawal.query.all()}

        new_withdrawals = []
        for _, row in df.iterrows():
            req_id = str(row['REQUEST ID']).strip()
            if req_id and req_id not in existing_ids:

                # Handle different currency formats
                amount_str = str(row['WITHDRAWAL AMOUNT']).upper()
                if 'USC' in amount_str:
                    # Extract number from USC format
                    amount_raw = ''.join(filter(lambda x: x.isdigit() or x in '.-', amount_str))
                    amount = float(amount_raw) / 100 if amount_raw else 0
                elif 'USD' in amount_str:
                    amount_raw = ''.join(filter(lambda x: x.isdigit() or x in '.-', amount_str))
                    amount = float(amount_raw) if amount_raw else 0
                else:
                    amount_raw = ''.join(filter(lambda x: x.isdigit() or x in '.-', amount_str))
                    amount = float(amount_raw) if amount_raw else 0

                review_time = parse_custom_datetime(str(row['REVIEW TIME']))
                if pd.isna(review_time):
                    continue

                new_withdrawal = CRMWithdrawal(
                    request_id=req_id,
                    review_time=review_time.to_pydatetime() if hasattr(review_time, 'to_pydatetime') else review_time,
                    trading_account=str(row['TRADING ACCOUNT']).strip(),
                    withdrawal_amount=amount
                )
                new_withdrawals.append(new_withdrawal)
                existing_ids.add(req_id)

        if new_withdrawals:
            db.session.bulk_save_objects(new_withdrawals)
            db.session.commit()

        return len(new_withdrawals)

    except Exception as e:
        db.session.rollback()
        print(f"Error processing CRM Withdrawals file: {e}")
        return 0

def process_crm_deposit_file(file_path):
    """Process CRM Deposit CSV/XLSX file and save to database"""
    try:
        df = read_file(file_path)
        if df.empty:
            return 0

        df.columns = [col.strip().upper() for col in df.columns]

        required_cols = ['REQUEST TIME', 'TRADING ACCOUNT', 'TRADING AMOUNT', 'REQUEST ID']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Missing one of the required columns: {required_cols}")

        existing_ids = {item.request_id for item in CRMDeposit.query.all()}

        new_deposits = []
        for _, row in df.iterrows():
            req_id = str(row['REQUEST ID']).strip()
            if req_id and req_id not in existing_ids:

                # Handle different currency formats
                amount_str = str(row['TRADING AMOUNT']).upper()
                parts = amount_str.split()
                
                if 'USC' in amount_str:
                    # Handle "USC 12345" format
                    amount_raw = ''.join(filter(lambda x: x.isdigit() or x in '.-', parts[-1]))
                    amount = float(amount_raw) / 100 if amount_raw else 0
                elif 'USD' in amount_str:
                    amount_raw = ''.join(filter(lambda x: x.isdigit() or x in '.-', parts[-1]))
                    amount = float(amount_raw) if amount_raw else 0
                else:
                    amount_raw = ''.join(filter(lambda x: x.isdigit() or x in '.-', parts[-1]))
                    amount = float(amount_raw) if amount_raw else 0

                request_time = parse_custom_datetime(str(row['REQUEST TIME']))
                if pd.isna(request_time):
                    continue

                # Get payment method if available
                payment_method = ''
                if 'PAYMENT METHOD' in df.columns:
                    payment_method = str(row['PAYMENT METHOD']).strip()

                new_deposit = CRMDeposit(
                    request_id=req_id,
                    request_time=request_time.to_pydatetime() if hasattr(request_time, 'to_pydatetime') else request_time,
                    trading_account=str(row['TRADING ACCOUNT']).strip(),
                    trading_amount=amount,
                    payment_method=payment_method
                )
                new_deposits.append(new_deposit)
                existing_ids.add(req_id)

        if new_deposits:
            db.session.bulk_save_objects(new_deposits)
            db.session.commit()

        return len(new_deposits)

    except Exception as e:
        db.session.rollback()
        print(f"Error processing CRM Deposit file: {e}")
        return 0

def process_account_list_file(file_path):
    """Process Account List CSV/XLSX file and save to database"""
    try:
        df = read_file(file_path)
        if df.empty:
            return 0, 0

        # This function replaces all existing data
        db.session.query(AccountList).delete()
        db.session.query(WelcomeBonusAccount).delete()

        df.columns = [col.strip().upper() for col in df.columns]

        # Skip initial meta rows if they exist
        if len(df) > 0 and "METATRADER" in str(df.iloc[0]).upper():
            df = df.iloc[1:]

        required_cols = ['LOGIN', 'NAME', 'GROUP']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Missing one of the required columns: {required_cols}")

        new_accounts = []
        new_welcome_accounts = []
        for _, row in df.iterrows():
            login = str(row['LOGIN']).strip()
            if login:
                new_accounts.append(AccountList(
                    login=login,
                    name=str(row['NAME']).strip(),
                    group=str(row['GROUP']).strip()
                ))
                if "WELCOME\\WELCOME BBOOK" in str(row['GROUP']).upper():
                    new_welcome_accounts.append(WelcomeBonusAccount(login=login))

        if new_accounts:
            db.session.bulk_save_objects(new_accounts)
        if new_welcome_accounts:
            db.session.bulk_save_objects(new_welcome_accounts)

        db.session.commit()

        return len(new_accounts), len(new_welcome_accounts)

    except Exception as e:
        db.session.rollback()
        print(f"Error processing Account List file: {e}")
        return 0, 0

def process_payment_file(file_path):
    """Process Payment CSV/XLSX file and save to database with enhanced logic from stage2"""
    try:
        df = read_file(file_path)
        if df.empty:
            return 0

        # Clean column names and handle BOM
        df.columns = [col.strip().replace('\ufeff', '').upper() for col in df.columns]

        # Column mapping as in stage2 code
        column_map = {
            'CONFIRMED': 'Confirmed',
            'TXID': 'Transaction ID', 
            'TRANSACTIONADDRESS': 'Wallet address',
            'STATUS': 'Status',
            'TYPE': 'Type',
            'PAYMENTGATEWAYNAME': 'Payment gateway',
            'FINALAMOUNT': 'Transaction amount',
            'FINALCURRENCY': 'Transaction currency',
            'TRANSACTIONAMOUNT': 'Settlement amount',
            'TRANSACTIONCURRENCYDISPLAYNAME': 'Settlement currency',
            'PROCESSINGFEE': 'Processing fee',
            'PRICE': 'Price',
            'COMMENT': 'Comment',
            'PAYMENTID': 'Payment ID',
            'CREATED': 'Booked',
            'TRADINGACCOUNT': 'Trading account',
            'CORRECTCOINSENT': 'correctCoinSent',
            'BALANCEAFTERTRANSACTION': 'Balance after',
            'TXID_2': 'Transaction ID',
            'TIERFEE': 'Tier fee'
        }

        # Get existing transaction IDs from all four tables
        existing_ids = set()
        for model in [M2pDeposit, SettlementDeposit, M2pWithdraw, SettlementWithdraw]:
            existing_ids.update({item.tx_id for item in model.query.all()})

        new_rows = {
            "m2p_deposit": [], "settlement_deposit": [],
            "m2p_withdraw": [], "settlement_withdraw": []
        }

        for _, row in df.iterrows():
            # Check for transaction ID in multiple possible columns
            tx_id = ''
            for col in ['TXID', 'TRANSACTION ID']:
                if col in df.columns and pd.notna(row.get(col)):
                    tx_id = str(row[col]).strip()
                    break

            if tx_id and tx_id not in existing_ids:
                # Enhanced status and payment gateway filtering
                status = str(row.get('STATUS', '')).upper()
                pg_name = str(row.get('PAYMENTGATEWAYNAME', '')).upper()
                type_str = str(row.get('TYPE', '')).upper()

                # Skip if not DONE status or BALANCE payment gateway
                if status != 'DONE' or pg_name == 'BALANCE':
                    continue

                # Determine target model based on type and payment gateway
                target_model = None
                target_list = None
                
                if type_str == 'DEPOSIT':
                    if 'SETTLEMENT' in pg_name:
                        target_model = SettlementDeposit
                        target_list = new_rows["settlement_deposit"]
                    else:
                        target_model = M2pDeposit
                        target_list = new_rows["m2p_deposit"]
                elif type_str == 'WITHDRAW':
                    if 'SETTLEMENT' in pg_name:
                        target_model = SettlementWithdraw
                        target_list = new_rows["settlement_withdraw"]
                    else:
                        target_model = M2pWithdraw
                        target_list = new_rows["m2p_withdraw"]

                if target_model and target_list is not None:
                    # Parse dates with custom parser
                    created_dt = parse_custom_datetime(str(row.get('CREATED', '')))
                    if pd.isna(created_dt):
                        continue

                    # Parse amounts
                    final_amount = 0
                    tier_fee = 0
                    try:
                        final_amount = float(row.get('FINALAMOUNT', 0))
                    except:
                        pass
                    
                    try:
                        tier_fee = float(row.get('TIERFEE', 0)) if pd.notna(row.get('TIERFEE')) else 0
                    except:
                        tier_fee = 0

                    new_record = target_model(
                        tx_id=tx_id,
                        created=created_dt.to_pydatetime() if hasattr(created_dt, 'to_pydatetime') else created_dt,
                        trading_account=str(row.get('TRADINGACCOUNT', '')).strip(),
                        final_amount=final_amount,
                        tier_fee=tier_fee
                    )
                    target_list.append(new_record)
                    existing_ids.add(tx_id)

        total_added = 0
        for model_list in new_rows.values():
            if model_list:
                db.session.bulk_save_objects(model_list)
                total_added += len(model_list)

        db.session.commit()

        return total_added

    except Exception as e:
        db.session.rollback()
        print(f"Error processing Payment file: {e}")
        return 0

# ═══════════════════════════════════════════════════════════════════
# ADVANCED REPORTING FUNCTIONS (Stage 2 Features)
# ═══════════════════════════════════════════════════════════════════

def sum_column_data(model_class, column_name, start_date=None, end_date=None, date_column=None):
    """Generic function to sum column data with optional date filtering"""
    try:
        query = db.session.query(db.func.sum(getattr(model_class, column_name)))
        
        if start_date and end_date and date_column:
            date_attr = getattr(model_class, date_column)
            query = query.filter(date_attr.between(start_date, end_date))
        
        result = query.scalar()
        return float(result) if result else 0.0
    except Exception as e:
        print(f"Error in sum_column_data: {e}")
        return 0.0

def calculate_welcome_bonus_withdrawals(start_date=None, end_date=None):
    """Calculate welcome bonus withdrawals with date filtering"""
    try:
        # Get welcome bonus account logins
        welcome_accounts = {acc.login for acc in WelcomeBonusAccount.query.all()}
        
        if not welcome_accounts:
            return 0.0

        # Query CRM withdrawals for welcome accounts
        query = db.session.query(db.func.sum(CRMWithdrawal.withdrawal_amount)).filter(
            CRMWithdrawal.trading_account.in_(welcome_accounts)
        )
        
        if start_date and end_date:
            query = query.filter(CRMWithdrawal.review_time.between(start_date, end_date))
        
        result = query.scalar()
        return float(result) if result else 0.0
    except Exception as e:
        print(f"Error calculating welcome bonus withdrawals: {e}")
        return 0.0

def calculate_topchange_total(start_date=None, end_date=None):
    """Calculate TopChange deposit total with date filtering"""
    try:
        query = db.session.query(db.func.sum(CRMDeposit.trading_amount)).filter(
            CRMDeposit.payment_method.ilike('%topchange%')
        )
        
        if start_date and end_date:
            query = query.filter(CRMDeposit.request_time.between(start_date, end_date))
        
        result = query.scalar()
        return float(result) if result else 0.0
    except Exception as e:
        print(f"Error calculating TopChange total: {e}")
        return 0.0

def generate_advanced_final_report(start_date=None, end_date=None):
    """
    Generate comprehensive final report with all data sources and optional date filtering
    Implements stage2 functionality
    """
    try:
        # Convert string dates to datetime objects if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Calculate all report values
        values = []

        # 1. Total Rebate (count of rebate records)
        rebate_query = IBRebate.query
        if start_date and end_date:
            rebate_query = rebate_query.filter(IBRebate.rebate_time.between(start_date, end_date))
        total_rebate = rebate_query.count()
        values.append(["Total Rebate", total_rebate])

        # 2. Payment Data - M2p Deposit
        m2p_deposit = sum_column_data(M2pDeposit, 'final_amount', start_date, end_date, 'created')
        values.append(["M2p Deposit", m2p_deposit])

        # 3. Payment Data - Settlement Deposit  
        settlement_deposit = sum_column_data(SettlementDeposit, 'final_amount', start_date, end_date, 'created')
        values.append(["Settlement Deposit", settlement_deposit])

        # 4. Payment Data - M2p Withdrawal
        m2p_withdrawal = sum_column_data(M2pWithdraw, 'final_amount', start_date, end_date, 'created')
        values.append(["M2p Withdrawal", m2p_withdrawal])

        # 5. Payment Data - Settlement Withdrawal
        settlement_withdrawal = sum_column_data(SettlementWithdraw, 'final_amount', start_date, end_date, 'created')
        values.append(["Settlement Withdrawal", settlement_withdrawal])

        # 6. CRM Deposit Total
        crm_deposit_total = sum_column_data(CRMDeposit, 'trading_amount', start_date, end_date, 'request_time')
        values.append(["CRM Deposit Total", crm_deposit_total])

        # 7. Tier Fee Deposit (M2p + Settlement)
        tier_fee_deposit1 = sum_column_data(M2pDeposit, 'tier_fee', start_date, end_date, 'created')
        tier_fee_deposit2 = sum_column_data(SettlementDeposit, 'tier_fee', start_date, end_date, 'created')
        tier_fee_deposit = tier_fee_deposit1 + tier_fee_deposit2
        values.append(["Tier Fee Deposit", tier_fee_deposit])

        # 8. Tier Fee Withdraw (M2p + Settlement)
        tier_fee_withdraw1 = sum_column_data(M2pWithdraw, 'tier_fee', start_date, end_date, 'created')
        tier_fee_withdraw2 = sum_column_data(SettlementWithdraw, 'tier_fee', start_date, end_date, 'created')
        tier_fee_withdraw = tier_fee_withdraw1 + tier_fee_withdraw2
        values.append(["Tier Fee Withdraw", tier_fee_withdraw])

        # 9. Welcome Bonus Withdrawals
        welcome_bonus_withdrawals = calculate_welcome_bonus_withdrawals(start_date, end_date)
        values.append(["Welcome Bonus Withdrawals", welcome_bonus_withdrawals])

        # 10. CRM TopChange Total
        crm_topchange_total = calculate_topchange_total(start_date, end_date)
        values.append(["CRM TopChange Total", crm_topchange_total])

        # 11. CRM Withdraw Total
        crm_withdraw_total = sum_column_data(CRMWithdrawal, 'withdrawal_amount', start_date, end_date, 'review_time')
        values.append(["CRM Withdraw Total", crm_withdraw_total])

        # Create DataFrame
        report_df = pd.DataFrame(values, columns=["Metric", "Value"])
        
        # Add date range info if provided
        if start_date and end_date:
            date_range = f"From {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            report_df = pd.concat([
                pd.DataFrame([["Date Range", date_range]], columns=["Metric", "Value"]),
                pd.DataFrame([["", ""]], columns=["Metric", "Value"]),
                report_df
            ], ignore_index=True)

        return report_df

    except Exception as e:
        print(f"Error generating advanced final report: {e}")
        return pd.DataFrame([["Error", str(e)]], columns=["Metric", "Value"])

def compare_crm_and_client_deposits(start_date=None, end_date=None):
    """
    Compare CRM and M2P deposits to find discrepancies
    Implements the comparison logic from stage2
    """
    try:
        # Get CRM deposits
        crm_query = CRMDeposit.query
        if start_date and end_date:
            crm_query = crm_query.filter(CRMDeposit.request_time.between(start_date, end_date))
        crm_deposits = crm_query.all()

        # Get M2P deposits  
        m2p_query = M2pDeposit.query
        if start_date and end_date:
            m2p_query = m2p_query.filter(M2pDeposit.created.between(start_date, end_date))
        m2p_deposits = m2p_query.all()

        unmatched = []
        matched_m2p = set()

        # Match CRM deposits with M2P deposits
        for crm_row in crm_deposits:
            match_found = False
            for m2p_row in m2p_deposits:
                if m2p_row in matched_m2p:
                    continue
                    
                # Check time difference (within 3.5 hours)
                time_diff = abs((crm_row.request_time - m2p_row.created).total_seconds())
                
                # Check if trading accounts match and amounts are close
                if (time_diff <= 3.5 * 3600 and
                    crm_row.trading_account in m2p_row.trading_account and
                    abs(crm_row.trading_amount - m2p_row.final_amount) <= 1):
                    
                    match_found = True
                    matched_m2p.add(m2p_row)
                    break

            # If no match found and not TopChange, add to unmatched
            if not match_found and (not crm_row.payment_method or crm_row.payment_method.lower() != 'topchange'):
                unmatched.append({
                    "Source": "CRM Deposit",
                    "Date": crm_row.request_time.strftime('%Y-%m-%d'),
                    "Client ID": crm_row.trading_account,
                    "Trading Account": "",
                    "Amount": crm_row.trading_amount,
                    "Client Name": "",
                    "Confirmed": "",
                    "Row Index": crm_row.id
                })

        # Add unmatched M2P deposits
        for m2p_row in m2p_deposits:
            if m2p_row not in matched_m2p:
                unmatched.append({
                    "Source": "M2p Deposit",
                    "Date": m2p_row.created.strftime('%Y-%m-%d'),
                    "Client ID": "",
                    "Trading Account": m2p_row.trading_account,
                    "Amount": m2p_row.final_amount,
                    "Client Name": "",
                    "Confirmed": "",
                    "Row Index": m2p_row.id
                })

        return pd.DataFrame(unmatched)

    except Exception as e:
        print(f"Error in deposit comparison: {e}")
        return pd.DataFrame()

def get_date_range_from_data():
    """Get the overall date range from all data sources"""
    try:
        all_dates = []
        
        # Collect dates from all tables
        for rebate in IBRebate.query.all():
            if rebate.rebate_time:
                all_dates.append(rebate.rebate_time)
                
        for deposit in CRMDeposit.query.all():
            if deposit.request_time:
                all_dates.append(deposit.request_time)
                
        for withdrawal in CRMWithdrawal.query.all():
            if withdrawal.review_time:
                all_dates.append(withdrawal.review_time)
                
        for deposit in M2pDeposit.query.all():
            if deposit.created:
                all_dates.append(deposit.created)
                
        for deposit in SettlementDeposit.query.all():
            if deposit.created:
                all_dates.append(deposit.created)
                
        for withdrawal in M2pWithdraw.query.all():
            if withdrawal.created:
                all_dates.append(withdrawal.created)
                
        for withdrawal in SettlementWithdraw.query.all():
            if withdrawal.created:
                all_dates.append(withdrawal.created)

        if all_dates:
            min_date = min(all_dates)
            max_date = max(all_dates)
            return min_date, max_date
        
        return None, None
        
    except Exception as e:
        print(f"Error getting date range: {e}")
        return None, None