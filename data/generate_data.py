import os
import random
import pandas as pd
from datetime import datetime, timedelta

output_dir = os.path.dirname(os.path.abspath(__file__))

load_run_id = "RUN_2026_09_20_01"
source_system = "POSTGRES_OLTP"
processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def add_audit_cols(df):
    df["load_run_id"] = load_run_id
    df["source_system"] = source_system
    df["processed_at"] = processed_at
    return df


# 1. Customers (50 sətir)
customers = []
for c_id in range(1, 51):
    customers.append(
        {
            "customer_id": c_id,
            "first_name": f"First_{c_id}",
            "last_name": f"Last_{c_id}",
            "email": f"user_{c_id}@example.com",
            "date_of_birth": (
                datetime(1980, 1, 1) + timedelta(days=c_id * 150)
            ).strftime("%Y-%m-%d"),
            "prior_credit_rating": random.choice(["A", "B", "C", "D"]),
            "current_credit_rating": random.choice(["A+", "A", "B", "C"]),
            "hash_nk": f"hash_customer_{c_id}",
        }
    )
customers_df = pd.DataFrame(customers)
add_audit_cols(customers_df).to_csv(
    os.path.join(output_dir, "customers.csv"), index=False
)

# 2. Customer History (SCD Type 2)
history = []
h_id = 100
for c_id in range(1, 51):
    h_id += 1
    # Köhnə versiya
    history.append(
        {
            "history_id": h_id,
            "customer_id": c_id,
            "address": f"Old Address {c_id}",
            "risk_segment": random.choice(["Low", "Medium"]),
            "effective_from": "2024-01-01",
            "effective_to": "2025-01-01",
            "is_current": False,
        }
    )
    h_id += 1
    # Cari aktiv versiya
    history.append(
        {
            "history_id": h_id,
            "customer_id": c_id,
            "address": f"Current Address {c_id}",
            "risk_segment": random.choice(["Low", "Medium", "High"]),
            "effective_from": "2025-01-01",
            "effective_to": None,
            "is_current": True,
        }
    )
customer_history_df = pd.DataFrame(history)
add_audit_cols(customer_history_df).to_csv(
    os.path.join(output_dir, "customer_history.csv"), index=False
)

# 3. Branches (5 regional filial)
branches_df = pd.DataFrame(
    [
        {
            "branch_id": 10,
            "branch_name": "Central Branch",
            "region": "Absheron",
            "country": "Azerbaijan",
        },
        {
            "branch_id": 20,
            "branch_name": "Ganja Branch",
            "region": "Ganja-Dashkasan",
            "country": "Azerbaijan",
        },
        {
            "branch_id": 30,
            "branch_name": "Sumqayit Branch",
            "region": "Absheron",
            "country": "Azerbaijan",
        },
        {
            "branch_id": 40,
            "branch_name": "Shaki Branch",
            "region": "Shaki-Zaqatala",
            "country": "Azerbaijan",
        },
        {
            "branch_id": 50,
            "branch_name": "Lankaran Branch",
            "region": "Lankaran-Astara",
            "country": "Azerbaijan",
        },
    ]
)
add_audit_cols(branches_df).to_csv(
    os.path.join(output_dir, "branches.csv"), index=False
)

# 4. Accounts (100 hesab)
accounts = []
for a_id in range(1001, 1101):
    c_id = random.randint(1, 50)
    b_id = random.choice([10, 20, 30, 40, 50])
    accounts.append(
        {
            "account_id": a_id,
            "customer_id": c_id,
            "branch_id": b_id,
            "account_type": random.choice(["Checking", "Savings", "Deposit"]),
            "account_status": "Active",
            "created_at": "2024-01-10",
        }
    )
accounts_df = pd.DataFrame(accounts)
add_audit_cols(accounts_df).to_csv(
    os.path.join(output_dir, "accounts.csv"), index=False
)

# 5. Bridge Table
bridge = []
for acc in accounts:
    bridge.append(
        {
            "account_id": acc["account_id"],
            "customer_id": acc["customer_id"],
            "allocation_weight": 1.0,
        }
    )
bridge_df = pd.DataFrame(bridge)
add_audit_cols(bridge_df).to_csv(
    os.path.join(output_dir, "account_customer_bridge.csv"), index=False
)

# 6. Dim Date (2026-cı ilin sentyabr ayı)
dates = []
base_date = datetime(2026, 9, 1)
for d in range(30):
    curr_date = base_date + timedelta(days=d)
    dates.append(
        {
            "date_key": int(curr_date.strftime("%Y%m%d")),
            "full_date": curr_date.strftime("%Y-%m-%d"),
            "year": curr_date.year,
            "quarter": (curr_date.month - 1) // 3 + 1,
            "month": curr_date.month,
            "day": curr_date.day,
            "day_of_week": curr_date.strftime("%A"),
            "is_weekend": curr_date.weekday() >= 5,
        }
    )
dates_df = pd.DataFrame(dates)
add_audit_cols(dates_df).to_csv(os.path.join(output_dir, "dim_date.csv"), index=False)

# 7. Transactions (1,000 sətir + 5 Mütləq Xəta)
transactions = []

# Mütləq xətalar (Defects 1-5)
transactions.append(
    {
        "transaction_id": 4,
        "account_id": 1001,
        "date_key": 20260902,
        "amount": 300.0,
        "currency": "AZN",
        "channel": "Web",
        "status": "Completed",
        "is_flagged_fraud": False,
        "transaction_ref": "TXN_4_A",
    }
)
transactions.append(
    {
        "transaction_id": 4,
        "account_id": 1001,
        "date_key": 20260902,
        "amount": 300.0,
        "currency": "AZN",
        "channel": "Web",
        "status": "Completed",
        "is_flagged_fraud": False,
        "transaction_ref": "TXN_4_B",
    }
)  # Defect 1: Duplicate
transactions.append(
    {
        "transaction_id": 6,
        "account_id": 1001,
        "date_key": 20260903,
        "amount": None,
        "currency": "AZN",
        "channel": "Mobile",
        "status": "Pending",
        "is_flagged_fraud": False,
        "transaction_ref": "TXN_6",
    }
)  # Defect 2: NULL amount
transactions.append(
    {
        "transaction_id": 7,
        "account_id": 999,
        "date_key": 20260903,
        "amount": 80.0,
        "currency": "AZN",
        "channel": "POS",
        "status": "Completed",
        "is_flagged_fraud": False,
        "transaction_ref": "TXN_7",
    }
)  # Defect 3: Orphan FK
transactions.append(
    {
        "transaction_id": 8,
        "account_id": 1002,
        "date_key": 20260903,
        "amount": -50.0,
        "currency": "XXX",
        "channel": "Web",
        "status": "Failed",
        "is_flagged_fraud": True,
        "transaction_ref": "TXN_8",
    }
)  # Defect 4 & 5: Negative & Invalid Currency

# Qalan ~1000 düzgün tranzaksiya
for t_id in range(10, 1010):
    date_obj = base_date + timedelta(days=random.randint(0, 29))
    transactions.append(
        {
            "transaction_id": t_id,
            "account_id": random.randint(1001, 1100),
            "date_key": int(date_obj.strftime("%Y%m%d")),
            "amount": round(random.uniform(10.0, 2500.0), 2),
            "currency": random.choice(["AZN", "USD", "EUR"]),
            "channel": random.choice(["ATM", "Mobile", "POS", "Web"]),
            "status": "Completed",
            "is_flagged_fraud": random.random() < 0.02,
            "transaction_ref": f"TXN_{t_id}",
        }
    )

transactions_df = pd.DataFrame(transactions)
add_audit_cols(transactions_df).to_csv(
    os.path.join(output_dir, "transactions.csv"), index=False
)

# 8. Snapshots (100 hesab üzrə)
snapshots = []
s_id = 1
for acc in accounts:
    snapshots.append(
        {
            "snapshot_id": s_id,
            "account_id": acc["account_id"],
            "date_key": 20260930,
            "closing_balance": round(random.uniform(500.0, 15000.0), 2),
        }
    )
    s_id += 1
snapshot_df = pd.DataFrame(snapshots)
add_audit_cols(snapshot_df).to_csv(
    os.path.join(output_dir, "account_balance_snapshot.csv"), index=False
)

# 9. Loans (20 kredit)
loans = []
for l_id in range(501, 521):
    loans.append(
        {
            "loan_id": l_id,
            "account_id": random.randint(1001, 1100),
            "start_date_key": 20260901,
            "end_date_key": 20260925,
            "loan_amount": round(random.uniform(2000.0, 20000.0), 2),
            "status": random.choice(["Active", "Closed"]),
        }
    )
loan_df = pd.DataFrame(loans)
add_audit_cols(loan_df).to_csv(
    os.path.join(output_dir, "loan_lifecycle.csv"), index=False
)

print("1,000+ sətirlik dolğun test datası uğurla formalaşdırıldı!")
