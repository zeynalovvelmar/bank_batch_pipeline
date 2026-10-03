CREATE TABLE account_balance_snapshot (snapshot_id TEXT, account_id TEXT, date_key TEXT, closing_balance TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY account_balance_snapshot FROM '/data/account_balance_snapshot.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE account_customer_bridge (account_id TEXT, customer_id TEXT, allocation_weight TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY account_customer_bridge FROM '/data/account_customer_bridge.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE accounts (account_id TEXT, customer_id TEXT, branch_id TEXT, account_type TEXT, account_status TEXT, created_at TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY accounts FROM '/data/accounts.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE branches (branch_id TEXT, branch_name TEXT, region TEXT, country TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY branches FROM '/data/branches.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE customer_history (history_id TEXT, customer_id TEXT, address TEXT, risk_segment TEXT, effective_from TEXT, effective_to TEXT, is_current TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY customer_history FROM '/data/customer_history.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE customers (customer_id TEXT, first_name TEXT, last_name TEXT, email TEXT, date_of_birth TEXT, prior_credit_rating TEXT, current_credit_rating TEXT, hash_nk TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY customers FROM '/data/customers.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE dim_date (date_key TEXT, full_date TEXT, year TEXT, quarter TEXT, month TEXT, day TEXT, day_of_week TEXT, is_weekend TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY dim_date FROM '/data/dim_date.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE loan_lifecycle (loan_id TEXT, account_id TEXT, start_date_key TEXT, end_date_key TEXT, loan_amount TEXT, status TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY loan_lifecycle FROM '/data/loan_lifecycle.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE transactions (transaction_id TEXT, account_id TEXT, date_key TEXT, amount TEXT, currency TEXT, channel TEXT, status TEXT, is_flagged_fraud TEXT, transaction_ref TEXT, load_run_id TEXT, source_system TEXT, processed_at TEXT);
COPY transactions FROM '/data/transactions.csv' DELIMITER ',' CSV HEADER;
