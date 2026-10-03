import streamlit as st
import pandas as pd
import s3fs

st.set_page_config(page_title="Bank Pipeline UI", layout="wide")
st.title("🏦 Bank Batch Pipeline - Data Lake Viewer")
st.markdown("Bu interfeys birbaşa **MinIO (S3)** anbarına qoşularaq Parquet fayllarını oxuyur və vizuallaşdırır.")

layer = st.sidebar.selectbox("Təbəqəni seçin (Layer)", ["bronze", "silver", "gold"])

if layer == "bronze" or layer == "silver":
    tables = ["accounts", "branches", "customers", "customer_history", "transactions", "dim_date", "loan_lifecycle"]
else:
    tables = ["fact_transaction", "dim_customer", "dim_branch", "dim_account", "dim_date"]

table = st.sidebar.selectbox("Cədvəli seçin", tables)

if st.sidebar.button("Datanı Yüklə"):
    st.subheader(f"Layer: {layer.upper()} | Cədvəl: {table}")
    try:
        with st.spinner('MinIO-dan məlumatlar çəkilir...'):
            # S3-den birbasa oxumaq ucun
            df = pd.read_parquet(
                f"s3://{layer}/{table}/",
                storage_options={
                    "client_kwargs": {'endpoint_url': 'http://minio:9000'},
                    "key": "admin",
                    "secret": "password123"
                }
            )
        st.dataframe(df.head(100), use_container_width=True)
        st.success(f"Məlumat uğurla yükləndi! (Cədvəldəki cəmi sətir sayı: {len(df)})")
    except Exception as e:
        st.error(f"Xəta baş verdi. Məlumat hələ mövcud olmaya bilər. Detal: {e}")
