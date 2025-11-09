import streamlit as st
import pandas as pd
import requests
import pytz
from datetime import datetime, time
import os

# ------------------------------
# ⚙️ Trendyol API Bilgileri
# ------------------------------
SELLER_ID = "107703"
API_KEY = "BTDnnGqkUveH8tSlGFC4"
API_SECRET = "wwDwc4pXf4J563N1pJww"

# ------------------------------
# 💾 Dosya Yolları
# ------------------------------
EXCEL_FILE = "urunler_ty.xlsx"
PARQUET_FILE = "urunler_ty.parquet"

# ------------------------------
# 💾 Ürün Cache Yükleme
# ------------------------------
def load_products_cache():
    """Parquet varsa oradan oku, yoksa Excel’den oku."""
    if os.path.exists(PARQUET_FILE):
        st.info("💽 Ürün verisi alınıyor...")
        return pd.read_parquet(PARQUET_FILE)
    elif os.path.exists(EXCEL_FILE):
        st.info("📗 Ürün verisi Excel'den okunuyor...")
        df = pd.read_excel(EXCEL_FILE)
        df.to_parquet(PARQUET_FILE)
        return df
    else:
        st.error("❌ Ürün verisi bulunamadı. Lütfen 'urunler_ty.parquet' dosyasını ekleyin.")
        st.stop()

# ------------------------------
# 🧩 Streamlit Başlangıç
# ------------------------------
st.set_page_config(page_title="Trendyol Satış Dashboard", layout="wide")
st.title("📦 Trendyol Satış Dashboard")

# ------------------------------
# 📅 Tarih Aralığı Seçimi
# ------------------------------
st.sidebar.header("Tarih Aralığı")
start_date = st.sidebar.date_input("Başlangıç Tarihi", datetime.now())
end_date = st.sidebar.date_input("Bitiş Tarihi", datetime.now())

# ------------------------------
# 🗂️ Sipariş Durumu Seçimi
# ------------------------------
st.sidebar.header("Sipariş Durumu")
status_option = st.sidebar.selectbox(
    "Durum Seçin",
    ["All"]
)

statuses_to_fetch = (
    ["Created", "Shipped", "Delivered", "Invoiced", "Picking"]
    if status_option == "All"
    else [status_option]
)

# ------------------------------
# 🚀 Verileri Getir Butonu
# ------------------------------
if st.sidebar.button("Verileri Getir"):
    st.info("🔄 Trendyol sipariş verileri alınıyor...")

    tz = pytz.timezone("Europe/Istanbul")  # GMT+3
    start_ts = int(tz.localize(datetime.combine(start_date, time.min)).timestamp() * 1000)
    end_ts = int(tz.localize(datetime.combine(end_date, time.max)).timestamp() * 1000)

    all_orders = []

    for status in statuses_to_fetch:
        page = 0
        while True:
            url = (
                f"https://apigw.trendyol.com/integration/order/sellers/{SELLER_ID}/orders"
                f"?status={status}&startDate={start_ts}&endDate={end_ts}"
                f"&orderByField=PackageLastModifiedDate&orderByDirection=DESC&size=200&page={page}"
            )
            response = requests.get(url, auth=(API_KEY, API_SECRET))
            if response.status_code != 200:
                st.error(f"API Hatası ({status}): {response.status_code}")
                st.stop()

            data = response.json()
            content = data.get("content", [])
            if not content:
                break

            all_orders.extend(content)
            page += 1
            if len(content) < 200:
                break

    if not all_orders:
        st.warning("Belirtilen aralıkta sipariş bulunamadı.")
        st.stop()

    # ------------------------------
    # 🧮 DataFrame Oluşturma
    # ------------------------------
    records = []
    tz_istanbul = pytz.timezone("Europe/Istanbul")

    for order in all_orders:
        created_ts = None
        for history in order.get("packageHistories", []):
            if history.get("status") == "Created":
                created_ts = history.get("createdDate")
                break

        if not created_ts:
            continue

        created_date = datetime.fromtimestamp(created_ts / 1000, tz=pytz.UTC).astimezone(tz_istanbul)

        for line in order.get("lines", []):
            records.append({
                "createdDate": created_date,
                "barcode": str(line.get("barcode")),
                "quantity": line.get("quantity"),
                "price": line.get("price"),
                "ciro": line.get("price", 0) * line.get("quantity", 0)
            })

    df = pd.DataFrame(records)
    if df.empty:
        st.warning("Sipariş verisi bulunamadı.")
        st.stop()

    # 📅 Tarih filtresi
    start_dt = tz_istanbul.localize(datetime.combine(start_date, time.min))
    end_dt = tz_istanbul.localize(datetime.combine(end_date, time.max))
    df = df[(df["createdDate"] >= start_dt) & (df["createdDate"] <= end_dt)]

    if df.empty:
        st.warning("Seçilen tarih aralığında sipariş yok.")
        st.stop()

    # 💾 Ürün bilgisi cache'den oku
    df_products = load_products_cache()
    df_merged = df.merge(df_products, on="barcode", how="left")

    # Eksik alanları doldur
    df_merged["productMainId"].fillna("UNKNOWN", inplace=True)
    df_merged["image"].fillna("https://via.placeholder.com/150", inplace=True)
    df_merged["productUrl"].fillna("#", inplace=True)
    df_merged["brand"].fillna("-", inplace=True)
    df_merged["categoryName"].fillna("-", inplace=True)

    # 📊 Gruplama
    df_grouped = (
        df_merged.groupby(["productMainId", "image", "productUrl", "brand", "categoryName"], as_index=False)
        .agg({"quantity": "sum", "ciro": "sum"})
        .sort_values(by="quantity", ascending=False)
        .reset_index(drop=True)
    )

    # ------------------------------
    # 🧾 Özet Bilgiler
    # ------------------------------
    toplam_ciro = df_grouped["ciro"].sum()
    toplam_adet = df_grouped["quantity"].sum()
    toplam_urun = len(df_grouped)

    st.markdown("### 📈 Satış Özeti")
    col1, col2, col3 = st.columns(3)
    col1.metric("Toplam Ciro", f"{toplam_ciro:,.2f} ₺")
    col2.metric("Toplam Satış Adedi", f"{int(toplam_adet)}")
    col3.metric("Toplam Ürün Sayısı", f"{toplam_urun}")

    st.divider()

    # ------------------------------
    # 💳 Top 10 Marka Kartı
    # ------------------------------
    top_brands = (
        df_grouped.groupby("brand", as_index=False)
        .agg({"quantity": "sum", "ciro": "sum"})
        .sort_values(by="quantity", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    st.markdown(
        f"""
        <div style="
            background-color:#f8f9fa;
            border-radius:16px;
            padding:16px;
            margin-bottom:20px;
            box-shadow:0 2px 5px rgba(0,0,0,0.1);
            border:1px solid #ddd;
            font-family:sans-serif;
        ">
            <div style="display:flex; font-weight:bold; border-bottom:1px solid #ccc; padding-bottom:8px; margin-bottom:8px; font-size:16px;">
                <div style="flex:1; text-align:left;">Marka</div>
                <div style="flex:1; text-align:center;">Adet</div>
                <div style="flex:1; text-align:right;">Satış</div>
            </div>
            {"".join([
                f'<div style="display:flex; padding:4px 0; font-size:14px;">'
                f'<div style="flex:1; text-align:left;">{row["brand"]}</div>'
                f'<div style="flex:1; text-align:center;">{int(row["quantity"])}</div>'
                f'<div style="flex:1; text-align:right;">{row["ciro"]:,.2f} TL</div>'
                f'</div>'
                for idx, row in top_brands.iterrows()
            ])}
        </div>
        """,
        unsafe_allow_html=True
    )

    # ------------------------------
    # 💳 Top 10 Kategori Kartı
    # ------------------------------
    top_categories = (
        df_grouped.groupby("categoryName", as_index=False)
        .agg({"quantity": "sum", "ciro": "sum"})
        .sort_values(by="quantity", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    st.markdown(
        f"""
        <div style="
            background-color:#f8f9fa;
            border-radius:16px;
            padding:16px;
            margin-bottom:20px;
            box-shadow:0 2px 5px rgba(0,0,0,0.1);
            border:1px solid #ddd;
            font-family:sans-serif;
        ">
            <div style="display:flex; font-weight:bold; border-bottom:1px solid #ccc; padding-bottom:8px; margin-bottom:8px; font-size:16px;">
                <div style="flex:1; text-align:left;">Kategori</div>
                <div style="flex:1; text-align:center;">Adet</div>
                <div style="flex:1; text-align:right;">Satış</div>
            </div>
            {"".join([
                f'<div style="display:flex; padding:4px 0; font-size:14px;">'
                f'<div style="flex:1; text-align:left;">{row["categoryName"]}</div>'
                f'<div style="flex:1; text-align:center;">{int(row["quantity"])}</div>'
                f'<div style="flex:1; text-align:right;">{row["ciro"]:,.2f} TL</div>'
                f'</div>'
                for idx, row in top_categories.iterrows()
            ])}
        </div>
        """,
        unsafe_allow_html=True
    )

    # ------------------------------
    # 💳 Ürün Kartları
    # ------------------------------
    for i in range(0, len(df_grouped), 5):
        cols = st.columns(5)
        for j, col in enumerate(cols):
            if i + j < len(df_grouped):
                row = df_grouped.iloc[i + j]
                with col:
                    st.markdown(
                        f"""
                        <div style="
                            background-color: #f8f9fa;
                            border-radius: 16px;
                            padding: 16px;
                            margin-bottom: 16px;
                            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                            border: 1px solid #ddd;
                            text-align: center;">
                            <img src="{row['image']}" width="120" style="border-radius:8px; margin-bottom:10px;">
                            <p style="color:#333;">{row['productMainId']}</p>
                            <p style="color:#555;">{row['brand']}</p>
                            <p><b>Satış Adedi:</b> {int(row['quantity'])}</p>
                            <p><b>Ciro:</b> {row['ciro']:,.2f} ₺</p>
                            <a href="{row['productUrl']}" target="_blank">🔗 Ürünü Gör</a>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
