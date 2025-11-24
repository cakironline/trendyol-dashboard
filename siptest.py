import streamlit as st
import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta, time
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
    if os.path.exists(PARQUET_FILE):
        st.info("💽 Ürün verisi alınıyor...")
        return pd.read_parquet(PARQUET_FILE)
    elif os.path.exists(EXCEL_FILE):
        st.info("📗 Ürün verisi Excel'den okunuyor...")
        df = pd.read_excel(EXCEL_FILE)
        df.to_parquet(PARQUET_FILE)
        return df
    else:
        st.error("❌ Ürün verisi bulunamadı.")
        st.stop()

# --------------------------------------------------
# 📌 14 GÜNLÜK PARÇALARA BÖLEN FONKSİYON
# --------------------------------------------------
def split_date_range(start, end, days=14):
    ranges = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=days), end)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(seconds=1)
    return ranges

# --------------------------------------------------
# 📌 ÖNCE SON 14 GÜNÜ ALAN + KALANI PARÇALAYAN FONKSİYON
# --------------------------------------------------
def build_date_ranges_with_last_14_days(user_start, user_end):
    tz = pytz.timezone("Europe/Istanbul")

    today = datetime.now(tz)
    last_14_start = today - timedelta(days=14)

    # Her zaman son 14 gün eklenecek
    final_ranges = [(last_14_start, today)]

    user_start = tz.localize(datetime.combine(user_start, time.min))
    user_end = tz.localize(datetime.combine(user_end, time.max))

    # Eğer kullanıcı sadece son 14 gün seçmişse ekstra eklemeye gerek yok
    if user_end <= last_14_start:
        # Kullanıcı seçimi tamamen eski → tamamını böl
        final_ranges.extend(split_date_range(user_start, user_end))
    else:
        # Kullanıcının seçtiği eski dönem (varsa)
        if user_start < last_14_start:
            old_end = last_14_start - timedelta(seconds=1)
            final_ranges.extend(split_date_range(user_start, old_end))

    return final_ranges


# ------------------------------
# 🧩 Streamlit Başlangıç
# ------------------------------
st.set_page_config(page_title="Trendyol Satış Dashboard", layout="wide")
st.title("⚡ Trendyol Satış Dashboard")

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
status_option = st.sidebar.selectbox("Durum Seçin", ["All"])
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

    tz = pytz.timezone("Europe/Istanbul")

    # ✨ BURADA 14 GÜNLÜK OTOMATİK PARÇALAMA ÇALIŞIYOR
    date_ranges = build_date_ranges_with_last_14_days(start_date, end_date)

    all_orders = []

    # -----------------------------------
    # 🔁 TÜM ARALIKLAR İÇİN API ÇAĞRISI
    # -----------------------------------
    for status in statuses_to_fetch:
        for dr_start, dr_end in date_ranges:

            start_ts = int(dr_start.timestamp() * 1000)
            end_ts = int(dr_end.timestamp() * 1000)

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
        st.warning("Seçilen aralıkta sipariş bulunamadı.")
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

    # 📅 Tarih filtresi (kullanıcı aralığı)
    start_dt = tz_istanbul.localize(datetime.combine(start_date, time.min))
    end_dt = tz_istanbul.localize(datetime.combine(end_date, time.max))
    df = df[(df["createdDate"] >= start_dt) & (df["createdDate"] <= end_dt)]

    # 🔥 Ürün bilgisi cache'den oku
    df_products = load_products_cache()
    df_merged = df.merge(df_products, on="barcode", how="left")

    df_merged["productMainId"].fillna("UNKNOWN", inplace=True)
    df_merged["image"].fillna("https://via.placeholder.com/150", inplace=True)
    df_merged["productUrl"].fillna("#", inplace=True)
    df_merged["brand"].fillna("-", inplace=True)
    df_merged["categoryName"].fillna("-", inplace=True)

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
    col3.metric("Toplam Option Sayısı", f"{toplam_urun}")

    st.divider()

    # ------------------------------
    # 💳 Top 10 Marka ve Kategori
    # ------------------------------
    top_brands = (
        df_grouped.groupby("brand", as_index=False)
        .agg({"quantity": "sum", "ciro": "sum"})
        .sort_values(by="quantity", ascending=False)
        .head(10)
    )

    top_categories = (
        df_grouped.groupby("categoryName", as_index=False)
        .agg({"quantity": "sum", "ciro": "sum"})
        .sort_values(by="quantity", ascending=False)
        .head(10)
    )

    col1, col2 = st.columns(2)

    def render_top10_card(title, df_top, col1_name):
        st.markdown(
            f"""
            <div style="
                background-color:#ffc430;
                border-radius:16px;
                padding:16px;
                margin-bottom:20px;
                box-shadow:0 2px 5px rgba(0,0,0,0.1);
                border:1px solid #ddd;">
                <div style="font-weight:bold; font-size:18px; margin-bottom:10px;">{title}</div>
                <div style="display:flex; font-weight:bold; border-bottom:1px solid #ccc; padding-bottom:6px; margin-bottom:6px;">
                    <div style="flex:1; text-align:left;">{col1_name}</div>
                    <div style="flex:1; text-align:center;">Adet</div>
                    <div style="flex:1; text-align:right;">Ciro</div>
                </div>
                {"".join([
                    f'<div style="display:flex; padding:2px 0;">'
                    f'<div style="flex:1;">{row[0]}</div>'
                    f'<div style="flex:1; text-align:center;">{int(row[1])}</div>'
                    f'<div style="flex:1; text-align:right;">{row[2]:,.2f} TL</div>'
                    f'</div>'
                    for row in df_top.itertuples(index=False, name=None)
                ])}
            </div>
            """,
            unsafe_allow_html=True
        )

    with col1:
        render_top10_card("📊 En Çok Satan Markalar", top_brands[["brand","quantity","ciro"]], "Marka")

    with col2:
        render_top10_card("📊 En Çok Satan Kategoriler", top_categories[["categoryName","quantity","ciro"]], "Kategori")


    # ------------------------------
    # 💳 Ürün Kartları
    # ------------------------------
    for i in range(0, len(df_grouped), 5):
        cols = st.columns(5)
        for j, col in enumerate(cols):
            if i + j < len(df_grouped):
                row = df_grouped.iloc[i + j]
                ortalama_fiyat = row["ciro"] / row["quantity"] if row["quantity"] else 0

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
                            <img src="{row['image']}" width="120" style="border-radius:8px;">
                            <p style="font-weight:bold;">{row['productMainId']}</p>
                            <p>{row['brand']}</p>
                            <p><b>Satış Adedi:</b> {int(row['quantity'])}</p>
                            <p><b>Ciro:</b> {row['ciro']:,.2f} ₺</p>
                            <p><b>Ortalama Fiyat:</b> {ortalama_fiyat:,.2f} ₺</p>
                            <a href="{row['productUrl']}" target="_blank">🔗 Ürünü Gör</a>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
