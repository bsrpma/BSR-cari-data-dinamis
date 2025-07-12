import os
import pandas as pd

# Atur tampilan terminal
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# Dapatkan direktori sekarang, parent, dan grandparent
dir_now = os.getcwd()
parent = os.path.dirname(dir_now)
gparent = os.path.dirname(parent)

def cari_data(filter_dict, kolom_group, kolom_tampil):
    dbase_juni = "R09 DBASE GT PRIANGAN TIMUR.parquet"
    loc_dbs_juni = os.path.join(gparent, "PMA_2", "dbase_gab", "_hasil", "25.07.R09", dbase_juni)

    # Cek file
    if not os.path.isfile(loc_dbs_juni):
        print(f'dbase {dbase_juni} tidak ditemukan')
        return None

    df = pd.read_parquet(loc_dbs_juni)
    print("Kolom tersedia:", df.columns.tolist())

    # --- FILTER DINAMIS ---
    for kolom, nilai in filter_dict.items():
        if kolom == "NAMA_SLS2_AWAL":
            continue  # akan diproses di bawah

        if kolom in df.columns and nilai:
            if kolom == "QTY" and isinstance(nilai, list) and len(nilai) > 0:
                if "KODE OUTLET" not in df.columns:
                    print("Kolom KODE OUTLET tidak tersedia untuk filter QTY")
                    continue
                group_qty = df.groupby("KODE OUTLET", as_index=False)["QTY"].sum()
                filter_exp = nilai[0]
                operator = filter_exp[:1]
                angka = float(filter_exp[1:])
                if operator == ">":
                    valid_outlet = group_qty[group_qty["QTY"] > angka]["KODE OUTLET"].tolist()
                elif operator == "<":
                    valid_outlet = group_qty[group_qty["QTY"] < angka]["KODE OUTLET"].tolist()
                else:
                    raise ValueError("Operator QTY hanya didukung > atau <")
                df = df[df["KODE OUTLET"].isin(valid_outlet)]
            else:
                df = df[df[kolom].isin(nilai)]

    # --- Filter khusus NAMA_SLS2_AWAL (di luar loop) ---
    prefix = filter_dict.get("NAMA_SLS2_AWAL", "")
    if prefix and "NAMA SLS2" in df.columns:
        df = df[df["NAMA SLS2"].str.startswith(prefix, na=False)]

    # --- Pastikan kolom grouping valid ---
    kolom_group_valid = [k for k in kolom_group if k in df.columns]

    # --- Group by ---
    df_grouped = df.groupby(kolom_group_valid, as_index=False)[["QTY", "VALUE"]].sum()

    # Format
    df_grouped["QTY_fmt"] = df_grouped["QTY"].apply(lambda x: f"{x:,.1f}")
    df_grouped["VALUE_fmt"] = df_grouped["VALUE"].apply(lambda x: f"{x:,.0f}")

    # Kolom display
    kolom_tampil_valid = [k for k in kolom_tampil if k in df_grouped.columns]
    kolom_final = kolom_tampil_valid + ["QTY_fmt", "VALUE_fmt"]
    df_display = df_grouped[kolom_final]

    # Total
    total_qty = df_grouped["QTY"].sum()
    total_value = df_grouped["VALUE"].sum()

    # Print
    print("\n===== Data (max 150 baris pertama) =====")
    print(df_display.head(150))

    print("\n===== Total Keseluruhan Dari Data Terfilter =====")
    print(f"Total QTY   : {total_qty:,.1f}")
    print(f"Total VALUE : {total_value:,.0f}")

    return df_display

def simpan_ke_excel(df):
    jawab = input("\nSimpan ke Excel? (ketik '1' untuk Ya, selain itu untuk Tidak): ")
    if jawab.strip() == "1":
        nama_file = input("Masukkan nama file Excel (tanpa .xlsx): ")
        if not nama_file:
            nama_file = "hasil_export"
        full_path = f"{nama_file}.xlsx"
        df.to_excel(full_path, index=False)
        print(f"✅ Data berhasil disimpan ke: {full_path}")
    else:
        print("❌ Tidak disimpan ke Excel.")

# ==========================
# --- SETTING DINAMIS DI SINI ---
# ==========================
filter_dict = {
    "KODE OUTLET": [],
    "NAMA OUTLET": [],
    "CHANNEL": [],
    "FC": [],
    "RUTE": [],
    "PMA": [],
    "KODE SALESMAN": [],
    "TGL": [],
    "KD_BRG": [303174],
    "NM_BRG": [],
    "BU": [],
    "MARK": [],
    "KODE BARANG": [],
    "DESCRIPTION": [],
    "QTY": [">2"],
    "VALUE": [],
    "KET": [],
    "BLN": [],
    "CUST-OUTLET": [],
    "REGION": [],
    "DIV": [],
    "Kd_Cust.": [],
    "KD SLS2": [],
    "NIK SLS2": [],
    "NAMA SLS2": [],
    "NAMA_SLS2_AWAL": "AE",
    "VALUE NETT": [],
    "NO FAKTUR": [],
    "VER ITEM": [],
    "BP": []
}

kolom_group_pilihan = ["PMA", "NAMA SLS2", "KD_BRG", "NM_BRG"]
kolom_tampil_pilihan = ["PMA", "NAMA SLS2"]

# ==========================
if __name__ == "__main__":
    df_hasil = cari_data(filter_dict, kolom_group_pilihan, kolom_tampil_pilihan)
    if df_hasil is not None and not df_hasil.empty:
        simpan_ke_excel(df_hasil)
