import os
import sys
import requests
import pandas as pd

# ==========================
# --- CEK VERSI GITHUB ---
# ==========================
class GitHelper:
    url_version = "https://raw.githubusercontent.com/bsrpma/BSR-cari-data-dinamis/main/version.txt"

    def __init__(self, versi_lokal="1.0.0"):
        self.versi_lokal = versi_lokal

    def cek_versi(self):
        try:
            r = requests.get(self.url_version, timeout=5)
            r.raise_for_status()
            versi_online = r.text.strip()

            if versi_online != self.versi_lokal:
                print(f"⚠️ Versi baru tersedia: {versi_online} (lokal: {self.versi_lokal})")
                print("  [1] Download versi baru")
                print("  [2] Lanjut pakai versi sekarang")
                pilihan = input("Masukkan pilihan (1/2): ").strip()

                if pilihan == "1":
                    print("▶️ Silakan download manual dari repo Github:")
                    print("   https://github.com/bsrpma/BSR-cari-data-dinamis")
                    print("✅ Setelah download, jalankan ulang script.")
                else:
                    print("Lanjut dengan versi lokal...\n")
            else:
                print("✅ Sudah versi terbaru.\n")

        except requests.exceptions.ConnectionError:
            print("⚠️ Tidak ada koneksi internet. Lanjut dengan versi lokal...\n")
        except Exception as e:
            print(f"❌ Gagal cek versi: {e}\nLanjut dengan versi lokal...\n")

# ==========================
# --- SETTING PANDAS ---
# ==========================
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# Dapatkan direktori sekarang, parent, dan grandparent
dir_now = os.getcwd()
parent = os.path.dirname(dir_now)
gparent = os.path.dirname(parent)

# ==========================
# --- FUNGSI CARI DATA ---
# ==========================
def cari_data(filter_dict, kolom_group, kolom_tampil):
    dbase_juni = "R09 DBASE GT PRIANGAN TIMUR.parquet"
    loc_dbs_juni = os.path.join(gparent, "PMA_2", "dbase_gab", "_hasil", "25.07.R09", dbase_juni)

    if not os.path.isfile(loc_dbs_juni):
        print(f"❌ dbase {dbase_juni} tidak ditemukan di {loc_dbs_juni}")
        return None, 0, 0

    df = pd.read_parquet(loc_dbs_juni)
    print("Kolom tersedia:", df.columns.tolist())

    # --- FILTER DINAMIS ---
    for kolom, nilai in filter_dict.items():
        if kolom == "NAMA_SLS2_AWAL":
            continue

        if kolom in df.columns and nilai:
            if kolom == "QTY" and isinstance(nilai, list) and len(nilai) > 0:
                if "KODE OUTLET" not in df.columns:
                    print("⚠️  Kolom KODE OUTLET tidak tersedia untuk filter QTY")
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
        elif kolom not in df.columns and nilai:
            print(f"⚠️  Kolom {kolom} tidak ditemukan, dilewati.")

    # --- Filter khusus NAMA_SLS2_AWAL ---
    prefix = filter_dict.get("NAMA_SLS2_AWAL", "")
    if prefix and "NAMA SLS2" in df.columns:
        df = df[df["NAMA SLS2"].str.startswith(prefix, na=False)]

    # --- Group by ---
    kolom_group_valid = [k for k in kolom_group if k in df.columns]
    if not kolom_group_valid:
        print("❌ Tidak ada kolom grouping yang valid.")
        return None, 0, 0

    df_grouped = df.groupby(kolom_group_valid, as_index=False)[["QTY", "VALUE"]].sum()

    # Tambah kolom formatted
    df_grouped["QTY_fmt"] = df_grouped["QTY"].apply(lambda x: f"{x:,.1f}")
    df_grouped["VALUE_fmt"] = df_grouped["VALUE"].apply(lambda x: f"{x:,.0f}")

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

    return df_display, total_qty, total_value

# ==========================
# --- SIMPAN KE EXCEL ---
# ==========================
def simpan_ke_excel(df, total_qty, total_value):
    jawab = input("\nSimpan ke Excel? (ketik 'y' untuk Ya, selain itu untuk Tidak): ")
    if jawab.strip().lower() == "y":
        nama_file = input("Masukkan nama file Excel (tanpa .xlsx): ")
        if not nama_file:
            nama_file = "hasil_export"
        full_path = f"{nama_file}.xlsx"

        with pd.ExcelWriter(full_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Data")
            workbook  = writer.book
            worksheet = writer.sheets["Data"]

            # Format kolom QTY dan VALUE jika ada
            format_qty = workbook.add_format({"num_format": "#,##0.0"})
            format_value = workbook.add_format({"num_format": "#,##0"})

            for idx, col in enumerate(df.columns):
                max_length = max([len(str(s)) for s in df[col].astype(str).values] + [len(col)]) + 5

                if col == "QTY_fmt":
                    worksheet.set_column(idx, idx, max_length, format_qty)
                elif col == "VALUE_fmt":
                    worksheet.set_column(idx, idx, max_length, format_value)
                else:
                    worksheet.set_column(idx, idx, max_length)

            # Tambahkan total di bawah
            last_row = len(df) + 1  # +1 karena header
            worksheet.write(last_row, df.columns.get_loc("QTY_fmt"), f"Total QTY: {total_qty:,.1f}")
            worksheet.write(last_row + 1, df.columns.get_loc("VALUE_fmt"), f"Total VALUE: {total_value:,.0f}")

        print(f"✅ Data berhasil disimpan ke: {full_path}")
    else:
        print("❌ Tidak disimpan ke Excel.")

# ==========================
# --- SETTING FILTER ---
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
# --- MAIN ---
# ==========================
if __name__ == "__main__":
    versi_lokal = "1.0.0"
    gh = GitHelper(versi_lokal=versi_lokal)
    gh.cek_versi()

    df_hasil, total_qty, total_value = cari_data(filter_dict, kolom_group_pilihan, kolom_tampil_pilihan)
    if df_hasil is not None and not df_hasil.empty:
        simpan_ke_excel(df_hasil, total_qty, total_value)
    else:
        print("❌ Tidak ada data terfilter, tidak disimpan.")
