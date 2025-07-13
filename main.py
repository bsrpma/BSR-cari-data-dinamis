import os
import time
import pandas as pd
import requests
import subprocess
from packaging import version

# ======================
# --- Git Helper ---
# ======================
class GitHelper:
    url_version = "https://raw.githubusercontent.com/bsrpma/BSR-cari-data-dinamis/main/version.txt"
    url_script = "https://raw.githubusercontent.com/bsrpma/BSR-cari-data-dinamis/main/main.py"
    nama_file_lokal = "main.py"
    nama_file_download = "main_download.py"
    nama_bat = "replace_script.bat"

    def __init__(self, versi_lokal="1.0.0"):
        self.versi_lokal = versi_lokal

    def cek_versi(self):
        try:
            r = requests.get(self.url_version, timeout=5)
            r.raise_for_status()
            versi_online = r.text.strip()
            print(f"Versi online (dari file): '{versi_online}'")

            if versi_online != self.versi_lokal:
                print(f"⚠️ Versi baru tersedia: {versi_online} (lokal: {self.versi_lokal})")
                print("  [1] Download versi baru otomatis")
                print("  [2] Lanjut pakai versi sekarang")
                pilihan = input("Masukkan pilihan (1/2): ").strip()

                if pilihan == "1":
                    self.download_script()
                    self.buat_bat()
                    print("✅ Script baru sudah di-download.")
                    print("💡 Akan update otomatis, script akan restart...")

                    # Langsung exit supaya .bat bisa berjalan
                    exit()
                else:
                    print("Lanjut dengan versi lokal...\n")
            else:
                print("✅ Sudah versi terbaru.\n")
        except requests.exceptions.ConnectionError:
            print("⚠️ Tidak ada koneksi internet. Lanjut dengan versi lokal...\n")
        except Exception as e:
            print(f"❌ Gagal cek versi: {e}\nLanjut dengan versi lokal...\n")

    def download_script(self):
        try:
            r = requests.get(self.url_script, timeout=10)
            r.raise_for_status()
            with open(self.nama_file_download, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"❌ Gagal download script: {e}")
            exit()

    def buat_bat(self):
        isi_bat = f"""
@echo off
timeout /t 2 >nul
del "{self.nama_file_lokal}"
rename "{self.nama_file_download}" "{self.nama_file_lokal}"
del "{self.nama_bat}"
start "" "{self.nama_file_lokal}"
        """
        with open(self.nama_bat, "w") as f:
            f.write(isi_bat.strip()) 

# ======================
# --- Model ---
# ======================
class DataModel:
    def __init__(self, lokasi_db):
        self.lokasi_db = os.path.abspath(lokasi_db)
        self.df = None

    def load_data(self):
        if not os.path.isfile(self.lokasi_db):
            print(f"❌ Database tidak ditemukan: {self.lokasi_db}")
            exit()
        self.df = pd.read_parquet(self.lokasi_db)

    def apply_filter(self, filter_dict):
        df = self.df.copy()

        if "NAMA_SLS2_AWAL" in filter_dict and filter_dict["NAMA_SLS2_AWAL"]:
            prefix = filter_dict["NAMA_SLS2_AWAL"][0]
            if "NAMA SLS2" in df.columns:
                df = df[df["NAMA SLS2"].str.startswith(prefix, na=False)]

        if "QTY" in filter_dict and filter_dict["QTY"]:
            filter_exp = filter_dict["QTY"][0]
            operator = filter_exp[:1]
            angka = float(filter_exp[1:])
            group_qty = df.groupby("KODE OUTLET", as_index=False)["QTY"].sum()
            if operator == ">":
                valid_outlet = group_qty[group_qty["QTY"] > angka]["KODE OUTLET"].tolist()
            elif operator == "<":
                valid_outlet = group_qty[group_qty["QTY"] < angka]["KODE OUTLET"].tolist()
            else:
                raise ValueError("Operator QTY hanya didukung > atau <")
            df = df[df["KODE OUTLET"].isin(valid_outlet)]

        for kolom, nilai in filter_dict.items():
            if kolom in ["QTY", "NAMA_SLS2_AWAL"]:
                continue
            if kolom in df.columns and nilai:
                nilai_str = [str(v) for v in nilai]
                df = df[df[kolom].astype(str).isin(nilai_str)]
            elif kolom not in df.columns and nilai:
                print(f"⚠️ Kolom {kolom} tidak ditemukan, dilewati.")

        return df

# ======================
# --- View ---
# ======================
class DataView:
    @staticmethod
    def view_terminal(df_grouped, total_qty, total_value):
        print("\n===== Data (max 150 baris pertama) =====")
        print(df_grouped.head(150))
        print("\n===== Total Keseluruhan Dari Data Terfilter =====")
        print(f"Total QTY   : {total_qty:,.1f}")
        print(f"Total VALUE : {total_value:,.0f}")

    @staticmethod
    def save_to_excel(df, total_qty, total_value):
        jawab = input("\nSimpan ke Excel? (ketik 'y' untuk Ya, selain itu untuk Tidak): ")
        if jawab.strip().lower() != "y":
            print("❌ Tidak disimpan ke Excel.")
            return

        nama_file = input("Masukkan nama file Excel (tanpa .xlsx): ")
        if not nama_file:
            nama_file = "hasil_export"
        full_path = f"{nama_file}.xlsx"

        with pd.ExcelWriter(full_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Data")
            workbook = writer.book
            worksheet = writer.sheets["Data"]

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

            last_row = len(df) + 2
            worksheet.write(last_row, 0, "TOTAL QTY")
            worksheet.write(last_row, 1, total_qty)
            worksheet.write(last_row + 1, 0, "TOTAL VALUE")
            worksheet.write(last_row + 1, 1, total_value)

        print(f"✅ Data berhasil disimpan ke: {full_path}")

# ======================
# --- Controller ---
# ======================
class Controller:
    def __init__(self, model, view, filter_dict, kolom_tampil):
        self.model = model
        self.view = view
        self.filter_dict = filter_dict
        self.kolom_tampil = kolom_tampil

    def run(self):
        self.model.load_data()
        df_filtered = self.model.apply_filter(self.filter_dict)

        if df_filtered is None or df_filtered.empty:
            print("❌ Tidak ada data terfilter.")
            return

        # Kolom numerik yang tidak boleh ikut group
        numeric_cols = ["QTY", "VALUE", "VALUE NETT"]

        # Kolom group valid = kolom tampil yang ada di dataframe, dan bukan kolom numerik
        kolom_group_valid = [k for k in self.kolom_tampil if k in df_filtered.columns and k not in numeric_cols]
        print("\n✅ Kolom group valid (tanpa kolom numerik):", kolom_group_valid)

        if not kolom_group_valid:
            print("❌ Tidak ada kolom grouping yang valid.")
            return

        df_grouped = df_filtered.groupby(kolom_group_valid, as_index=False)[["QTY", "VALUE"]].sum()

        df_grouped["QTY_fmt"] = df_grouped["QTY"].apply(lambda x: f"{x:,.1f}")
        df_grouped["VALUE_fmt"] = df_grouped["VALUE"].apply(lambda x: f"{x:,.0f}")

        kolom_final = kolom_group_valid + ["QTY_fmt", "VALUE_fmt"]
        df_display = df_grouped[kolom_final]

        total_qty = df_grouped["QTY"].sum()
        total_value = df_grouped["VALUE"].sum()

        self.view.view_terminal(df_display, total_qty, total_value)
        self.view.save_to_excel(df_display, total_qty, total_value)

# ======================
# --- Util Baca Filter & Kolom ---
# ======================
def baca_filter(file_filter):
    filter_dict = {}
    if not os.path.isfile(file_filter):
        print(f"❌ File {file_filter} tidak ditemukan.")
        exit()
    with open(file_filter, "r", encoding="utf-8") as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                v = value.strip()
                if "," in v:
                    filter_dict[key.strip()] = [i.strip() for i in v.split(",")]
                else:
                    filter_dict[key.strip()] = [v] if v else []
    return filter_dict

def baca_kolom(file_kolom):
    kolom_list = []
    if not os.path.isfile(file_kolom):
        print(f"❌ File {file_kolom} tidak ditemukan.")
        exit()
    with open(file_kolom, "r", encoding="utf-8") as f:
        for line in f:
            if "=" in line:
                key, val = line.strip().split("=", 1)
                if val.strip().upper() == "Y":
                    kolom_list.append(key.strip())
    return kolom_list

# ======================
# --- Main Program ---
# ======================
if __name__ == "__main__":
    versi_lokal = "1.0.0"
    gh = GitHelper(versi_lokal=versi_lokal)
    gh.cek_versi()

    lokasi_file_txt = "lokasi_dbase.txt"
    if not os.path.isfile(lokasi_file_txt):
        print(f"❌ File {lokasi_file_txt} tidak ditemukan.")
        exit()

    with open(lokasi_file_txt, "r", encoding="utf-8") as f:
        dbase_path = f.read().strip()

    filter_dict = baca_filter("filter.txt")
    kolom_tampil = baca_kolom("kolom.txt")

    model = DataModel(dbase_path)
    view = DataView()
    controller = Controller(model, view, filter_dict, kolom_tampil)

    controller.run()
