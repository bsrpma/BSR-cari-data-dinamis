import os
import pandas as pd
import requests
import threading

# ======================
# --- Cek Versi GitHub ---
# ======================
class GitHelper:
    url_version = "https://raw.githubusercontent.com/bsrpma/BSR-cari-data-dinamis/main/version.txt"
    url_script = "https://raw.githubusercontent.com/bsrpma/BSR-cari-data-dinamis/main/main.py"  # <- ganti sesuai script kamu
    nama_file_lokal = "main.py"

    def __init__(self, versi_lokal="1.0.0"):
        self.versi_lokal = versi_lokal

    def cek_versi(self):
        try:
            r = requests.get(self.url_version, timeout=5)
            r.raise_for_status()
            versi_online = r.text.strip()

            if versi_online != self.versi_lokal:
                print(f"⚠️ Versi baru tersedia: {versi_online} (lokal: {self.versi_lokal})")
                print("  [1] Download versi baru otomatis")
                print("  [2] Lanjut pakai versi sekarang")
                pilihan = input("Masukkan pilihan (1/2): ").strip()

                if pilihan == "1":
                    self.download_script()
                    print(f"✅ Script versi baru berhasil di-download sebagai '{self.nama_file_lokal}'")
                    print("💡 Silakan jalankan ulang script setelah update.")
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

            with open(self.nama_file_lokal, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"❌ Gagal download script: {e}")
            exit()

# ======================
# --- Pengaturan Pandas ---
# ======================
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# ======================
# --- Load Lokasi File ---
# ======================
lokasi_file_txt = "lokasi_dbase.txt"
if not os.path.isfile(lokasi_file_txt):
    print(f"❌ File {lokasi_file_txt} tidak ditemukan. Silakan buat file tersebut.")
    exit()

with open(lokasi_file_txt, "r", encoding="utf-8") as f:
    dbase_juni = f.read().strip()

loc_dbs_juni = os.path.abspath(dbase_juni)

# ======================
# --- Baca Filter.txt ---
# ======================
def baca_filter(file_filter):
    filter_dict = {}
    if not os.path.isfile(file_filter):
        print(f"❌ File {file_filter} tidak ditemukan. Silakan buat file tersebut.")
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

filter_dict_raw = baca_filter("filter.txt")

# ======================
# --- Baca Kolom.txt ---
# ======================
def baca_kolom(file_kolom):
    kolom_list = []
    if not os.path.isfile(file_kolom):
        print(f"❌ File {file_kolom} tidak ditemukan. Silakan buat file tersebut.")
        exit()

    with open(file_kolom, "r", encoding="utf-8") as f:
        for line in f:
            if "=" in line:
                key, val = line.strip().split("=", 1)
                if val.strip().upper() == "Y":
                    kolom_list.append(key.strip())
    return kolom_list

# ======================
# --- Fungsi Cari Data ---
# ======================
def cari_data(filter_dict, kolom_group, kolom_tampil):
    if not os.path.isfile(loc_dbs_juni):
        print(f"❌ dbase {dbase_juni} tidak ditemukan di {loc_dbs_juni}")
        return None, 0, 0

    df = pd.read_parquet(loc_dbs_juni)

    # print("\n===== Cek tipe data kolom sebelum filter =====")
    # print(df.dtypes)

    # --- Filter QTY per KODE OUTLET ---
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

    # --- Filter NAMA_SLS2_AWAL ---
    if "NAMA_SLS2_AWAL" in filter_dict and filter_dict["NAMA_SLS2_AWAL"]:
        prefix = filter_dict["NAMA_SLS2_AWAL"][0]
        if "NAMA SLS2" in df.columns:
            df = df[df["NAMA SLS2"].str.startswith(prefix, na=False)]

    # --- Filter kolom lain ---
    for kolom, nilai in filter_dict.items():
        if kolom in ["QTY", "NAMA_SLS2_AWAL"]:
            continue
        if kolom in df.columns and nilai:
            nilai_str = [str(v) for v in nilai]
            df = df[df[kolom].astype(str).isin(nilai_str)]
        elif kolom not in df.columns and nilai:
            print(f"⚠️ Kolom {kolom} tidak ditemukan, dilewati.")

    # --- Grouping ---
    kolom_group_valid = [k for k in kolom_tampil if k in df.columns]
    if not kolom_group_valid:
        print("❌ Tidak ada kolom grouping yang valid.")
        return None, 0, 0

    df_grouped = df.groupby(kolom_group_valid, as_index=False)[["QTY", "VALUE"]].sum()

    # --- Filter final di hasil group jika QTY diinginkan ---
    if "QTY" in filter_dict and filter_dict["QTY"]:
        filter_exp = filter_dict["QTY"][0]
        operator = filter_exp[:1]
        angka = float(filter_exp[1:])
        if operator == ">":
            df_grouped = df_grouped[df_grouped["QTY"] > angka]
        elif operator == "<":
            df_grouped = df_grouped[df_grouped["QTY"] < angka]

    df_grouped["QTY_fmt"] = df_grouped["QTY"].apply(lambda x: f"{x:,.1f}")
    df_grouped["VALUE_fmt"] = df_grouped["VALUE"].apply(lambda x: f"{x:,.0f}")

    kolom_final = kolom_group_valid + ["QTY_fmt", "VALUE_fmt"]
    df_display = df_grouped[kolom_final]

    total_qty = df_grouped["QTY"].sum()
    total_value = df_grouped["VALUE"].sum()

    print("\n===== Data (max 150 baris pertama) =====")
    print(df_display.head(150))

    print("\n===== Total Keseluruhan Dari Data Terfilter =====")
    print(f"Total QTY   : {total_qty:,.1f}")
    print(f"Total VALUE : {total_value:,.0f}")

    return df_display, total_qty, total_value

# ======================
# --- Simpan ke Excel ---
# ======================
def input_timeout(prompt, timeout=5):
    result = [None]

    def inner():
        result[0] = input(prompt)

    thread = threading.Thread(target=inner)
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        return None
    else:
        return result[0]
    
def simpan_ke_excel(df, total_qty, total_value):
    jawab = input_timeout("\nSimpan ke Excel? (ketik 'y' untuk Ya, selain itu untuk Tidak): ", timeout=5)
    if jawab is None or jawab.strip().lower() != "y":
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
# --- Main Program ---
# ======================
if __name__ == "__main__":
    versi_lokal = "1.0.0"
    gh = GitHelper(versi_lokal=versi_lokal)
    gh.cek_versi()

    kolom_file_txt = "kolom.txt"
    kolom_tampil_pilihan = baca_kolom(kolom_file_txt)
    kolom_group_pilihan = kolom_tampil_pilihan  # Boleh sama kalau mau

    df_hasil, total_qty, total_value = cari_data(filter_dict_raw, kolom_group_pilihan, kolom_tampil_pilihan)
    if df_hasil is not None and not df_hasil.empty:
        simpan_ke_excel(df_hasil, total_qty, total_value)
    else:
        print("❌ Tidak ada data terfilter, tidak disimpan.")