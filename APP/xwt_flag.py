import pandas as pd
import os
import glob
import csv
from openpyxl import load_workbook

# Buka WT BASE
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "../SRC/DB/xwt_base.csv")
os.startfile(file_path)
input("Press Enter to Continue")

for file in glob.glob("../RES/WTR/*"):
    os.remove(file)
for file in glob.glob("../TEMP/WTR/*"):
    os.remove(file)
for file in glob.glob("../RES/XLS/wtgab.xls*"):
    os.remove(file)

# exec(open("wt.py").read())

existing_data = []
folder_path = "../RES/CSV/"
path_result = "../RES/WTR/"
path_dump = "../TEMP/WTR/"

with open("../SRC/DB/xwt_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)
print("Seleksi WT by Request")
files = os.listdir(folder_path)
for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    docno = row[1]
    rtype = row[2]

    data_found = False  # Flag to check if data is found

    for search in files:
        df = pd.read_csv(
            "../RES/CSV/" + search,
            names=[
                "RECID",
                "RTYPE",
                "DOCNO",
                "SEQNO",
                "DIV",
                "PRDCD",
                "QTY",
                "PRICE",
                "GROSS",
                "CTERM",
                "DOCNO2",
                "ISTYPE",
                "INVNO",
                "TOKO",
                "DATE",
                "DATE2",
                "KETERANGAN",
                "PTAG",
                "CAT_COD",
                "LOKASI",
                "TGL1",
                "TGL2",
                "PPN",
                "TOKO_1",
                "DATE3",
                "DOCNO3",
                "SHOP",
                "PRICE_IDM",
                "PPNBM_IDM",
                "PPNRP_IDM",
                "LT",
                "RAK",
                "BAR",
                "BKP",
                "SUB_BKP",
                "PLUMD",
                "GROSS_JUAL",
                "PRICE_JUAL",
                "KODE_SUPPLIER",
                "DISC05",
                "RATE_PPN",
                "JAM",
                "FLAG_BO",
            ],
            sep="|",
            header=None,
        )

        # Ubah kolom-kolom tertentu menjadi string
        string_columns = [
            "DOCNO",
            "SEQNO",
            "DIV",
            "PRDCD",
            "QTY",
            "PRICE",
            "GROSS",
            "CTERM",
            "DOCNO2",
            "ISTYPE",
            "INVNO",
            "DATE",
            "DATE2",
            "KETERANGAN",
            "PTAG",
            "CAT_COD",
            "LOKASI",
            "TGL1",
            "TGL2",
            "TOKO_1",
            "DATE3",
            "DOCNO3",
            "SHOP",
            "PRICE_IDM",
            "PPNBM_IDM",
            "LT",
            "RAK",
            "BAR",
            "BKP",
            "SUB_BKP",
            "PLUMD",
            "GROSS_JUAL",
            "PRICE_JUAL",
            "KODE_SUPPLIER",
            "DISC05",
            "RATE_PPN",
        ]

        df[string_columns] = df[string_columns].astype(str)
        # df["filename"] = search
        df = df.loc[(df["SHOP"].isin([toko]))]
        df = df.loc[(df["DOCNO"].isin([docno]))]
        df = df.loc[(df["RTYPE"].isin([rtype]))]

        # Modifikasi kolom KETERANGAN berdasarkan FLAG_BO
        df.loc[df["FLAG_BO"] == "POT", "KETERANGAN"] = "NPT-T-"

        # print(df)
        cek = len(df)
        if cek > 0:
            data_found = True  # Set flag to True if data is found

            format = rtype + "-" + docno + "-" + search
            lasttoko = format[6:]
            df.to_csv(path_dump + format, index=False, mode="a", sep="|", header=True)

            filer = os.listdir(path_dump)

            merge = []
            for csv_merge in filer:
                if csv_merge.endswith(lasttoko):
                    df = pd.read_csv(
                        path_dump + csv_merge,
                        sep="|",
                        dtype={
                            "DOCNO": str,
                            "SEQNO": str,
                            "DIV": str,
                            "PRDCD": str,
                            "QTY": str,
                            "PRICE": str,
                            "GROSS": str,
                            "CTERM": str,
                            "DOCNO2": str,
                            "ISTYPE": str,
                            "INVNO": str,
                            "DATE": str,
                            "DATE2": str,
                            "KETERANGAN": str,
                            "PTAG": str,
                            "CAT_COD": str,
                            "LOKASI": str,
                            "TGL1": str,
                            "TGL2": str,
                            "TOKO_1": str,
                            "DATE3": str,
                            "DOCNO3": str,
                            "SHOP": str,
                            "PRICE_IDM": str,
                            "PPNBM_IDM": str,
                            "LT": str,
                            "RAK": str,
                            "BAR": str,
                            "BKP": str,
                            "SUB_BKP": str,
                            "PLUMD": str,
                            "GROSS_JUAL": str,
                            "PRICE_JUAL": str,
                            "KODE_SUPPLIER": str,
                            "DISC05": str,
                            "RATE_PPN": str,
                        },
                    )
                    merge.append(df)

                    merged_csv = pd.concat(merge, ignore_index=True)
                    merged_csv.to_csv(path_result + search, sep="|", index=False)

    if not data_found:  # Check if data was not found for the current row
        print(f"Data untuk toko {toko}, docno {docno}, rtype {rtype} tidak ditemukan.")

# filer = os.listdir(path_result)

# merge = []
# for csv_merge in filer:
#     df = pd.read_csv(path_result + csv_merge, sep="|")
#     merge.append(df)

# merged_csv = pd.concat(merge)
# merged_csv.to_excel("../RES/XLS/wtgab.xlsx", index=False)
print("Silahkan Cek Hasil WT Seleksi di Folder Result")
# open explorer
relative_path = "../RES/WTR"
folder_path = os.path.abspath(relative_path)
os.system(f'explorer "{folder_path}"')
