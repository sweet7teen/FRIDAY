import pandas as pd
import os
import mysql.connector
import glob
import csv
from alive_progress import alive_bar
from time import sleep

# Bersihkan folder RES dari file sebelumnya
folders_to_clean = ["../RES/WTR/*", "../RES/HRREV/*", "../RES/CSV/*"]
for folder in folders_to_clean:
    for file in glob.glob(folder):
        os.remove(file)


# Buka file WRC BASE
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "../SRC/DB/wrcb_base.csv")
os.startfile(file_path)
input("Press Enter to Continue")

# Matikan peringatan SettingWithCopyWarning
pd.options.mode.chained_assignment = None

# DB CONNECTION
hosts = {"SMD": "172.31.68.51", "BLG": "172.31.68.45", "TRK": "172.31.68.57"}
user = "cabang"
pw = "c@B4n9@wRc$d7"
db = "poscabang"

connections = {
    "SMD": mysql.connector.connect(
        host=hosts["SMD"], user=user, password=pw, database=db
    ),
    "BLG": mysql.connector.connect(
        host=hosts["BLG"], user=user, password=pw, database=db
    ),
    "TRK": mysql.connector.connect(
        host=hosts["TRK"], user=user, password=pw, database=db
    ),
}


# Fungsi untuk query ke database
def query_database(cabang, toko, tgl, table_name):
    cursor = connections[cabang].cursor()
    query = f"""
        SELECT shop,RECID,RTYPE,DOCNO,SEQNO,`DIV`,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,TOKO_1 DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM_IDM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PRDCD PLUMD,GROSS_JUAL,PRICE_JUAL,''KODE_SUPPLIER,DISC_05 DISC05,RATE_PPN,TIME(TGL1) JAM FROM {table_name} WHERE SHOP='{toko}' AND DATE='{tgl}' AND ISTYPE NOT IN ('V','RMB','GGC') ORDER BY RTYPE, DOCNO
    """
    cursor.execute(query)
    result = cursor.fetchall()
    dfs = pd.DataFrame(
        result,
        columns=[
            "shop",
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
        ],
    )
    df = dfs[
        [
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
        ]
    ]

    df["TGL1"] = df["TGL1"].apply(
        lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
    )
    df["TGL2"] = df["TGL2"].apply(
        lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
    )

    df["JAM"] = df["JAM"].astype(str).str[-8:]
    df["BKP"] = df["BKP"].replace({".T.": "T", ".F.": "F"})

    return df


# Baca file CSV dari SRC DB
with open("../SRC/DB/wrcb_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

print("Seleksi WT by Request")

# Loop untuk memproses data dari CSV
for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    tgl = row[1]
    mondate = tgl[:4]  # Ambil 4 digit pertama dari tanggal
    table_names = "mstran_" + mondate  # Nama table berdasarkan bulan
    data_found = False  # Variabel penanda apakah data ditemukan

    # Progress bar
    with alive_bar(
        10,
        title="Searching " + table_names + " Toko " + toko,
        bar="halloween",
        spinner="loving",
    ) as bar:
        for _ in range(10):
            sleep(0.02)
            bar()

        for cabang in ["SMD", "BLG", "TRK"]:
            df = query_database(cabang, toko, tgl, table_names)
            if not df.empty:
                dte = df["DATE"].astype(str).iloc[0]
                formatwt = "WT" + dte[2:6] + toko[:1] + "." + toko[1:4]
                df.to_csv(f"../RES/WTR/{formatwt}", index=False, header=True, sep="|")
                data_found = True
                break

        if not data_found:
            print(f"Tidak ada data cocok untuk table {table_names} dan toko {toko}")
