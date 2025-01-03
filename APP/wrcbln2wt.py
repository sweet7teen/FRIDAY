import pandas as pd
import os
import mysql.connector
import glob
import csv
from alive_progress import alive_bar
from time import sleep
from datetime import datetime
from pathlib import Path
import shutil
import subprocess

# Buka WRC BASE
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "../SRC/DB/wrcb_base.csv")
os.startfile(file_path)
input("Press Enter to Continue")

# Matikan peringatan SettingWithCopyWarning
pd.options.mode.chained_assignment = None

for file in glob.glob("../RES/WTR/*"):
    os.remove(file)
for file in glob.glob("../RES/HRREV/*"):
    os.remove(file)
for file in glob.glob("../RES/CSV/*"):
    os.remove(file)

# DB CONNECTION
host_smd = "172.31.68.51"
host_blg = "172.31.68.45"
host_trk = "172.31.68.57"
user = "cabang"
pw = "c@B4n9@wRc$d7"
db = "poscabang"

existing_data = []

path_rekon = "../RES/WTR/"

mydb_smd = mysql.connector.connect(host=host_smd, user=user, password=pw, database=db)

mydb_blg = mysql.connector.connect(host=host_blg, user=user, password=pw, database=db)

mydb_trk = mysql.connector.connect(host=host_trk, user=user, password=pw, database=db)

existing_data = []
# path_result = './result/'
path_dump = "../TEMP/WTR/"
hr_rev = "../RES/HRREV/"
path_csv = "../RES/CSV/"

with open("../SRC/DB/wrcb_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

print("Seleksi WT by Request")

for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    tgl = row[1]

    strdate = tgl[2:6]
    mondate = tgl[:4]
    table_names = "mstran_" + mondate
    # formatwt = 'WT'

    with alive_bar(
        10,
        title="Searching " + table_names + " Toko " + toko,
        bar="halloween",
        spinner="loving",
    ) as bar:
        for i in range(10):
            sleep(0.02)
            bar()
        mycursor_smd = mydb_smd.cursor()
        mycursor_blg = mydb_blg.cursor()
        mycursor_trk = mydb_trk.cursor()
        data_found = False  # Variabel penanda

        # Coba mencari data di host SMD
        mycursor_smd.execute(
            "SELECT shop,RECID,RTYPE,DOCNO,SEQNO,`DIV`,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,TOKO_1 DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM_IDM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PRDCD PLUMD,GROSS_JUAL,PRICE_JUAL,''KODE_SUPPLIER,DISC_05 DISC05,RATE_PPN,TIME(TGL1) JAM FROM %s WHERE SHOP='%s' AND DATE='%s' AND ISTYPE NOT IN ('V','RMB','GGC') order by RTYPE,DOCNO"
            % (table_names, toko, tgl)
        )

        df_smd = pd.DataFrame(
            mycursor_smd.fetchall(),
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

        df_selected_smd = df_smd[
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

        df_selected_smd["TGL1"] = df_selected_smd["TGL1"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )
        df_selected_smd["TGL2"] = df_selected_smd["TGL2"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )

        df_selected_smd["JAM"] = df_selected_smd["JAM"].astype(str).str[-8:]
        df_selected_smd["BKP"] = df_selected_smd["BKP"].replace(
            {".T.": "T", ".F.": "F"}
        )

        cek_smd = len(df_selected_smd)

        if not df_selected_smd.empty:
            dte = df_selected_smd["DATE"].astype(str).iloc[0]
            formatwt = "WT" + dte[2:6] + toko[:1] + "." + toko[1:4]
            df_selected_smd.to_csv(
                path_rekon + formatwt, index=False, header=True, sep="|"
            )
            data_found = True
        else:
            print(
                f"Tidak ada data cocok untuk table {table_names} dan toko {toko} di host SMD"
            )

        # Jika data sudah ditemukan di host BKU, lanjut ke toko berikutnya
        if data_found:
            continue

        # Coba mencari data di host BLG
        mycursor_blg.execute(
            "SELECT shop,RECID,RTYPE,DOCNO,SEQNO,`DIV`,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,TOKO_1 DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM_IDM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PRDCD PLUMD,GROSS_JUAL,PRICE_JUAL,''KODE_SUPPLIER,DISC_05 DISC05,RATE_PPN,TIME(TGL1) JAM FROM %s WHERE SHOP='%s' AND DATE='%s' AND ISTYPE NOT IN ('V','RMB','GGC') order by RTYPE,DOCNO"
            % (table_names, toko, tgl)
        )

        df_blg = pd.DataFrame(
            mycursor_blg.fetchall(),
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

        df_selected_blg = df_blg[
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

        df_selected_blg["TGL1"] = df_selected_blg["TGL1"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )
        df_selected_blg["TGL2"] = df_selected_blg["TGL2"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )

        df_selected_blg["JAM"] = df_selected_blg["JAM"].astype(str).str[-8:]
        df_selected_blg["BKP"] = df_selected_blg["BKP"].replace(
            {".T.": "T", ".F.": "F"}
        )

        cek_blg = len(df_selected_blg)

        if not df_selected_blg.empty:
            dte = df_selected_blg["DATE"].astype(str).iloc[0]
            formatwt = "WT" + dte[2:6] + toko[:1] + "." + toko[1:4]

            if cek_blg > 0:
                df_selected_blg.to_csv(
                    path_rekon + formatwt, index=False, header=True, sep="|"
                )
                data_found = True
        else:
            print(
                f"Tidak ada data cocok untuk table {table_names} dan toko {toko} di host SMD"
            )

        # Jika data sudah ditemukan di host BLG, lanjut ke toko berikutnya
        if data_found:
            continue

        # Coba mencari data di host BLG
        mycursor_trk.execute(
            "SELECT shop,RECID,RTYPE,DOCNO,SEQNO,`DIV`,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,TOKO_1 DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM_IDM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PRDCD PLUMD,GROSS_JUAL,PRICE_JUAL,''KODE_SUPPLIER,DISC_05 DISC05,RATE_PPN,TIME(TGL1) JAM FROM %s WHERE SHOP='%s' AND DATE='%s' AND ISTYPE NOT IN ('V','RMB','GGC') order by RTYPE,DOCNO"
            % (table_names, toko, tgl)
        )

        df_trk = pd.DataFrame(
            mycursor_trk.fetchall(),
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

        df_selected_trk = df_trk[
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

        df_selected_trk["TGL1"] = df_selected_trk["TGL1"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )
        df_selected_trk["TGL2"] = df_selected_trk["TGL2"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )

        df_selected_trk["JAM"] = df_selected_trk["JAM"].astype(str).str[-8:]
        df_selected_trk["BKP"] = df_selected_trk["BKP"].replace(
            {".T.": "T", ".F.": "F"}
        )

        cek_trk = len(df_selected_trk)

        if not df_selected_trk.empty:
            dte = df_selected_trk["DATE"].astype(str).iloc[0]
            formatwt = "WT" + dte[2:6] + toko[:1] + "." + toko[1:4]

            if cek_trk > 0:
                df_selected_trk.to_csv(
                    path_rekon + formatwt, index=False, header=True, sep="|"
                )
                data_found = True
        else:
            print(
                f"Tidak ada data cocok untuk table {table_names} dan toko {toko} di host SMD"
            )

        # Jika data sudah ditemukan di host BLG, lanjut ke toko berikutnya
        if data_found:
            continue

        # Jika tidak ada data yang cocok di kedua host, tampilkan pesan
        if not data_found:
            print(
                f"Tidak ada data cocok untuk table {table_names} dan toko {toko} di host BKU maupun BLG"
            )


print("Berhasil Eksport WT di folder WT-WRC!")


print()
#######################################################

for file in glob.glob("../RES/HRREV/WT*"):
    os.remove(file)

# print("Please Wait, Merging WT Files!")
# exec(open("../APP/merged.py").read())

# open explorer
relative_path = "../RES/WTR"
folder_path = os.path.abspath(relative_path)
os.system(f'explorer "{folder_path}"')
