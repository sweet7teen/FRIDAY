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
file_path = os.path.join(current_dir, "../SRC/DB/wrc_base.csv")
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

with open("../SRC/DB/wrc_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

print("Seleksi WT by Request")

for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    tgl = row[1]

    table_names = "wt_" + tgl
    strdate = tgl[2:6]
    formatwt = "WT" + strdate + toko[:1] + "." + toko[1:4]

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
            "SELECT shop,RECID,RTYPE,BUKTI_NO,SEQNO,DIVISI,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE_2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PLUMD,GROSS_JUAL,PRICE_JUAL,KODE_SUPPLIER,DISC05,RATE_PPN,JAM,FLAG_BO FROM %s WHERE SHOP='%s' order by RTYPE,BUKTI_NO"
            % (table_names, toko)
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
                "FLAG_BO",
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
                "FLAG_BO",
            ]
        ]

        df_selected_smd["TGL1"] = df_selected_smd["TGL1"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )
        df_selected_smd["TGL2"] = df_selected_smd["TGL2"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )

        df_selected_smd["JAM"] = df_selected_smd["JAM"].astype(str).str[-8:]
        df_selected_smd["BKP"] = df_selected_smd["BKP"].replace({"Y": "T", "N": "F"})

        cek_smd = len(df_selected_smd)

        if cek_smd > 0:
            df_selected_smd.to_csv(
                path_rekon + formatwt, index=False, header=True, sep="|"
            )
            data_found = True

        # Jika data sudah ditemukan di host BKU, lanjut ke toko berikutnya
        if data_found:
            continue

        # Coba mencari data di host BLG
        mycursor_blg.execute(
            "SELECT shop,RECID,RTYPE,BUKTI_NO,SEQNO,DIVISI,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE_2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PLUMD,GROSS_JUAL,PRICE_JUAL,KODE_SUPPLIER,DISC05,RATE_PPN,JAM,FLAG_BO FROM %s WHERE SHOP='%s' order by RTYPE,BUKTI_NO"
            % (table_names, toko)
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
                "FLAG_BO",
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
                "FLAG_BO",
            ]
        ]

        df_selected_blg["TGL1"] = df_selected_blg["TGL1"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )
        df_selected_blg["TGL2"] = df_selected_blg["TGL2"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )

        df_selected_blg["JAM"] = df_selected_blg["JAM"].astype(str).str[-8:]
        df_selected_blg["BKP"] = df_selected_blg["BKP"].replace({"Y": "T", "N": "F"})

        cek_blg = len(df_selected_blg)
        if cek_blg > 0:
            df_selected_blg.to_csv(
                path_rekon + formatwt, index=False, header=True, sep="|"
            )

            data_found = True

        # Jika data sudah ditemukan di host BLG, lanjut ke toko berikutnya
        if data_found:
            continue

        # Coba mencari data di host BLG
        mycursor_trk.execute(
            "SELECT shop,RECID,RTYPE,BUKTI_NO,SEQNO,DIVISI,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE_2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PLUMD,GROSS_JUAL,PRICE_JUAL,KODE_SUPPLIER,DISC05,RATE_PPN,JAM,FLAG_BO FROM %s WHERE SHOP='%s' order by RTYPE,BUKTI_NO"
            % (table_names, toko)
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
                "FLAG_BO",
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
                "FLAG_BO",
            ]
        ]

        df_selected_trk["TGL1"] = df_selected_trk["TGL1"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )
        df_selected_trk["TGL2"] = df_selected_trk["TGL2"].apply(
            lambda x: x.strftime("%d-%m-%Y") if not pd.isnull(x) else x
        )

        df_selected_trk["JAM"] = df_selected_trk["JAM"].astype(str).str[-8:]
        df_selected_trk["BKP"] = df_selected_trk["BKP"].replace({"Y": "T", "N": "F"})

        cek_trk = len(df_selected_trk)
        if cek_trk > 0:
            df_selected_trk.to_csv(
                path_rekon + formatwt, index=False, header=True, sep="|"
            )

            data_found = True

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

for file in glob.glob("../RES/HR/*"):
    os.remove(file)

existing_data = []
with open("../SRC/DB/wrc_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

folder_path = "../RES/CSV/"
path_result = "../RES/WTR/"
hr_result = "../RES/HR/"

# Create a list to store the names of not found files
not_found_files = []

for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    tanggal = row[1]

    periode = tanggal[:4]
    wt = "WT" + tanggal[2:] + toko[:1] + "." + toko[1:]
    hr = toko[:1] + "R" + tanggal + "." + toko[1:]

    if hr.startswith("T"):
        hr = "HR" + tanggal + "." + toko[1:]
    elif hr.startswith("R"):
        hr = "CR" + tanggal + "." + toko[1:]
    elif hr.startswith("F"):
        hr = "FR" + tanggal + "." + toko[1:]

    path_v = "V:\\DTHR\\HR" + periode
    # path_hr = 'v:/dthr/hr'+periode+'/'+cbg+'/'
    # path = Path(path_hr+hr)

    found = False  # Flag to check if the file is found

    for root, dirs, files in os.walk(path_v):
        for name in files:
            if name.startswith(hr):
                source_file = os.path.join(root, hr)
                if os.path.exists(source_file):
                    print(f"Copying: {source_file} -> {hr_result}")
                    shutil.copy(source_file, hr_result)
                    shutil.copy(source_file, hr_rev)
                    found = True
                    break  # Exit the loop if the file is found
        if found:
            break  # Exit the loop if the file is found

    if not found:
        print(f"File not found: {hr}")
        not_found_files.append(hr)

print()

#####################################################################
existing_data = []

with open("../SRC/DB/wrc_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

folder_path = "../RES/CSV/"
path_result = "../RES/WTR/"
hr_result = "../RES/HR/"

# Create a list to store the names of not found files
not_found_files = []

for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    tanggal = row[1]

    periode = tanggal[:4]
    wt = "WT" + tanggal[2:] + toko[:1] + "." + toko[1:]
    hr = toko[:1] + "R" + tanggal + "." + toko[1:]

    if hr.startswith("T"):
        hr = "HR" + tanggal + "." + toko[1:]
    elif hr.startswith("R"):
        hr = "CR" + tanggal + "." + toko[1:]
    elif hr.startswith("F"):
        hr = "FR" + tanggal + "." + toko[1:]

    path_v = "V:\\DTHR\\HR" + periode

    table_names = "wt_" + tanggal
    strdate = tanggal[2:6]
    formatwt = "WT" + strdate + toko[:1] + "." + toko[1:4]

    src_file = path_rekon + formatwt
    if os.path.exists(src_file):
        try:
            shutil.copy(src_file, hr_rev)
            shutil.copy(src_file, path_csv)
            # print(f"File '{src_file}' berhasil dicopy ke '{hr_rev}'.")
        except Exception as e:
            print(f"Error saat mencopy file: {e}")
    else:
        print(f"File '{src_file}' tidak ditemukan. Skipping...")
    
    # shutil.copy(path_rekon + formatwt, hr_rev)
    # shutil.copy(path_rekon + formatwt, path_csv)

    # Define the paths to the existing zip file and the new file
    existing_zip_file = "../RES/HRREV/" + hr
    new_file_to_add = "../RES/HRREV/" + formatwt

    # Define the name of the file to be replaced within the zip archive
    file_to_replace = formatwt

    # Run the 7z command to update the zip archive
    command = ["7z", "u", existing_zip_file, new_file_to_add]

    with open("../RES/TXT/wrc2wt-log.txt", "a") as log_file:
        if os.path.exists(file_to_replace):
            try:
                # subprocess.run(command, check=True)
                process = subprocess.run(command, stdout=log_file, stderr=log_file)
                print(
                    f"File '{file_to_replace}' Zipped to '{existing_zip_file}' successfully."
                )
            except subprocess.CalledProcessError as e:
                print(f"Error executing the command: {e}")
        else:
            print(f"File '{file_to_replace}' not found. Skipping...")


for file in glob.glob("../RES/HRREV/WT*"):
    os.remove(file)

print("Please Wait, Merging WT Files!")
# exec(open("../APP/merged.py").read())

# open explorer
relative_path = "../RES/WTR"
folder_path = os.path.abspath(relative_path)
os.system(f'explorer "{folder_path}"')
