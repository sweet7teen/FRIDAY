import pandas as pd
import os
import glob
import csv
import zipfile
from datetime import datetime
from pathlib import Path
import shutil

# Buka WT BASE
current_dir = os.getcwd()
file_path = os.path.normpath(os.path.join(current_dir, "../SRC/DB/wt_base.csv"))
os.startfile(file_path)

print()
print("Please Input Toko and Periode on CSV")
print()
input("Press Enter to Continue")

import time

start = time.time()


# cbg = input('Input Kode Cabang : ')

# now = datetime.now()
# year = now.strftime("%y")
# month = now.strftime("%m")
# day = now.strftime("%d")


for file in glob.glob("../RES/WTR/*"):
    os.remove(file)

for file in glob.glob("../RES/HR/*"):
    os.remove(file)

for file in glob.glob("../RES/CSV/*"):
    os.remove(file)


existing_data = []
with open("../SRC/DB/wt_base.csv", "r") as file:
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
                    found = True
                    break  # Exit the loop if the file is found
        if found:
            break  # Exit the loop if the file is found

    if not found:
        print(f"File not found: {hr}")
        not_found_files.append(hr)


print()

###################################

hr_result = "../RES/HR/"
file_list = os.listdir(hr_result)

# Loop through each file in the folder
for file_name in file_list:
    # Check if the file is a regular file (not a directory)
    if os.path.isfile(os.path.join(hr_result, file_name)):
        # Store the file name in a variable
        filename = os.path.splitext(file_name)[0]  # Remove the file extension

        hr = file_name
        firsthr = hr
        if firsthr.startswith("FR"):
            firsthr = "F"
        elif firsthr.startswith("HR"):
            firsthr = "T"
        else:
            firsthr = "R"
        wt = "WT" + hr[4:8] + firsthr + "." + hr[9:12]
        try:
            zf = zipfile.ZipFile(hr_result + hr)
            # df = pd.read_csv(zf.open(wt, 'r'), sep='|')
            df = zipfile.Path(hr_result + hr, wt)
            if df.exists():
                print(f"Extract {wt} -> {path_result}")
                df = pd.read_csv(zf.open(wt, "r"), sep="|")
                df.loc[df["FLAG_BO"] == "POT", "KETERANGAN"] = "NPT-T-"
                
                cek = len(df)
                if cek > 0:
                    df.to_csv(path_result + wt, index=False, sep="|")
                    shutil.copy(path_result + wt, folder_path)
                else:
                    continue
            else:
                print(f"File {wt} Tidak ada Pada File {hr}")
                continue

        except zipfile.BadZipfile as ex:
            print(f"File {hr} Is Not a Zip File")
            continue
        except FileNotFoundError as ex:
            print(f"File not found: {hr}")
            continue


# Print the names of files that were not found
print("\nFiles not found:")
for not_found_file in not_found_files:
    print(not_found_file)
print()
exec(open("merged.py").read())
print("Silahkan Cek File WT di Folder WT-RMB")
print()

# open explorer
relative_path = "../RES/WTR"
folder_path = os.path.abspath(relative_path)
os.system(f'explorer "{folder_path}"')

end = time.time()
elapsed_time = end - start

# Konversi waktu ke menit dan detik
minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Time Elapsed :", minutes, "Minutes", seconds, "Seconds")
