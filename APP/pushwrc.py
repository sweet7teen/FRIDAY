import os
import glob
import csv
import shutil

current_dir = os.getcwd()

for file in glob.glob("E:/mid/push wrc/result/*"):
    os.remove(file)

existing_data = []

with open("e:/MID/PUSH WRC/wt_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

hr_result = "e:/MID/PUSH WRC/RESULT/"

# Create a list to store the names of not found files
not_found_files = []

# List of folders to search
folders = ["E:/g050/backup/harian/", "E:/g241/backup/harian/", "E:/g242/backup/harian/"]

for i, row in enumerate(existing_data):
    if i == 0:
        continue

    toko = row[0]
    tanggal = row[1]

    periode = tanggal[:4]
    hr = toko[:1] + "R" + tanggal + "." + toko[1:]

    if hr.startswith("T"):
        hr = "HR" + tanggal + "." + toko[1:]
    elif hr.startswith("R"):
        hr = "CR" + tanggal + "." + toko[1:]
    elif hr.startswith("F"):
        hr = "FR" + tanggal + "." + toko[1:]

    found = False  # Flag to check if the file is found

    # Search through each specified folder
    for path_v in folders:
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
        if found:
            break  # Exit the loop if the file is found

    if not found:
        print(f"File not found: {hr}")
        not_found_files.append(hr)

print()
