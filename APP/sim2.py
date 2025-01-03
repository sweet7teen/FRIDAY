import glob
import os
import mysql.connector
import csv
import pandas as pd

periode = input("Input Periode Rekon : (YYMMDD) ")
table_wt = "wt_" + periode

# Hapus file yang ada
for file in glob.glob("../RES/BC-CSV/*"):
    os.remove(file)
for file in glob.glob("../RES/XLS/WRC_Result*"):
    os.remove(file)


def save_to_csv(data, file_name):
    if not data:
        print(f"No data to save for {file_name}")
        return
    keys = data[0].keys()
    output_path = os.path.join("../RES/BC-CSV/", file_name)
    with open(output_path, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


# DB CONNECTION
hosts = {"G050": "172.31.68.51", "G241": "172.31.68.45", "G242": "172.31.68.57"}
user = "cabang"
pw = "c@B4n9@wRc$d7"
db = "poscabang"

query = f"SELECT SHOP, DATE(TGL1) DATE, COUNT(prdcd) PRDCD, SUM(qty) QTY FROM {table_wt} GROUP BY SHOP;"

# Tempat untuk menyimpan semua hasil CSV
csv_files = []

# Proses setiap koneksi database
for key, host in hosts.items():
    print(f"Processing {key}")

    try:
        # Koneksi ke database
        mydb = mysql.connector.connect(host=host, user=user, password=pw, database=db)
        cursor = mydb.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()

        # Add CABANG column to each result
        for row in results:
            row["CABANG"] = key

        # Simpan hasil ke CSV
        file_name = f"{key}_result.csv"
        save_to_csv(results, file_name)
        csv_files.append(file_name)

        # Tutup koneksi
        cursor.close()
        mydb.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

print("CSV files generated:", csv_files)

# Merge semua file CSV ke dalam satu file Excel
excel_file = "../RES/XLS/WRC_Result.xlsx"
all_dataframes = [
    pd.read_csv(os.path.join("../RES/BC-CSV/", file)) for file in csv_files
]
merged_df = pd.concat(all_dataframes)
merged_df.to_excel(excel_file, index=False)
