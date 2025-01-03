import mysql.connector
from mysql.connector import Error
import csv
import subprocess
import pandas as pd
import json
import os
import glob

for file in glob.glob("../RES/BC-CSV/*"):
    os.remove(file)
for file in glob.glob("../RES/XLS/TK_result*"):
    os.remove(file)

periode = input("Input Periode Rekon : (YYMMDD)")
table_wt = "wt_" + periode


# Fungsi untuk mengecek apakah IP dapat dijangkau dan menangkap pesan DHU/DNU
def check_ping(ip):
    try:
        result = subprocess.run(
            ["ping", "-n", "1", ip], capture_output=True, text=True, timeout=3
        )
        output = result.stdout.lower()

        if (
            "destination host unreachable" in output
            or "destination net unreachable" in output
        ):
            print(f"IP {ip}: Destination unreachable (DHU/DNU).")
            return False
        elif "time=" in output:
            print(f"IP {ip} reachable.")
            return True
        else:
            print(f"IP {ip}: No valid response.")
            return False
    except subprocess.TimeoutExpired:
        print(f"IP {ip}: Ping timed out.")
        return False


# Fungsi untuk mencoba login ke database
def try_login(ip, password):
    try:
        connection = mysql.connector.connect(
            host=ip, user="root", password=password, database="pos", charset="utf8"
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to database {ip} with password: {e}")
        return None


# Fungsi untuk mengeksekusi query SELECT dan menyimpan hasil
def execute_query(connection, query):
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result


# Fungsi untuk menyimpan hasil query ke CSV di folder ../RES/BC-CSV/
def save_to_csv(data, file_name):
    keys = data[0].keys()
    output_path = os.path.join("../RES/BC-CSV/", file_name)
    with open(output_path, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


# Daftar list cabang
SMD = ["G050", "G241", "G242"]
BMS = ["G092"]
PTK = ["G245"]

# Ambil input dari user
print("Pilih cabang yang ingin dieksekusi:")
print("1. SMD")
print("2. BMS")
print("3. PTK")
choice = input("Masukkan pilihan (1/2/3): ")

# Pilih list berdasarkan input
if choice == "1":
    selected_list = SMD
elif choice == "2":
    selected_list = BMS
elif choice == "3":
    selected_list = PTK
else:
    print("Pilihan tidak valid.")
    exit()

# Load data dari file JSON
session_file = "../RES/JSON/session_pass.json"
with open(session_file, "r") as f:
    data = json.load(f)

# Daftar password
passwords = [
    "iczmGcFJe//jBPZPGPFz0qTFHmiKHou6I=JFw32YgFQP",
    "nPx1FgDLqho0FYBbnAuFazlr4gl0vgD94=wL3sip88JJ",
    "giTk2Yr0K9VV5nBKzn22JafQt9iUiuQ3A=3soy2lSa1t",
]

# Query yang ingin dijalankan
query = f"SELECT (select toko from toko) as SHOP, DATE(bukti_tgl) DATE, COUNT(prdcd) PRDCD, SUM(qty) QTY, (SELECT `desc` FROM const WHERE rkey='bpb') CABANG FROM mstran WHERE istype NOT IN ('GGC','RMB') AND date(bukti_tgl)='{periode}' AND bukti_tgl<CURDATE()  AND qty!=0 GROUP BY DATE(bukti_tgl) "  # Ganti dengan query yang sesuai

print()
print("SQL Query")
print(query)
print()

# Urutkan data berdasarkan kdtk
sorted_data = sorted(data, key=lambda x: x["kdtk"])

# Tempat untuk menyimpan semua hasil CSV
csv_files = []

# Proses login dan eksekusi query satu per satu setelah ping
for entry in sorted_data:
    if entry["cabang"] in selected_list:
        ip = entry["ip"]
        kdtk = entry["kdtk"]
        cbg = entry["cabang"]

        # Ping dulu sebelum konek ke database
        if check_ping(ip):
            found_password = None

            # Cek apakah sudah ada password yang tersimpan untuk IP ini
            for session in data:
                if session["ip"] == ip:
                    found_password = session.get("pw")
                    break

            # Kalau ada password yang tersimpan, langsung pakai
            if found_password:
                connection = try_login(ip, found_password)
                if connection:
                    try:
                        print(f"Connected to {kdtk} - {ip} with saved password")
                        result = execute_query(connection, query)

                        # Simpan hasil ke CSV di folder ../RES/BC-CSV/
                        file_name = f"{kdtk}_{ip}_result.csv"
                        save_to_csv(result, file_name)
                        csv_files.append(file_name)

                        connection.close()
                    except Exception as e:
                        print(f"Error executing query for {ip}: {e}")
                else:
                    print(f"Failed to connect to {kdtk} - {ip} with saved password")

            # Kalau ga ada password, coba satu per satu dari list passwords
            else:
                for password in passwords:
                    connection = try_login(ip, password)
                    if connection:
                        try:
                            print(f"Connected to {kdtk} - {ip} with password")
                            result = execute_query(connection, query)

                            # Simpan hasil ke CSV di folder ../RES/BC-CSV/
                            file_name = f"{kdtk}_{ip}_result.csv"
                            save_to_csv(result, file_name)
                            csv_files.append(file_name)

                            connection.close()

                            # Update JSON dengan password yang berhasil
                            for session in data:
                                if session["ip"] == ip:
                                    session["pw"] = password

                            with open(session_file, "w") as f:
                                json.dump(data, f, indent=4)
                            break  # Berhenti coba password lain jika sudah berhasil login
                        except Exception as e:
                            print(f"Error executing query for {kdtk} - {ip}: {e}")
                    else:
                        print(f"Failed to connect to {kdtk} - {ip} with password")
        else:
            print(f"Skipping {kdtk} - {ip} (unreachable).")

# Merge semua file CSV ke dalam satu file Excel
excel_file = "../RES/XLS/TK_Result.xlsx"
all_dataframes = [
    pd.read_csv(os.path.join("../RES/BC-CSV/", file)) for file in csv_files
]
merged_df = pd.concat(all_dataframes)
merged_df.to_excel(excel_file, index=False)
