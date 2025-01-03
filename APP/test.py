import mysql.connector
import pandas as pd
import glob
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

# Daftar password untuk database toko
passwords = [
    "nPx1FgDLqho0FYBbnAuFazlr4gl0vgD94=wL3sip88JJ",
    "giTk2Yr0K9VV5nBKzn22JafQt9iUiuQ3A=3soy2lSa1t",
    "v4dg4IDbVLYJnB7zOv3lKg8jw8WPvrwd4=NqpoGGrLCX",
    "WiA8E/q8aS/5NOgQE5ZobzpOCn3IhyKzE=ofv9imr2vN",    
    "o8hz+tWOpAX9euH9ZAZR6oobF9XYpAqGg=aiCtc1lS+l",
    "5CCNQV3rio/dI/iboPPnww9nzUHh8bpac=fU59bpWfE4",
    "fH+cu2vIZa3QD+UCfrWUL1fKhBsEFgCzM=PsoRr0pMwj"
]

# Fungsi untuk membersihkan file CSV dan XLS yang ada di direktori tertentu
def clean_directory(directory, file_pattern):
    for file in glob.glob(os.path.join(directory, file_pattern)):
        os.remove(file)

# Membersihkan file di direktori ../RES/CSV/ dan ../RES/XLS/
clean_directory("../RES/CSV/", "hasil_query*")
clean_directory("../RES/XLS/", "hasil*")
clean_directory("../RES/BC-CSV/", "hasil*")

# Fungsi untuk koneksi ke database
def connect_to_db(host, user, passwd):
    try:
        return mysql.connector.connect(
            host=host,
            user=user,
            password=passwd,
            charset='utf8'
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

# Fungsi untuk membaca query dari file teks
def read_query_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Fungsi untuk ping IP address
def ping_ip(ip):
    try:
        output = subprocess.check_output(["ping", "-n", "1", "-w", "1000", ip], stderr=subprocess.STDOUT, universal_newlines=True)
        if "bytes" in output and "Destination" not in output and "Request timed out" not in output:
            return True
    except subprocess.CalledProcessError:
        pass
    return False

# Fungsi untuk eksekusi query BC per toko
def process_toko(toko, passwords, bc_query):
    db_ip = toko[0]
    db_user = "root"
    toko_name = toko[1]
    status = "NOK"

    if ping_ip(db_ip):
        db_conn = None

        # Mencoba koneksi dengan setiap password yang tersedia
        for password in passwords:
            print(f"Trying to connect to database for toko {toko_name}")
            db_conn = connect_to_db(db_ip, db_user, password)
            if db_conn:
                status = "OK"
                break  # Berhenti mencoba password jika koneksi berhasil

        if db_conn is not None:
            try:
                db_cursor = db_conn.cursor()
                db_cursor.execute("USE pos")
                db_cursor.execute(bc_query)
                result = db_cursor.fetchall()

                # Menentukan nama kolom
                column_names = [i[0] for i in db_cursor.description]

                # Membuat dataframe dari hasil query
                df = pd.DataFrame(result, columns=column_names)

                # Simpan dataframe ke file CSV temporary
                csv_file_path = f"../RES/BC-CSV/hasil_query_toko_{toko_name}.csv"
                df.to_csv(csv_file_path, index=False)

                return csv_file_path, toko_name, status
            except mysql.connector.Error as err:
                print(f"Error executing query for toko {toko_name}: {err}")
            finally:
                if db_conn.is_connected():
                    db_cursor.close()
                    db_conn.close()
        else:
            print(f"Failed to connect to database for toko {toko_name}")
    else:
        print(f"Ping to {db_ip} failed for toko {toko_name}")

    return None, toko_name, status

# Koneksi ke database master
master_db = mysql.connector.connect(
    host="192.168.26.78",
    user="root"
)

# Buka file query master
current_dir = os.getcwd()
file_path = os.path.join(current_dir, '../SRC/TXT/querymaster.txt')
os.startfile(file_path)

print("\nPlease Insert Query on Notepad\n")
input("Press Enter to Continue")

# Membaca query master dari file teks
master_query = read_query_from_file('../SRC/TXT/querymaster.txt')

cursor = master_db.cursor()
cursor.execute("USE lokal")
cursor.execute(master_query)
list_toko = cursor.fetchall()

# Buka file query BC
file_path = os.path.join(current_dir, '../SRC/TXT/querybc.txt')
os.startfile(file_path)

print("\nPlease Insert Query on Notepad\n")
input("Press Enter to Continue")

# Membaca query BC dari file teks
bc_query = read_query_from_file('../SRC/TXT/querybc.txt')

csv_files = []
log_entries = []

# Menggunakan ThreadPoolExecutor untuk paralelisasi proses query
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_toko, toko, passwords, bc_query) for toko in list_toko]
    for future in as_completed(futures):
        csv_file_path, toko_name, status = future.result()
        log_entries.append(f"{toko_name},{status}")
        if csv_file_path is not None:
            csv_files.append(csv_file_path)

# Gabungkan semua file CSV menjadi satu DataFrame
if csv_files:
    dataframes = [pd.read_csv(csv_file) for csv_file in csv_files]
    combined_df = pd.concat(dataframes, ignore_index=True)

    print("Eksport Excel")
    # Simpan hasil gabungan ke file Excel dengan satu sheet
    combined_df.to_excel('../RES/XLS/hasil_query_toko.xlsx', index=False, sheet_name='AllData')

# Simpan log ke file
with open('../RES/TXT/log.txt', 'w') as log_file:
    log_file.write("TOKO,STATUS\n")
    log_file.write("\n".join(log_entries))

master_db.close()

print("Proses selesai, hasil sudah disimpan di '../RES/XLS/hasil_query_toko.xlsx'")
print("Log sudah disimpan di '../RES/log.txt'")
