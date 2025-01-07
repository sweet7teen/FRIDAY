import mysql.connector.pooling
import pandas as pd
import os
import glob
import csv
import shutil
import subprocess

# Buka WRC BASE
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "../SRC/DB/wt_base.csv")
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

path_dump = "../TEMP/WTR/"
hr_rev = "../RES/HRREV/"
path_csv = "../RES/CSV/"
path_rekon = "../RES/WTR/"


# Inisialisasi pool koneksi MySQL
def initialize_connection_pool(
    host, user, password, database, pool_name="mypool", pool_size=5
):
    try:
        # Konfigurasi pool koneksi
        dbconfig = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "pool_name": pool_name,
            "pool_size": pool_size,
        }

        # Inisialisasi pool koneksi
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(**dbconfig)
        print("Connection pool created successfully!")
        return connection_pool

    except mysql.connector.Error as error:
        print("Error initializing connection pool:", error)
        return None


# Mengambil koneksi dari pool
def get_connection_from_pool(pool):
    try:
        connection = pool.get_connection()
        print("Connection retrieved from pool.")
        return connection

    except mysql.connector.Error as error:
        print("Error getting connection from pool:", error)
        return None


# Mengembalikan koneksi ke pool
def release_connection_to_pool(connection):
    try:
        connection.close()
        print("Connection released back to pool.")
        print("")

    except mysql.connector.Error as error:
        print("Error releasing connection to pool:", error)


# Fungsi untuk membaca data dari database lokal
def read_local_database(host, user, password, database):
    try:
        # Inisialisasi pool koneksi
        connection_pool = initialize_connection_pool(host, user, password, database)

        # Mengambil koneksi dari pool
        connection = get_connection_from_pool(connection_pool)

        # Membuat cursor untuk eksekusi query
        cursor = connection.cursor()

        # Query untuk membaca data IP dan KDTK
        query = "SELECT IP, KDTK FROM mtoko"

        # Eksekusi query
        cursor.execute(query)

        # Mengambil semua hasil query
        records = cursor.fetchall()

        # Menutup cursor
        cursor.close()

        # Mengembalikan koneksi ke pool
        release_connection_to_pool(connection)

        # Mengembalikan hasil query
        return records

    except mysql.connector.Error as error:
        print("Error reading local database:", error)
        return []


# Fungsi untuk membaca KDTK dari file CSV
def read_kdtks_from_csv(filename, column=0):
    try:
        kdtks = []

        with open(filename, mode="r") as file:
            # Skip header
            next(file)

            # Baca setiap baris dari CSV
            for line in file:
                # Split baris menjadi kolom
                columns = line.strip().split(",")

                # Ambil kolom sesuai parameter
                data = columns[column]

                # Tambahkan data ke list
                kdtks.append(data)

        return kdtks

    except Exception as e:
        print("Error reading data from CSV:", e)
        return []


# Fungsi untuk mencari IP berdasarkan KDTK
def find_ip_by_kdtk(data, kdtk):
    for record in data:
        ip, k = record
        if k == kdtk:
            return ip
    return None


# Fungsi untuk melakukan koneksi ke MySQL server
def connect_to_mysql(ip, password, formatwt, tanggal):
    try:
        # Koneksi ke MySQL server
        connection = mysql.connector.connect(
            host=ip, user="root", password=password, database="pos", charset="utf8"
        )

        # Cek apakah koneksi berhasil
        if connection.is_connected():
            print("Connected to ", kdtk, "-", ip)
            print("Getting File", formatwt)
            conn = connection.cursor()

            conn.execute(
                "SELECT a.RECID,IF(a.RTYPE='BPB','B',a.RTYPE) AS RTYPE,a.BUKTI_NO AS DOCNO,IF(a.RTYPE='X',1,0) AS SEQNO,b.DEPART AS `DIV`,a.PRDCD,a.QTY,a.PRICE,a.GROSS,a.CR_TERM AS CTERM,IF(a.INVNO='' OR a.INVNO IS NULL,'0',a.INVNO) AS DOCNO2,a.ISTYPE,IF(a.PO_NO='' OR a.PO_NO IS NULL,a.INVNO,a.PO_NO) AS INVNO,IF(a.GUDANG='' AND a.SUPCO IS NULL ,(SELECT toko FROM toko),IF((a.SUPCO<>NULL) OR (a.SUPCO<>''),a.SUPCO,IF(a.GUDANG<>'' OR a.GUDANG<>NULL,a.GUDANG,(SELECT KIRIM FROM toko)))) AS TOKO,CONCAT(SUBSTR(a.bukti_tgl,3,2),SUBSTR(a.bukti_tgl,6,2),SUBSTR(a.bukti_tgl,9,2)) AS 'DATE','0' AS DATE2,a.KETER AS KETERANGAN,b.PTAG,b.CAT_COD,a.LOKASI,DATE_FORMAT(SUBSTR(a.BUKTI_TGL,1,10),'%%d-%%m-%%Y') AS TGL1,DATE_FORMAT(a.INV_DATE,'%%d-%%m-%%Y') AS TGL2,a.PPN,'' AS TOKO_1,'' AS DATE3,0 AS DOCNO3,(SELECT toko FROM toko) AS SHOP,a.PRICE_IDM,0 AS PPNBM_IDM,a.PPN_RP_IDM AS PPNRP_IDM,a.LT,a.RAK,a.BAR,IF(a.BKP='Y','T','F') AS BKP,IF(a.SUB_BKP IS NULL,'N',a.SUB_BKP) AS SUB_BKP,a.PLU_NAS AS PLUMD,a.GROSS_JUAL,a.PRICE_JUAL,0 AS KODE_SUPPLIER,a.DISC_05 AS DISC05,IF(a.PPN_RATE IS NULL,0,a.PPN_RATE) AS RATE_PPN,a.JAM,IF(b.FLAGPROD LIKE '%%POT=Y%%','POT','') AS FLAG_BO FROM mstran a LEFT JOIN prodmast b ON a.prdcd=b.prdcd WHERE a.bukti_tgl LIKE '%%%s%%' AND a.ISTYPE<>'RMB' ORDER BY a.BUKTI_NO,a.RTYPE,a.PRDCD"
                % (tanggal)
            )

            df = pd.DataFrame(
                conn.fetchall(),
                columns=[
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

            df_select = df[
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

            # Mengonversi nilai bytearray menjadi string pada kolom DATE
            df_select["DATE"] = df_select["DATE"].apply(lambda x: str(x, "utf-8"))
            df_select["JAM"] = df_select["JAM"].astype(str).str[-8:]

            df_select.loc[df["FLAG_BO"] == "POT", "KETERANGAN"] = "NPT-T-"

            cek = len(df_select)

            if cek > 0:
                df_select.to_csv(
                    path_rekon + formatwt, index=False, header=True, sep="|"
                )

            connection.close()  # Tutup koneksi setelah berhasil terhubung
            return True

    except mysql.connector.Error as error:
        print(
            "Error connecting to ", kdtk, " server at", ip, "with password:", password
        )
        print("MySQL Error:", error)

    return False


# Contoh penggunaan
if __name__ == "__main__":
    # Pengaturan database lokal
    host_local = "192.168.26.78"
    user_local = "root"
    password_local = ""
    database_local = "lokal"

    # Baca data dari database lokal
    data_local = read_local_database(
        host_local, user_local, password_local, database_local
    )

    # Contoh penggunaan fungsi lainnya
    csv_filename = (
        "../SRC/DB/wt_base.csv"  # Ganti dengan nama file CSV yang lo udah siapin
    )
    kdtks = read_kdtks_from_csv(csv_filename, column=0)
    tgls = read_kdtks_from_csv(csv_filename, column=1)

    for kdtk, tgl in zip(kdtks, tgls):
        ip = find_ip_by_kdtk(data_local, kdtk)
        if ip:
            # print("IP for KDTK", kdtk, ":", ip)
            # Connect dan eksekusi di sini
            ip_target = ip
            th = "20" + str(tgl[:2])  # Ambil dua karakter pertama untuk tahun
            bl = str(tgl[2:4])  # Ambil dua karakter berikutnya untuk bulan
            tg = str(tgl[4:])  # Ambil dua karakter terakhir untuk tanggal
            tanggal = th + "-" + bl + "-" + tg
            formatwt = "WT" + bl + tg + str(kdtk[0]) + "." + str(kdtk[1:4])
            # print(formatwt)
            passwords = [
                "iczmGcFJe//jBPZPGPFz0qTFHmiKHou6I=JFw32YgFQP",
                "nPx1FgDLqho0FYBbnAuFazlr4gl0vgD94=wL3sip88JJ",
                "giTk2Yr0K9VV5nBKzn22JafQt9iUiuQ3A=3soy2lSa1t",
            ]
            for password in passwords:
                if connect_to_mysql(ip_target, password, formatwt, tanggal):
                    break
            else:
                print("All passwords failed for IP:", ip)
        else:
            print("KDTK", kdtk, "not found in local database")


print()
#######################################################

for file in glob.glob("../RES/HR/*"):
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

with open("../SRC/DB/wt_base.csv", "r") as file:
    reader = csv.reader(file)
    existing_data = list(reader)

folder_path = "../RES/CSV/"
path_result = "../RES/WTR/"
hr_result = "../RES/HR/"

# Create a list to store the names of not found files
not_found_files = []


try:
    for i, row in enumerate(existing_data):
        if i == 0:
            continue

        try:
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

            shutil.copy(path_rekon + formatwt, hr_rev)
            shutil.copy(path_rekon + formatwt, path_csv)

            # Define the paths to the existing zip file and the new file
            existing_zip_file = "../RES/HRREV/" + hr
            new_file_to_add = "../RES/HRREV/" + formatwt

            # Define the name of the file to be replaced within the zip archive
            file_to_replace = formatwt

            # Check if the existing zip file exists
            if not os.path.exists(existing_zip_file):
                print(f"Zip file '{existing_zip_file}' not found. Skipping...")
            else:
                # Run the 7z command to update the zip archive
                command = ["7z", "u", existing_zip_file, new_file_to_add]
                

                with open("../RES/TXT/wrc2wt-log.txt", "a") as log_file:
                    try:
                        process = subprocess.run(command, stdout=log_file, stderr=log_file)
                        print(
                            f"File '{file_to_replace}' Zipped to '{existing_zip_file}' successfully."
                        )
                    except subprocess.CalledProcessError as e:
                        print(f"Error executing the command: {e}")
                        pass

        except Exception as inner_error:
            print(f"Error processing file {hr}: {inner_error}")
            # Log the error and move to the next file

except Exception as e:
    print(f"Critical error: {e}")


for file in glob.glob("../RES/HRREV/WT*"):
    os.remove(file)

# open explorer
relative_path = "../RES/WTR"
folder_path = os.path.abspath(relative_path)
os.system(f'explorer "{folder_path}"')
