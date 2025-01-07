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
                "SELECT RECID,KDTOKO,TGLTRX,DOCNO,`DIV`,KDSUPPLIER,INVNO,PRDCD,QTY,PRICE,GROSS,PPN,PRICE_IDM,PPNBM_IDM,PPNRP_IDM,KETERANGAN,DPP_JUAL_KONS,PPN_JUAL_KONS,BKP,SUB_BKP,RATE_PPN FROM %s ORDER BY docno DESC"
                % (formatwt)
            )

            data = conn.fetchall()
            # print(len(data[0]))  # Ini buat ngecek berapa kolom yang ada di data
            columns = [
                "RECID",
                "KDTOKO",
                "TGLTRX",
                "DOCNO",
                "DIV",
                "KDSUPPLIER",
                "INVNO",
                "PRDCD",
                "QTY",
                "PRICE",
                "GROSS",
                "PPN",
                "PRICE_IDM",
                "PPNBM_IDM",
                "PPNRP_IDM",
                "KETERANGAN",
                "DPP_JUAL_KONS",
                "PPN_JUAL_KONS",
                "BKP",
                "SUB_BKP",
                "RATE_PPN",
            ]
            df = pd.DataFrame(data, columns=columns)

            # df = pd.DataFrame(conn.fetchall(), columns=['RECID','KDTOKO','TGLTRX','DOCNO','`DIV`','KDSUPPLIER','INVNO','PRDCD','QTY','PRICE','GROSS','PPN','PRICE_IDM','PPNBM_IDM','PPNRP_IDM','KETERANGAN','DPP_JUAL_KONS','PPN_JUAL_KONS','BKP,SUB_BKP','RATE_PPN'])

            df_select = df[
                [
                    "RECID",
                    "KDTOKO",
                    "TGLTRX",
                    "DOCNO",
                    "DIV",
                    "KDSUPPLIER",
                    "INVNO",
                    "PRDCD",
                    "QTY",
                    "PRICE",
                    "GROSS",
                    "PPN",
                    "PRICE_IDM",
                    "PPNBM_IDM",
                    "PPNRP_IDM",
                    "KETERANGAN",
                    "DPP_JUAL_KONS",
                    "PPN_JUAL_KONS",
                    "BKP",
                    "SUB_BKP",
                    "RATE_PPN",
                ]
            ]

            # # Mengonversi nilai bytearray menjadi string pada kolom DATE
            # df_select['DATE'] = df_select['DATE'].apply(lambda x: str(x, 'utf-8'))
            # df_select['JAM'] = df_select['JAM'].astype(str).str[-8:]

            cek = len(df_select)

            if cek > 0:
                df_select.to_csv(
                    path_rekon + formatwtq, index=False, header=True, sep="|"
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
            # formatwt = 'WT' + bl + tg + str(kdtk[0]) + '.' + str(kdtk[1:4])
            formatwtq = "RMB" + th + bl + tg + str(kdtk[0]) + "." + str(kdtk[1:4])
            formatwt = "RMB" + th + bl + tg + str(kdtk[0]) + str(kdtk[1:4])
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
