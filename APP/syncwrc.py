from ftplib import FTP
import io
import csv
from tqdm import tqdm
import os

# Buka WRC BASE
current_dir = os.getcwd()
file_path = os.path.join(current_dir, '../SRC/DB/wt_base.csv')
os.startfile(file_path)

print()
print("Please Input Toko and Periode on CSV")
print()
input("Press Enter to Continue")

def copy_file(source_ftp, source_dir, source_file, dest_ftp, dest_dir):
    # Buka direktori di FTP source
    source_ftp.cwd(source_dir)
    
    # Cek apakah file ada di direktori tersebut
    file_exists = source_file in source_ftp.nlst()
    
    # Jika file tidak ada, kembalikan False
    if not file_exists:
        return False
    
    # Salin file langsung dari FTP source ke FTP destination
    with io.BytesIO() as file_obj:
        source_ftp.retrbinary('RETR ' + source_file, file_obj.write)
        file_obj.seek(0)
        
        dest_ftp.cwd(dest_dir)
        
        # Lalu simpan filenya di situ dengan nama yang sama kayak di source
        dest_ftp.storbinary('STOR ' + source_file, file_obj)
    
    # Jika file ditemukan dan berhasil disalin, kembalikan True
    return True

# Buat objek FTP untuk source
source_ftp = FTP()

# Panggil fungsi copy_file dengan parameter yang sesuai
source_ftp.connect('192.168.59.70')
source_ftp.login('hrtampung', '123')

# List direktori yang mau kita loop beserta alamat FTP tujuan
list_direktori = [
    ('/G050/BACKUP/HARIAN/', '172.31.68.51'),
    ('/G241/BACKUP/HARIAN/', '172.31.68.45'),
    ('/G242/BACKUP/HARIAN/', '172.31.68.57')
]

# Baca nama file dari CSV
with open('../SRC/DB/wt_base.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip baris pertama
    file_found = False  # Flag untuk menandai apakah file sudah ditemukan
    for row in reader:
        toko = row[0]
        tgl  = row[1]
        hr = toko[:1]+'R'+tgl+'.' + toko[1:]
        if hr.startswith('T'):
            hr = 'HR'+tgl+'.' + toko[1:]
        elif hr.startswith('R'):
            hr = 'CR'+tgl+'.' + toko[1:]
        elif hr.startswith('F'):
            hr = 'FR'+tgl+'.' + toko[1:]

        # Looping antara direktori
        for direktori, ftp_address in list_direktori:
            # Buat objek FTP untuk destination
            dest_ftp = FTP()
            dest_ftp.connect(ftp_address)
            dest_ftp.login('edp', 'edp54321')
            
            # Panggil fungsi copy_file untuk nyolokin filenya
            if copy_file(source_ftp, direktori, hr, dest_ftp, '/WebRekapCabang/myfiles/01/'):
                file_found = True
                kode_direktori = direktori.split('/')[1]  # Ambil kode direktori (misal: G050)
                print(f"File {hr} ditemukan di {direktori} dan diupload ke WRC {kode_direktori}")
                dest_ftp.quit()  # Tutup koneksi FTP destination
                break  # Jika file ditemukan, hentikan looping direktori
            dest_ftp.quit()  # Tutup koneksi FTP destination
        
        if not file_found:
            tqdm.write(f"File {hr} tidak ditemukan di semua direktori yang ada.")

# Tutup koneksi FTP source
source_ftp.quit()

# Kasih keterangan kalo file tidak ditemukan
if not file_found:
    print("File tidak ditemukan di semua direktori yang ada.")
