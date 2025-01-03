from ftplib import FTP
import os
from tqdm import tqdm

def download_files_with_period_from_ftp(ftp_host, ftp_user, ftp_pass, remote_folder, local_folder, period):
    try:
        # Nyambungin ke FTP server
        ftp = FTP(ftp_host)
        ftp.login(ftp_user, ftp_pass)

        # Pindahin ke folder tempat file berada
        ftp.cwd(remote_folder)

        # List semua file di folder FTP
        files = ftp.nlst()

        # Loop buat download file yang cocok dengan periode HR
        for file in files:
            if period in file:
                remote_filepath = os.path.join(remote_folder, file)
                local_filepath = os.path.join(local_folder, file)
                # Cek ukuran file di FTP
                ftp_file_size = ftp.size(file)
                # Cek ukuran file lokal (kalo udah ada)
                local_file_size = os.path.getsize(local_filepath) if os.path.exists(local_filepath) else None
                # Replace file kalo ukuran file di FTP lebih besar
                if local_file_size is None or ftp_file_size > local_file_size:
                    # Download file dengan progress bar
                    with open(local_filepath, 'wb') as local_file, tqdm(unit='B', unit_scale=True, desc=file, total=ftp_file_size) as progress:
                        ftp.retrbinary('RETR {}'.format(file), lambda data: write_and_update_progress(data, local_file, progress))
                else:
                    print('File {} sudah ada, skip Download.'.format(file))

    except Exception as e:
        print('Error:', e)

    finally:
        # Tutup koneksi FTP
        ftp.quit()

def write_and_update_progress(data, local_file, progress):
    local_file.write(data)
    progress.update(len(data))

# Contoh penggunaan
ftp_host = '192.168.59.111'
period_hr = input("Masukkan periode HR : ")
user_input = input("Masukkan kode gudang : ")
user = user_input.upper()  # Ubah jadi uppercase
ftp_user = ''
ftp_pass = ''

# Cek kode gudang dan tentuin user dan password
if user == 'G050':
    ftp_user = 'eis'
elif user == 'G241':
    ftp_user = 'eis_g241'
elif user == 'G242':
    ftp_user = 'eis_g242'
elif user == 'G245':
    ftp_user = 'eis_g245'
elif user == 'G092':
    ftp_user = 'eis_G092'
else:
    print('Kode gudang tidak valid!')
    exit()

ftp_pass = 'edp@2021'

remote_folder = '/was_proses'
# Ambil periode tanpa tanggal
period = period_hr[:4]
local_folder = os.path.join('V:\\DTHR\\HR{}'.format(period), user)

download_files_with_period_from_ftp(ftp_host, ftp_user, ftp_pass, remote_folder, local_folder, period_hr)
