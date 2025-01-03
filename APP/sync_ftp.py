from ftplib import FTP
import io

def stream_ftp_to_local_ftp(ftp_source_info, ftp_local_info, remote_folder, local_folder, periode):
    # Koneksi ke FTP Source
    ftp_source = FTP(ftp_source_info["host"])
    ftp_source.login(user=ftp_source_info["user"], passwd=ftp_source_info["pass"])
    ftp_source.cwd(remote_folder)

    # Koneksi ke FTP Lokal
    ftp_local = FTP(ftp_local_info["host"])
    ftp_local.login(user=ftp_local_info["user"], passwd=ftp_local_info["pass"])

    # Buat subfolder berdasarkan warehouse di FTP Lokal
    try:
        ftp_local.cwd(local_folder)
    except:
        print(f"Creating folder {local_folder} in FTP Local...")
        ftp_local.mkd(local_folder)

    # Balik ke folder utama
    ftp_local.cwd(local_folder)

    # List file di FTP Source
    files = ftp_source.nlst()

    # Filter file berdasarkan periode
    filtered_files = [file for file in files if periode in file]

    if not filtered_files:
        print(f"Tidak ada file yang mengandung periode {periode} di FTP Source.")
        return

    # List file yang sudah ada di FTP Lokal
    existing_files = {}
    for line in ftp_local.mlsd(local_folder):
        name, meta = line
        if "size" in meta:
            existing_files[name] = int(meta["size"])  # Simpan nama dan ukuran file

    for file in filtered_files:
        print(f"Checking {file}...")
        # Dapatkan ukuran file di FTP Source
        size_source = ftp_source.size(file)

        # Cek apakah file sudah ada di FTP Lokal dengan ukuran yang sama
        if file in existing_files and existing_files[file] == size_source:
            print(f"File {file} sudah ada, skip.")
            continue

        # Stream file langsung dari FTP Source ke FTP Lokal menggunakan buffer
        print(f"Sync {file} from ftp")
        buffer = io.BytesIO()
        ftp_source.retrbinary(f"RETR {file}", buffer.write)
        buffer.seek(0)  # Reset posisi pointer ke awal file
        ftp_local.storbinary(f"STOR {file}", buffer)
        buffer.close()

    ftp_source.quit()
    ftp_local.quit()
    print("Sinkronisasi selesai untuk file dengan periode!")

# Mapping warehouse ke user FTP
warehouse_codes = {
    "G050": "eis",
    "G241": "eis_g241",
    "G242": "eis_g242",
    "G245": "eis",
    "G143": "eis_g143",
    "G165": "eis_g165",
}

# Input dari user
selected_warehouse = input(f"Pilih warehouse ({', '.join(warehouse_codes.keys())}): ").strip().upper()
periode = input("Masukkan periode (contoh: 241222): ").strip()

if selected_warehouse not in warehouse_codes:
    print(f"Warehouse {selected_warehouse} gak valid. Coba lagi!")
else:
    # Info FTP Source berdasarkan warehouse yang dipilih
    ftp_source_info = {
        "host": "192.168.59.111",
        "user": warehouse_codes[selected_warehouse],
        "pass": "edp@2021",
    }

    # Info FTP Lokal
    ftp_local_info = {
        "host": "192.168.26.20",
        "user": "harian",
        "pass": "123456",
    }

    # Folder lokal dengan subfolder warehouse
    local_folder = f"/HR2412/{selected_warehouse}"

    # Jalankan sinkronisasi
    stream_ftp_to_local_ftp(
        ftp_source_info,
        ftp_local_info,
        remote_folder="/was_proses",
        local_folder=local_folder,
        periode=periode,
    )
