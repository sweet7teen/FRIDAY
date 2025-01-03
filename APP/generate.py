import os
import glob
import pyperclip

for file in glob.glob("../RES/TXT/put*"):
    os.remove(file)

def generate_put_syntax(kode_toko, periode_tanggal, jenis):
    # Mengonversi kode toko menjadi huruf kapital
    kode_toko = kode_toko.upper()

    # Mendapatkan karakter pertama dari kode toko
    karakter_pertama = kode_toko[0]

    # Generate syntax berdasarkan jenis dan karakter pertama kode toko
    if jenis == "HR":
        if karakter_pertama == "T":
            syntax = f"C:\\IDMCommandListeners\\wput D:\\BACKOFF\\HARIAN\\HR{periode_tanggal}.{kode_toko[-3:]} ftp://edp_ams_smd:edp@ams050@172.24.31.91/edp/G050/HR{periode_tanggal}.{kode_toko[-3:]}"
        elif karakter_pertama == "F":
            syntax = f"C:\\IDMCommandListeners\\wput D:\\BACKOFF\\HARIAN\\FR{periode_tanggal}.{kode_toko[-3:]} ftp://edp_ams_smd:edp@ams050@172.24.31.91/edp/G050/FR{periode_tanggal}.{kode_toko[-3:]}"
        else:
            syntax = None
    elif jenis == "IDT":
        syntax = f"C:\\IDMCommandListeners\\wput D:\\BACKOFF\\DATA\\{kode_toko}{periode_tanggal}.IDT ftp://edp_ams_smd:edp@ams050@172.24.31.91/edp/G050/{kode_toko}{periode_tanggal}.IDT"
    else:
        syntax = None
    
    if syntax:
        return syntax
    else:
        return "Format kode toko tidak valid."

def write_to_notepad(content):
    # Tulis konten ke file notepad
    with open("../RES/TXT/put_syntax.txt", "w") as file:
        file.write(content)

def main():
    # Menu pilihan untuk backup per hari (HR) atau backup per IDT
    print("Pilih menu:")
    print("1. HR")
    print("2. IDT")
    menu_jenis = input("Input (1/2) : ")

    if menu_jenis == "1":
        jenis_backup = "HR"
    elif menu_jenis == "2":
        jenis_backup = "IDT"
    elif menu_jenis == "3":
        jenis_backup = "FILET"
    else:
        print("Pilihan tidak valid.")
        return

    # Menu pilihan untuk Put atau Get
    print("Pilih menu:")
    print("1. Put")
    print("2. Get")
    menu_tindakan = input("Input (1/2) : ")

    if menu_tindakan == "1":
        # Input kode toko dan periode tanggal untuk Put
        kode_toko = input("Masukkan kode toko: ")
        periode_tanggal = input("Masukkan periode tanggal: ")
        # Generate syntax untuk Put
        syntax_put = generate_put_syntax(kode_toko, periode_tanggal, jenis_backup)
        print(f"Syntax untuk Put {jenis_backup}:")
        print(syntax_put)
        pyperclip.copy(syntax_put)
        # # Tulis syntax ke Notepad
        # write_to_notepad(syntax_put)
        # # Buka Notepad dengan syntax yang ditulis
        # os.system("notepad ../RES/TXT/put_syntax.txt")
    elif menu_tindakan == "2":
        # Implementasi menu Get
        print("Menu Get belum diimplementasikan.")
    else:
        print("Pilihan tidak valid.")

if __name__ == "__main__":
    main()
