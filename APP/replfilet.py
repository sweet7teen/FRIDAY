import os
import pandas as pd

def replace_in_files(directory, old_str, new_str):
    for filename in os.listdir(directory):
        if filename.startswith("MTRAN"):
            file_path = os.path.join(directory, filename)
            print("Sedang membaca file:", file_path)
            df = pd.read_csv(file_path)
            
            if 'TANGGAL' in df.columns:
                print("Kolom 'TANGGAL' ditemukan, sedang melakukan perubahan...")
                df['TANGGAL'] = df['TANGGAL'].str.replace(old_str, new_str)
                print("Perubahan selesai, menyimpan file kembali...")
                df.to_csv(file_path, index=False)
                print("File berhasil disimpan.")

# Ganti 'path_ke_folder_csv' dengan path folder tempat CSV lo disimpan
directory = '../mtran/1/'
old_str = '/'
new_str = '-'
replace_in_files(directory, old_str, new_str)
