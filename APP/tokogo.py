import pandas as pd
import glob
import os
    
path_download = "D:\\DOWNLOAD\\"

# Path file lama dan baru
file_lama = os.path.join(path_download, "Toko Baru.xlsx")
file_baru = os.path.join(path_download, "Toko Baru.html")

os.rename(file_lama, file_baru)  # Rename dulu

# Baca file HTML yang isinya Excel
file_path = file_baru  # Pake path string langsung

tables = pd.read_html(file_path)

# Print tabel pertama (kalau ada banyak tabel, bisa pilih index)
df = tables[0]

# Filter data berdasarkan kolom kdcab
filter_kdcab = ['G050', 'G241', 'G242']  # Nilai yang lo mau
df_filtered = df[df.iloc[:, 2].isin(filter_kdcab)]  # Filter kolom ke-3 (index 2)
df_sorted = df_filtered.sort_values(by=df_filtered.columns[1], ascending=True)

# Simpan ke file Excel lagi kalau perlu
df_sorted.to_excel("D:\DOWNLOAD\Toko Baru New.xlsx", index=False)

file_path = os.path.join(path_download, "Toko Baru New.xlsx")
os.startfile(file_path)

for file in glob.glob("D:\\DOWNLOAD\\Toko Baru.*"):
    os.remove(file)
