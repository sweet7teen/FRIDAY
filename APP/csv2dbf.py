import os
import csv
import re
import glob

for file in glob.glob("../TGAB/CSV/*"):
    os.remove(file)
for file in glob.glob("../TGAB/DBF/*"):
    os.remove(file)

#open explorer
relative_path = '../TGAB/ACSV'
folder_path = os.path.abspath(relative_path)
os.system(f'explorer "{folder_path}"')

print()
print("Copy file CSV yang akan di convert ke folder /TGAB/ACSV")
print()
input("Press Enter to Continue")

print("Proses convert csv ke comma delimited!")


input_folder = '../TGAB/ACSV/'
output_folder = '../TGAB/CSV/'

# Pastikan folder outputnya udah ada
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Loop semua file di folder input
for filename in os.listdir(input_folder):
    if filename.endswith('.csv'):
        input_file = os.path.join(input_folder, filename)
        output_file = os.path.join(output_folder, filename)
        with open(input_file, 'r') as f_input:
            with open(output_file, 'w', newline='') as f_output:
                csv_reader = csv.reader(f_input, delimiter='|')
                csv_writer = csv.writer(f_output, delimiter=',', quoting=csv.QUOTE_NONE)
                for row in csv_reader:
                    csv_writer.writerow([re.sub(r'["]', '', field) for field in row])

print("CSV sukses di convert ke comma delimited!")

print("Proses convert csv ke dbf!")
def convert_csv_to_dbf(csv_file, dbf_file):
    libreoffice_path = "\"C:\Program Files (x86)\LibreOffice\program\soffice.exe" # Sesuaikan path LibreOffice di komputer lo

    # Command untuk convert CSV ke DBF pake LibreOffice
    command = f'"{libreoffice_path}" --headless --convert-to dbf "{csv_file}" --outdir "{os.path.dirname(dbf_file)}"'
    
    # Jalankan command
    os.system(command)
    #   print("Konversi CSV ke DBF selesai!")


# Ganti path folder dan ekstensi file sesuai kebutuhan
folder_path = "../TGAB/CSV/"
file_extension = ".csv"

# Iterate through CSV files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(file_extension):
        csv_file = os.path.join(folder_path, filename)
        dbf_file = os.path.join("../TGAB/DBF/", filename)
        convert_csv_to_dbf(csv_file, dbf_file)



print("Konversi CSV ke DBF selesai!")
