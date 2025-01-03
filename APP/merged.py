import pandas as pd
import os
import glob

# Path to the directory containing your CSV files
csv_directory = "../RES/CSV/"

for file in glob.glob("../RES/XLS/wtgab.xl*"):
    os.remove(file)

# List all CSV files in the directory
csv_files = [filename for filename in os.listdir(
    csv_directory) if filename.startswith("WT")]

# Create an empty DataFrame to store merged data
merged_data = pd.DataFrame()

# Loop through each CSV file and merge its contents into the DataFrame
for csv_file in csv_files:
    csv_path = os.path.join(csv_directory, csv_file)
    df = pd.read_csv(csv_path, sep='|', dtype={"DOCNO": str, "SEQNO": str, "DIV": str,  "CTERM": str, "DOCNO2": str, "ISTYPE": str, "INVNO": str, "DATE": str, "DATE2": str, "KETERANGAN": str, "PTAG": str, "CAT_COD": str, "LOKASI": str, "TGL1": str,"TGL2": str, "TOKO_1": str, "DATE3": str, "DOCNO3": str, "SHOP": str, "PPNBM_IDM": str, "LT": str, "RAK": str, "BAR": str, "BKP": str, "SUB_BKP": str, "PLUMD": str, "GROSS_JUAL": str, "PRICE_JUAL": str, "KODE_SUPPLIER": str, "DISC05": str, "RATE_PPN": str})
    merged_data = pd.concat([merged_data, df], ignore_index=True)

# Save the merged data to an Excel file
excel_filename = "../RES/XLS/wtgab.xlsx"
merged_data.to_excel(excel_filename, index=False)
