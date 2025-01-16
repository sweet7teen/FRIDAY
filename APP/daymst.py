import glob
import os
import mysql.connector
import pandas as pd
import time

start = time.time()


start_date = int(input("Enter the start date (YYMMDD): "))
end_date = int(input("Enter the end date (YYMMDD): "))

# for file in glob.glob("../RES/XLS/mstran*"):
#     os.remove(file)
for file in glob.glob("../TEMP/WTR/*"):
    os.remove(file)

# DB CONNECTION
host_smd = "172.31.68.51"
host_blg = "172.31.68.45"
host_tar = "172.31.68.57"
user = "cabang"
pw = "c@B4n9@wRc$d7"
db = "poscabang"

existing_data = []

path_rekon = "../TEMP/WTR/"

mydb_smd = mysql.connector.connect(host=host_smd, user=user, password=pw, database=db)

mydb_blg = mysql.connector.connect(host=host_blg, user=user, password=pw, database=db)

mydb_tar = mysql.connector.connect(host=host_tar, user=user, password=pw, database=db)

for num in range(start_date, end_date + 1):
    period = str(num)
    table_names = "wt_" + period
    rmb = "rmb_" + period
    print(table_names)

    query_bpb = (
        "SELECT SHOP,BUKTI_NO DOCNO,DATE(TGL1) TGL1,CONCAT(YEAR(TGL1),'-',SHOP,'-',BUKTI_NO,IF(RTYPE='B','',IF(RTYPE='K','-CM',CONCAT('-',RTYPE))),IF(LEFT(TOKO,1)='G', CONCAT('-',SUB_BKP),'')) AS INV_EDP,TOKO,ISTYPE,SUB_BKP,QTY,SUM(GROSS) GROSS,SUM(PPN) PPN,SUM(DISC05) DISC05 FROM %s WHERE RTYPE='B' AND ISTYPE NOT IN ('GGC','V') GROUP BY SHOP,BUKTI_NO,RTYPE,SUB_BKP,ISTYPE;"
        % (table_names)
    )

    query_nrb = (
        "SELECT SHOP,BUKTI_NO DOCNO,DATE(TGL1) TGL1,CONCAT(YEAR(TGL1),'-',SHOP,'-',BUKTI_NO,IF(RTYPE='B','',IF(RTYPE='K','-CM',CONCAT('-',RTYPE))),IF(LEFT(TOKO,1)='G', CONCAT('-',SUB_BKP),'')) AS INV_EDP,TOKO,ISTYPE,SUB_BKP,QTY,SUM(GROSS) GROSS,SUM(PPN) PPN,SUM(DISC05) DISC05 FROM %s WHERE RTYPE='K' GROUP BY SHOP,BUKTI_NO,RTYPE,SUB_BKP,ISTYPE;"
        % (table_names)
    )

    query_vir = (
        "SELECT KDTOKO,TGLTRX,DOCNO,INVNO,KDSUPPLIER,SUB_BKP,sum(QTY),SUM(GROSS) GROSS,SUM(PPN) PPN FROM %s WHERE KDTOKO LIKE 'F%%' GROUP BY KDTOKO,TGLTRX;"
        % (rmb)
    )

    # query_vir = (
    #     "SELECT SHOP,BUKTI_NO DOCNO,DATE(TGL1) TGL1,CONCAT(YEAR(TGL1),'-',SHOP,'-',BUKTI_NO,IF(RTYPE='B','',IF(RTYPE='K','-CM',CONCAT('-',RTYPE))),IF(LEFT(TOKO,1)='G', CONCAT('-',SUB_BKP),'')) AS INV_EDP,TOKO,ISTYPE,SUB_BKP,QTY,SUM(GROSS) GROSS,SUM(PPN) PPN,SUM(DISC05) DISC05 FROM %s WHERE RTYPE='B' AND ISTYPE='V' GROUP BY SHOP,BUKTI_NO,RTYPE,SUB_BKP;"
    #     % (table_names)
    # )

    query_babkl = (
        "SELECT SHOP,BUKTI_NO DOCNO,DATE(TGL1) TGL1,CONCAT(YEAR(TGL1),'-',SHOP,'-',BUKTI_NO,IF(RTYPE='B','',IF(RTYPE='K','-CM',CONCAT('-',RTYPE))),IF(LEFT(TOKO,1)='G', CONCAT('-',SUB_BKP),'')) AS INV_EDP,TOKO,ISTYPE,SUB_BKP,QTY,SUM(GROSS) GROSS,SUM(PPN) PPN,SUM(DISC05) DISC05 FROM %s WHERE ISTYPE='BABKL' GROUP BY SHOP,BUKTI_NO,RTYPE,SUB_BKP;"
        % (table_names)
    )

    column_query = [
        "SHOP",
        "DOCNO",
        "TGL",
        "INV",
        "TOKO",
        "ISTYPE",
        "SUB_BKP",
        "QTY",
        "GROSS",
        "PPN",
        "DISC05",
    ]

    columnvir_query = [
        "KDTOKO",
        "TGLTRX",
        "DOCNO",
        "INVNO",
        "KDSUPPLIER",
        "SUB_BKP",
        "QTY",
        "GROSS",
        "PPN",
    ]

    bpb = mydb_smd.cursor()
    bpb.execute(query_bpb)
    df_bpb = pd.DataFrame(bpb.fetchall(), columns=column_query)
    cek = len(df_bpb)
    if cek >= 0:
        df_bpb.to_csv(
            path_rekon + table_names + "smd_" + "bpb" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    nrb = mydb_smd.cursor()
    nrb.execute(query_nrb)
    df_nrb = pd.DataFrame(nrb.fetchall(), columns=column_query)
    cek = len(df_nrb)
    if cek >= 0:
        df_nrb.to_csv(
            path_rekon + table_names + "smd_" + "nrb" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    vir = mydb_smd.cursor()
    vir.execute(query_vir)
    df_vir = pd.DataFrame(vir.fetchall(), columns=columnvir_query)
    cek = len(df_vir)
    if cek >= 0:
        df_vir.to_csv(
            path_rekon + table_names + "smd_" + "vir" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    babkl = mydb_smd.cursor()
    babkl.execute(query_babkl)
    df_babkl = pd.DataFrame(babkl.fetchall(), columns=column_query)
    cek = len(df_babkl)
    if cek >= 0:
        df_babkl.to_csv(
            path_rekon + table_names + "smd_" + "babkl" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    bpb = mydb_blg.cursor()
    bpb.execute(query_bpb)
    df_bpb = pd.DataFrame(bpb.fetchall(), columns=column_query)
    cek = len(df_bpb)
    if cek >= 0:
        df_bpb.to_csv(
            path_rekon + table_names + "blg_" + "bpb" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    nrb = mydb_blg.cursor()
    nrb.execute(query_nrb)
    df_nrb = pd.DataFrame(nrb.fetchall(), columns=column_query)
    cek = len(df_nrb)
    if cek >= 0:
        df_nrb.to_csv(
            path_rekon + table_names + "blg_" + "nrb" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    vir = mydb_blg.cursor()
    vir.execute(query_vir)
    df_vir = pd.DataFrame(vir.fetchall(), columns=columnvir_query)
    cek = len(df_vir)
    if cek >= 0:
        df_vir.to_csv(
            path_rekon + table_names + "blg_" + "vir" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    babkl = mydb_blg.cursor()
    babkl.execute(query_babkl)
    df_babkl = pd.DataFrame(babkl.fetchall(), columns=column_query)
    cek = len(df_babkl)
    if cek >= 0:
        df_babkl.to_csv(
            path_rekon + table_names + "blg_" + "babkl" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    bpb = mydb_tar.cursor()
    bpb.execute(query_bpb)
    df_bpb = pd.DataFrame(bpb.fetchall(), columns=column_query)
    cek = len(df_bpb)
    if cek >= 0:
        df_bpb.to_csv(
            path_rekon + table_names + "tar_" + "bpb" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    nrb = mydb_tar.cursor()
    nrb.execute(query_nrb)
    df_nrb = pd.DataFrame(nrb.fetchall(), columns=column_query)
    cek = len(df_nrb)
    if cek >= 0:
        df_nrb.to_csv(
            path_rekon + table_names + "tar_" + "nrb" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    vir = mydb_tar.cursor()
    vir.execute(query_vir)
    df_vir = pd.DataFrame(vir.fetchall(), columns=columnvir_query)
    cek = len(df_vir)
    if cek >= 0:
        df_vir.to_csv(
            path_rekon + table_names + "tar_" + "vir" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue

    babkl = mydb_tar.cursor()
    babkl.execute(query_babkl)
    df_babkl = pd.DataFrame(babkl.fetchall(), columns=column_query)
    cek = len(df_babkl)
    if cek >= 0:
        df_babkl.to_csv(
            path_rekon + table_names + "tar_" + "babkl" + ".csv",
            index=False,
            header=True,
            sep="|",
        )
    else:
        continue


print("Export Data to Excel")


csv_directory = "../TEMP/WTR/"

# List all CSV files in the directory
csv_files = [
    filename
    for filename in os.listdir(csv_directory)
    if filename.endswith(("bpb.csv", "nrb.csv", "vir.csv", "babkl.csv"))
]

# Create an empty dictionary to store DataFrames for each type of data
dataframes = {
    "bpb": pd.DataFrame(),
    "nrb": pd.DataFrame(),
    "vir": pd.DataFrame(),
    "babkl": pd.DataFrame(),
}

# Loop through each CSV file and merge its contents into the respective DataFrame based on its category
for csv_file in csv_files:
    csv_path = os.path.join(csv_directory, csv_file)
    df = pd.read_csv(csv_path, sep="|")
    # Determine the category based on the file name
    category = csv_file.split(".")[0].split("_")[
        -1
    ]  # Extract the category from the file name
    # Concatenate the current DataFrame to the respective category DataFrame
    dataframes[category] = pd.concat([dataframes[category], df], ignore_index=True)

# Save each category's DataFrame to a separate sheet in an Excel file
excel_filename = "../RES/XLS/mstran_" + str(end_date) + ".xlsx"

with pd.ExcelWriter(excel_filename) as writer:
    for category, df in dataframes.items():
        df.to_excel(writer, sheet_name=category.upper(), index=False)

# # Load your Excel file into a DataFrame
# excel_file = "../TEMP/XLS/mstranperday.xlsx"
# df = pd.read_excel(excel_file)

# # Remove duplicates based on specific columns (e.g., "Column1" and "Column2")
# # Keep the first occurrence of each unique row
# df_no_duplicates = df.drop_duplicates(
#     subset=["SHOP","DOCNO","TGL","TOKO","ISTYPE","SUB_BKP"], keep="first")

# # Save the DataFrame with duplicates removed to a new Excel file
# output_excel_file = "../RES/XLS/mstran_"+ str(start_date) + "-"+ str(end_date) +".xlsx"

# df_no_duplicates.to_excel(output_excel_file, index=False)

# print("Duplicates removed and saved to", output_excel_file)

# <code to time>
end = time.time()

min = end - start
final_min = min / 60

print("Time Elapsed:", round(final_min, 2), "Minutes")

# Buka Result
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "../RES/XLS/mstran_" + str(end_date) + ".xlsx")
os.startfile(file_path)
