import glob
import os
import mysql.connector
import pandas as pd
import csv
import time

start = time.time()


start_date = int(input("Enter the start date (YYMMDD): "))
end_date = int(input("Enter the end date (YYMMDD): "))

for file in glob.glob("../RES/XLS/mstran_stx*"):
    os.remove(file)
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

path_rekon = '../TEMP/WTR/'

mydb_smd = mysql.connector.connect(
    host=host_smd,
    user=user,
    password=pw,
    database=db)

mydb_blg = mysql.connector.connect(
    host=host_blg,
    user=user,
    password=pw,
    database=db)

mydb_tar = mysql.connector.connect(
    host=host_tar,
    user=user,
    password=pw,
    database=db)

for num in range(start_date, end_date + 1):
    period = str(num)
    table_names = 'wt_' + period
    table_names_rmb = 'rmb_' + period
    print(table_names)
    print(table_names_rmb)

    query_v=f"SELECT SHOP,DOCNO2,RECID,RTYPE,BUKTI_NO,SEQNO,DIVISI,PRDCD,QTY,PRICE,GROSS,CTERM,DOCNO2,ISTYPE,INVNO,TOKO,DATE,DATE_2,KETERANGAN,PTAG,CAT_COD,LOKASI,TGL1,TGL2,PPN,TOKO_1,DATE3,DOCNO3,SHOP,PRICE_IDM,PPNBM,PPNRP_IDM,LT,RAK,BAR,BKP,SUB_BKP,PLUMD,GROSS_JUAL,PRICE_JUAL,KODE_SUPPLIER,DISC05,GUDANG,SUPCO,BONUS,FMDFEE,PPNFEE,HRG_BOTOL,DISC1,DISC2,DISC3,DISC4CR,DISC4RR,DISC4JR,PO_NO,PO_DATE,BKL,NOSPH,TGLSPH,ADDID,ADDTIME,UPDID,UPDTIME,ROWID,STATUS_RECON,RATE_PPN,JAM FROM {table_names} WHERE ISTYPE='V';"

    query_rmb=f"SELECT KDTOKO,RECID,KDTOKO,TGLTRX,DOCNO,`DIV`,KDSUPPLIER,INVNO,PRDCD,QTY,PRICE,GROSS,PPN,PRICE_IDM,PPNBM_IDM,PPNRP_IDM,KETERANGAN,DPP_JUAL_KONS,PPN_JUAL_KONS,BKP,SUB_BKP,RATE_PPN,KATEGORI FROM {table_names_rmb};"


    column_query=['SHOP','DOCNO2','RECID','RTYPE','BUKTI_NO','SEQNO','DIVISI','PRDCD','QTY','PRICE','GROSS','CTERM','DOCNO2','ISTYPE','INVNO','TOKO','DATE','DATE_2','KETERANGAN','PTAG','CAT_COD','LOKASI','TGL1','TGL2','PPN','TOKO_1','DATE3','DOCNO3','SHOP','PRICE_IDM','PPNBM','PPNRP_IDM','LT','RAK','BAR','BKP','SUB_BKP','PLUMD','GROSS_JUAL','PRICE_JUAL','KODE_SUPPLIER','DISC05','GUDANG','SUPCO','BONUS','FMDFEE','PPNFEE','HRG_BOTOL','DISC1','DISC2','DISC3','DISC4CR','DISC4RR','DISC4JR','PO_NO','PO_DATE','BKL','NOSPH','TGLSPH','ADDID','ADDTIME','UPDID','UPDTIME','ROWID','STATUS_RECON','RATE_PPN','JAM']

    column_query_rmb=['KDTOKO','RECID','KDTOKO','TGLTRX','DOCNO','DIV','KDSUPPLIER','INVNO','PRDCD','QTY','PRICE','GROSS','PPN','PRICE_IDM','PPNBM_IDM','PPNRP_IDM','KETERANGAN','DPP_JUAL_KONS','PPN_JUAL_KONS','BKP','SUB_BKP','RATE_PPN','KATEGORI']

    
    mstran = mydb_smd.cursor()
    mstran.execute(query_v)
    df_mstran = pd.DataFrame(mstran.fetchall(), columns=column_query)
    df_selected_mstran = df_mstran[['RECID','RTYPE','BUKTI_NO','SEQNO','DIVISI','PRDCD','QTY','PRICE','GROSS','CTERM','DOCNO2','ISTYPE','INVNO','TOKO','DATE','DATE_2','KETERANGAN','PTAG','CAT_COD','LOKASI','TGL1','TGL2','PPN','TOKO_1','DATE3','DOCNO3','SHOP','PRICE_IDM','PPNBM','PPNRP_IDM','LT','RAK','BAR','BKP','SUB_BKP','PLUMD','GROSS_JUAL','PRICE_JUAL','KODE_SUPPLIER','DISC05','GUDANG','SUPCO','BONUS','FMDFEE','PPNFEE','HRG_BOTOL','DISC1','DISC2','DISC3','DISC4CR','DISC4RR','DISC4JR','PO_NO','PO_DATE','BKL','NOSPH','TGLSPH','ADDID','ADDTIME','UPDID','UPDTIME','ROWID','STATUS_RECON','RATE_PPN','JAM']]
    df_selected_mstran['JAM'] = df_selected_mstran['JAM'].astype(str).str[-8:]
    cek = len(df_selected_mstran)
    if cek >= 0:
        df_selected_mstran.to_csv(path_rekon + table_names + 'smd_' + 'mstran' +
                  '.csv', index=False, header=True, sep='|')
    else:
        continue

    
    rmb = mydb_smd.cursor()
    rmb.execute(query_rmb)
    df_rmb = pd.DataFrame(rmb.fetchall(), columns=column_query_rmb)
    df_selected_rmb = df_rmb[['RECID','KDTOKO','TGLTRX','DOCNO','DIV','KDSUPPLIER','INVNO','PRDCD','QTY','PRICE','GROSS','PPN','PRICE_IDM','PPNBM_IDM','PPNRP_IDM','KETERANGAN','DPP_JUAL_KONS','PPN_JUAL_KONS','BKP','SUB_BKP','RATE_PPN','KATEGORI']]
    cek = len(df_selected_rmb)
    if cek >= 0:
        df_selected_rmb.to_csv(path_rekon + table_names + 'smd_' + 'rmb' +
                  '.csv', index=False, header=True, sep='|')
    else:
        continue

    mstran = mydb_blg.cursor()
    mstran.execute(query_v)
    df_mstran = pd.DataFrame(mstran.fetchall(), columns=column_query)
    df_selected_mstran = df_mstran[['RECID','RTYPE','BUKTI_NO','SEQNO','DIVISI','PRDCD','QTY','PRICE','GROSS','CTERM','DOCNO2','ISTYPE','INVNO','TOKO','DATE','DATE_2','KETERANGAN','PTAG','CAT_COD','LOKASI','TGL1','TGL2','PPN','TOKO_1','DATE3','DOCNO3','SHOP','PRICE_IDM','PPNBM','PPNRP_IDM','LT','RAK','BAR','BKP','SUB_BKP','PLUMD','GROSS_JUAL','PRICE_JUAL','KODE_SUPPLIER','DISC05','GUDANG','SUPCO','BONUS','FMDFEE','PPNFEE','HRG_BOTOL','DISC1','DISC2','DISC3','DISC4CR','DISC4RR','DISC4JR','PO_NO','PO_DATE','BKL','NOSPH','TGLSPH','ADDID','ADDTIME','UPDID','UPDTIME','ROWID','STATUS_RECON','RATE_PPN','JAM']]
    df_selected_mstran['JAM'] = df_selected_mstran['JAM'].astype(str).str[-8:]
    cek = len(df_selected_mstran)
    if cek >= 0:
        df_selected_mstran.to_csv(path_rekon + table_names + 'blg_' + 'mstran' +
                  '.csv', index=False, header=True, sep='|')
    else:
        continue

    rmb = mydb_blg.cursor()
    rmb.execute(query_rmb)
    df_rmb = pd.DataFrame(rmb.fetchall(), columns=column_query_rmb)
    df_selected_rmb = df_rmb[['RECID','KDTOKO','TGLTRX','DOCNO','DIV','KDSUPPLIER','INVNO','PRDCD','QTY','PRICE','GROSS','PPN','PRICE_IDM','PPNBM_IDM','PPNRP_IDM','KETERANGAN','DPP_JUAL_KONS','PPN_JUAL_KONS','BKP','SUB_BKP','RATE_PPN','KATEGORI']]
    cek = len(df_selected_rmb)
    if cek >= 0:
        df_selected_rmb.to_csv(path_rekon + table_names + 'blg_' + 'rmb' +
                  '.csv', index=False, header=True, sep='|')
    else:
        continue

    mstran = mydb_tar.cursor()
    mstran.execute(query_v)
    df_mstran = pd.DataFrame(mstran.fetchall(), columns=column_query)
    df_selected_mstran = df_mstran[['RECID','RTYPE','BUKTI_NO','SEQNO','DIVISI','PRDCD','QTY','PRICE','GROSS','CTERM','DOCNO2','ISTYPE','INVNO','TOKO','DATE','DATE_2','KETERANGAN','PTAG','CAT_COD','LOKASI','TGL1','TGL2','PPN','TOKO_1','DATE3','DOCNO3','SHOP','PRICE_IDM','PPNBM','PPNRP_IDM','LT','RAK','BAR','BKP','SUB_BKP','PLUMD','GROSS_JUAL','PRICE_JUAL','KODE_SUPPLIER','DISC05','GUDANG','SUPCO','BONUS','FMDFEE','PPNFEE','HRG_BOTOL','DISC1','DISC2','DISC3','DISC4CR','DISC4RR','DISC4JR','PO_NO','PO_DATE','BKL','NOSPH','TGLSPH','ADDID','ADDTIME','UPDID','UPDTIME','ROWID','STATUS_RECON','RATE_PPN','JAM']]
    df_selected_mstran['JAM'] = df_selected_mstran['JAM'].astype(str).str[-8:]
    cek = len(df_selected_mstran)
    if cek >= 0:
        df_selected_mstran.to_csv(path_rekon + table_names + 'tar_' + 'mstran' +
                  '.csv', index=False, header=True, sep='|')
    else:
        continue

    rmb = mydb_tar.cursor()
    rmb.execute(query_rmb)
    df_rmb = pd.DataFrame(rmb.fetchall(), columns=column_query_rmb)
    df_selected_rmb = df_rmb[['RECID','KDTOKO','TGLTRX','DOCNO','DIV','KDSUPPLIER','INVNO','PRDCD','QTY','PRICE','GROSS','PPN','PRICE_IDM','PPNBM_IDM','PPNRP_IDM','KETERANGAN','DPP_JUAL_KONS','PPN_JUAL_KONS','BKP','SUB_BKP','RATE_PPN','KATEGORI']]
    cek = len(df_selected_rmb)
    if cek >= 0:
        df_selected_rmb.to_csv(path_rekon + table_names + 'tar_' + 'rmb' +
                  '.csv', index=False, header=True, sep='|')
    else:
        continue   


print("Export Data to Excel")


csv_directory = "../TEMP/WTR/"

# List all CSV files in the directory
csv_files = [filename for filename in os.listdir(csv_directory) if filename.endswith(("mstran.csv", "rmb.csv"))]

# Create an empty dictionary to store DataFrames for each type of data
dataframes = {'mstran': pd.DataFrame(), 'rmb': pd.DataFrame()}

# Loop through each CSV file and merge its contents into the respective DataFrame based on its category
for csv_file in csv_files:
    csv_path = os.path.join(csv_directory, csv_file)
    df = pd.read_csv(csv_path, sep='|')
    # Determine the category based on the file name
    category = csv_file.split('.')[0].split('_')[-1]  # Extract the category from the file name
    # Concatenate the current DataFrame to the respective category DataFrame
    dataframes[category] = pd.concat([dataframes[category], df], ignore_index=True)

# Save each category's DataFrame to a separate sheet in an Excel file
excel_filename = "../RES/XLS/mstran_stx_"+ str(end_date) +".xlsx"

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

print("Time Elapsed:", round(final_min, 2), 'Minutes')

# Buka Result
current_dir = os.getcwd()
file_path = os.path.join(current_dir, '../RES/XLS/mstran_stx_'+ str(end_date) +'.xlsx')
os.startfile(file_path)
