import pandas as pd
import os
import glob

for file in glob.glob("../RES/XLS/Recon*"):
    os.remove(file)

current_dir = os.getcwd()
file_path_tk = os.path.join(current_dir, "../RES/XLS/TK_Result.xlsx")
file_path_wrc = os.path.join(current_dir, "../RES/XLS/WRC_Result.xlsx")

print(f"File path TK: {file_path_tk}")
print(f"File path WRC: {file_path_wrc}")

# Load data from Excel files
tk_df = pd.read_excel(file_path_tk)
wrc_df = pd.read_excel(file_path_wrc)


# Merge data on 'SHOP' and 'DATE'
merged_df = pd.merge(
    tk_df, wrc_df, on=["SHOP", "DATE", "CABANG"], how="outer", suffixes=("_TK", "_WRC")
)

# Convert PRDCD columns to numeric if needed, ignore errors for non-numeric values
merged_df["PRDCD_TK"] = pd.to_numeric(merged_df["PRDCD_TK"], errors="coerce")
merged_df["PRDCD_WRC"] = pd.to_numeric(merged_df["PRDCD_WRC"], errors="coerce")

# Calculate differences
result_df = pd.DataFrame(
    {
        "CABANG": merged_df["CABANG"],
        "SHOP": merged_df["SHOP"],
        "DATE": merged_df["DATE"],
        "PRDCD_TOKO": merged_df["PRDCD_TK"],
        "QTY_TOKO": merged_df["QTY_TK"],
        "PRDCD_WRC": merged_df["PRDCD_WRC"],
        "QTY_WRC": merged_df["QTY_WRC"],
        "SEL_PRDCD": merged_df["PRDCD_TK"]
        - merged_df["PRDCD_WRC"],  # Difference in PRDCD
        "SEL_QTY": merged_df["QTY_TK"] - merged_df["QTY_WRC"],  # Difference in quantity
    }
)

# Save the result to a new Excel file
result_df.to_excel("../RES/XLS/Reconciliation_Result.xlsx", index=False)
