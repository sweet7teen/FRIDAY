import mysql.connector
from mysql.connector import Error
import json

# Koneksi ke DB lokal
db_lokal = mysql.connector.connect(
    host="192.168.26.78", user="root", password="", database="lokal"
)

cursor = db_lokal.cursor()
cursor.execute("SELECT cabang, kdtk, ip FROM mtoko ORDER BY cabang")
data = cursor.fetchall()

# Simpan data ke file JSON
session_pass = "../RES/JSON/session_pass.json"
with open(session_pass, "w") as f:
    json.dump(
        [{"cabang": row[0], "kdtk": row[1], "ip": row[2]} for row in data],
        f,
        indent=4,
    )

cursor.close()
db_lokal.close()
