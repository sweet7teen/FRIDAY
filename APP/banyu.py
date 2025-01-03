from flask import Flask, request, jsonify
import mysql.connector
from flask_cors import CORS  # Import module CORS

app = Flask(__name__)
CORS(app)  # Aktifkan CORS untuk semua rute

# Koneksi ke database
try:
    db = mysql.connector.connect(
        host="192.168.26.78",
        user="root",
        password="",
        database="rekon"
        # host="172.31.68.57",
        # user="cabang",
        # password="c@B4n9@wRc$d7",
        # database="poscabang"
    )
    db_status = "Database connected"
except Exception as e:
    db_status = f"Error connecting to database: {str(e)}"

@app.route('/')
def index():
    return "API is running"

@app.route('/check_db')
def check_db():
    return db_status

@app.route('/absen_wt', methods=['POST'])
def absen_wt():
    try:
        # Ambil syntax query dari body request
        syntax_query = request.json['query']
        
        # Eksekusi syntax query
        cursor = db.cursor()
        cursor.execute(syntax_query)
        
        # Jika query merupakan SELECT, ambil hasilnya
        if cursor.description:
            hasil = cursor.fetchall()
        else:
            hasil = None
        
        db.commit()
        cursor.close()

        return jsonify({'message': 'Query berhasil dijalankan', 'hasil': hasil})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
