import pandas as pd
import mysql.connector
from getpass import getpass

# Konfigurasi koneksi database
db_config = {
    "user": "cabang",
    "password": "c@B4n9@wRc$d7",
    "database": "poscabang"
}

# Daftar host database
hosts = {
    "SMD": "172.31.68.51",
    "BLG": "172.31.68.45",
    "TRK": "172.31.68.57"
}

# Fungsi untuk baca file CSV
def load_kodetoko(csv_file):
    df = pd.read_csv(csv_file)
    return df['kodetoko'].tolist()

# Fungsi untuk koneksi ke database
def connect_db(host, user, password, database):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    return conn

# Fungsi untuk generate dan execute query
def execute_query(conn, kodetoko, periode):
    cursor = conn.cursor()

    # Langkah 1: DELETE dan INSERT ke gl_acc_a
    query_1 = f"""
    DELETE FROM gl_acc_a;
    INSERT INTO GL_ACC_A SELECT glsource,glcat,gldate,segment4,segment5,gldesc,acctdr,acctcr,vdsitecd,refinv,gldok,tipedok 
    FROM gl_acc 
    WHERE segment5='{kodetoko}';
    """
    cursor.execute(query_1)

    # Langkah 2: DROP dan CREATE gl_acc_b
    query_2 = f"""
    DROP TABLE IF EXISTS gl_acc_b;
    CREATE TABLE gl_acc_b 
    SELECT 'BPB' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='payables' AND gldesc NOT LIKE '%cm%' UNION
    SELECT 'NRB' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='payables' AND gldesc LIKE '%cm%' UNION
    SELECT 'DISC05' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='idm-aicee' UNION
    SELECT 'V' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='idm-stx' AND gldesc LIKE '%trx vbs%' UNION
    SELECT 'RMB' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114006' AND glsource='idm-pos' AND gldesc LIKE '%rmb%' UNION
    SELECT 'Intransit' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='idm-toko-it' AND gldesc LIKE '%dalam perjalanan%' UNION
    SELECT 'IT' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='idm-toko-it' AND gldesc NOT LIKE '%dalam perjalanan%' UNION
    SELECT 'ID' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='idm-dc-id' UNION
    SELECT 'TAT IO' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='spreadsheet' AND gldesc LIKE 'transfer antar toko%' UNION
    SELECT 'Jurnal' source, COALESCE(SUM(acctdr),0) debit, COALESCE(SUM(acctcr),0) kredit 
    FROM gl_acc_a 
    WHERE segment5='{kodetoko}' AND segment4='114003' AND glsource='spreadsheet' AND gldesc NOT LIKE 'transfer antar toko%';
    """
    cursor.execute(query_2)

    # Langkah 3: DROP dan CREATE gl_acc_c
    query_3 = f"""
    DROP TABLE IF EXISTS gl_acc_c;
    CREATE TABLE gl_acc_c
    SELECT 'BPB' keter, ROUND(COALESCE(SUM(gross)-ABS(SUM(disc_05)),0),0) gross 
    FROM mstran_{periode} 
    WHERE shop='{kodetoko}' AND rtype='b' AND (toko LIKE 'gi%' OR toko LIKE 's%') AND istype NOT IN ('rmb','v') UNION
    SELECT 'NRB' keter, ROUND(COALESCE(SUM(gross),0),0) gross 
    FROM mstran_{periode} 
    WHERE shop='{kodetoko}' AND rtype='k' AND (toko LIKE 'gi%' OR toko LIKE 's%') AND istype NOT IN ('rmb','v') UNION
    SELECT 'DISC05' keter, ROUND(COALESCE(SUM(disc_05),0),0) gross 
    FROM mstran_{periode} 
    WHERE shop='{kodetoko}' AND rtype IN ('b','k') AND toko LIKE 'S%' OR toko LIKE 'GI%' UNION
    SELECT 'V' keter, ROUND(COALESCE(SUM(gross),0),0) gross 
    FROM mstran_{periode} 
    WHERE shop='{kodetoko}' AND rtype='b' AND istype='v' UNION
    SELECT 'RMB' keter, ROUND(COALESCE(SUM(gross),0),0) gross 
    FROM mstran_{periode} 
    WHERE shop='{kodetoko}' AND rtype='b' AND istype='rmb' UNION
    SELECT 'Intransit' AS keter, ROUND(COALESCE((SELECT SUM(glnrpam) FROM bulanan_id_{periode} WHERE glccoce='{kodetoko}' AND glcflag='d' AND glnacno='114003'), 0) - COALESCE((SELECT SUM(gross) FROM mstran_{periode} WHERE shop='{kodetoko}' AND rtype='b' AND toko LIKE 'g%' AND toko NOT LIKE 'gi%'), 0), 0) AS gross UNION
    SELECT 'IT' AS keter, ROUND(COALESCE((SELECT SUM(glnrpam) FROM bulanan_it_{periode} WHERE glcloka='{kodetoko}' AND glnacno='114003' AND glctipe IN ('BRK', 'HTB', 'PTB', 'PTM', 'PTP', 'RDT', 'PFS', 'BATLJ', 'BASMP','BAKLB') AND glcflag='D'), 0) - COALESCE((SELECT SUM(glnrpam) FROM bulanan_it_{periode} WHERE glcloka='{kodetoko}' AND glnacno='114003' AND glctipe IN ('BRK', 'HTB', 'PTB', 'PTM', 'PTP', 'RDT', 'PFS', 'BATLJ', 'BASMP','BAKLB') AND glcflag='C'), 0), 0) AS gross UNION
    SELECT 'ID' AS keter, ROUND(COALESCE((SELECT SUM(glnrpam) FROM bulanan_id_{periode} WHERE glccoce='{kodetoko}' AND glnacno='114003' AND glcflag='D'), 0) - COALESCE((SELECT SUM(glnrpam) FROM bulanan_id_{periode} WHERE glccoce='{kodetoko}' AND glnacno='114003' AND glcflag='c'), 0), 0) AS gross UNION
    SELECT 'TAT IO' AS keter, ROUND(COALESCE((SELECT SUM(gross) FROM mstran_{periode} WHERE shop='{kodetoko}' AND rtype='i'), 0) - COALESCE((SELECT SUM(gross) FROM mstran_{periode} WHERE shop='{kodetoko}' AND rtype='o'), 0), 0) AS gross UNION
    SELECT 'Jurnal' keter, '0' gross FROM mstran_{periode};
    """
    cursor.execute(query_3)

    # Langkah 4: DROP dan CREATE gl_acc_d
    query_4 = """
    DROP TABLE IF EXISTS gl_acc_d;
    CREATE TABLE gl_acc_d (
      `shop` VARCHAR(4) DEFAULT NULL,
      `site` VARCHAR(3) DEFAULT NULL,
      `source` VARCHAR(259) CHARACTER SET utf8 DEFAULT NULL,
      `debit` DECIMAL(32,0) DEFAULT NULL,
      `kredit` DECIMAL(32,0) DEFAULT NULL,
      `base` VARCHAR(3) DEFAULT NULL,
      `keter` VARCHAR(259) DEFAULT NULL,
      `gross` DECIMAL(32,0) DEFAULT NULL,
      `result` VARCHAR(259) DEFAULT NULL,
      `status` VARCHAR(259) DEFAULT NULL
    ) ENGINE=INNODB DEFAULT CHARSET=latin1;
    """
    cursor.execute(query_4)

    # Langkah 5: INSERT ke gl_acc_d
    query_5 = f"""
    INSERT INTO gl_acc_d 
    SELECT '{kodetoko}' shop, 'ACC' site, a.source, a.debit, a.kredit, 'EDP' base, b.keter, b.gross, '' result, '' `STATUS` 
    FROM gl_acc_b AS a 
    JOIN gl_acc_c AS b 
    ON a.source=b.keter;
    """
    cursor.execute(query_5)

    # Langkah 6: UPDATE result dan status di gl_acc_d
    query_6 = """
    UPDATE gl_acc_d 
    SET result=CASE 
      WHEN ABS(debit-kredit) = ABS(gross) THEN 0 
      ELSE ABS(ABS(debit - kredit) - ABS(gross)) 
    END;
    """
    cursor.execute(query_6)

    query_7 = """
    UPDATE gl_acc_d 
    SET status=CASE 
      WHEN result BETWEEN -500 AND 500 THEN 'Klop' 
      ELSE 'Sel' 
    END;
    """
    cursor.execute(query_7)

    # Langkah 7: SELECT hasil
    query_8 = "SELECT * FROM gl_acc_d;"
    cursor.execute(query_8)
    result = cursor.fetchall()

    # Tampilkan hasil
    for row in result:
        print(row)

    cursor.close()

def main():
    csv_file = '../SRC/DB/tokomain.csv'  # Path file CSV
    kodetoko_list = load_kodetoko(csv_file)

    periode = input("Masukkan periode (contoh: 2405): ")

    for kodetoko in kodetoko_list:
        for location, host in hosts.items():
            conn = connect_db(host, **db_config)
            execute_query(conn, kodetoko, periode)
            conn.close()

if __name__ == "__main__":
    main()
