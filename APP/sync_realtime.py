import os
import threading
import time
from datetime import datetime, timedelta
from ftplib import FTP
from tqdm import tqdm


def download_files_with_period_from_ftp(
    ftp_host, ftp_user, ftp_pass, remote_folder, local_folder, period
):
    try:
        ftp = FTP(ftp_host)
        ftp.login(ftp_user, ftp_pass)
        ftp.cwd(remote_folder)
        files = ftp.nlst()

        for file in files:
            if period in file:
                remote_filepath = os.path.join(remote_folder, file)
                local_filepath = os.path.join(local_folder, file)
                ftp_file_size = ftp.size(file)
                local_file_size = (
                    os.path.getsize(local_filepath)
                    if os.path.exists(local_filepath)
                    else None
                )
                if local_file_size is None or ftp_file_size > local_file_size:
                    with open(local_filepath, "wb") as local_file, tqdm(
                        unit="B", unit_scale=True, desc=file, total=ftp_file_size
                    ) as progress:
                        ftp.retrbinary(
                            "RETR {}".format(file),
                            lambda data: write_and_update_progress(
                                data, local_file, progress
                            ),
                        )
                else:
                    pass

    except Exception as e:
        print("Error:", e)
    finally:
        ftp.quit()


def write_and_update_progress(data, local_file, progress):
    local_file.write(data)
    progress.update(len(data))


def download_for_warehouse(ftp_host, ftp_user, ftp_pass, remote_folder, user, period, period_hr):
    # local_folder = os.path.join("Q:\\HARIAN\\", period, user)
    local_folder = os.path.join("V:\\DTHR\\HR2412", user)
    download_files_with_period_from_ftp(
        ftp_host, ftp_user, ftp_pass, remote_folder, local_folder, period_hr
    )


def main():
    ftp_host = "192.168.59.111"
    ftp_pass = "edp@2021"
    remote_folder = "/was_proses"
    warehouse_codes = {
        "G050": "eis",
        "G241": "eis_g241",
        "G242": "eis_g242"
    }

    while True:
        # Dapatkan tanggal H-1
        yesterday = datetime.now() - timedelta(days=1)
        period_hr = yesterday.strftime("%y%m%d")
        period = period_hr[2:4] + "20" + period_hr[:2]

        # Mulai download tiap gudang pakai threading
        threads = []
        for user, ftp_user in warehouse_codes.items():
            thread = threading.Thread(
                target=download_for_warehouse,
                args=(ftp_host, ftp_user, ftp_pass, remote_folder, user, period, period_hr),
            )
            threads.append(thread)
            thread.start()

        # Tunggu semua thread selesai
        for thread in threads:
            thread.join()

        print("Selesai download untuk semua gudang. Jalankan ulang program dalam 5 menit..")
        time.sleep(300)  # 5 menit jeda


# Mulai program
if __name__ == "__main__":
    main()
