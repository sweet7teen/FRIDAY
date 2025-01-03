import os
import time

while True:
    os.system("cls")  
    print()
    print('Welcome to Friday, Please Input Number Below')
    print()
    print('1. WT')
    print('2. WRC2WT')
    print('3. XWT')
    print('4. RMB')
    print('5. MSTRAN/DAY')
    print()
    print('Input The Suitable Option in The Next Line:')
    text_input = int(input())

    if text_input == 1:
        os.system("cls")   
        exec(open("wt.py").read())
    elif text_input == 2:
        os.system("cls")   
        exec(open("wrc2wt.py").read())
    elif text_input == 3:
        os.system("cls")   
        exec(open("xwt.py").read())
    elif text_input == 4:
        os.system("cls")   
        exec(open("rmb.py").read())
    elif text_input == 5:
        os.system("cls")   
        exec(open("daymst.py").read())
    else:
        
        print('wrong input')
        time.sleep(2)  # Memberikan jeda 2 detik sebelum mencetak 'wrong input'
