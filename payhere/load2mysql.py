import pymysql
import os

Attribution = ["상가업소번호", "상호명", "지점명", "상권업종대분류코드", "상권업종대분류명", "상권업종중분류코드", "상권업종중분류명", "상권업종소분류코드", "상권업종소분류명", "표준산업분류코드", "표준산업분류명", "시도코드", "시도명", "시군구코드", "시군구명", "행정동코드", "행정동명", "법정동코드", "법정동명", "지번코드", "대지구분코드", "대지구분명", "지번본번지", "지번부번지", "지번주소", "도로명코드", "도로명", "건물본번지", "건물부번지", "건물관리번호", "건물명", "도로명주소", "구우편번호", "신우편번호", "동정보", "층정보", "호정보", "경도", "위도"]
data_type = ["VARCHAR(100)"] * (len(Attribution) - 2) + ["Decimal(15,12)", "Decimal(15,13)"]    
At = ', '.join(Attribution)
At_dt = ', '.join(list(map(lambda x : ' '.join(x), zip(Attribution, data_type))))
area_conv = {"부산":"busan", "대구":"daegu", "세종":"sejong", "제주":"jeju", "경기":"gyeonggi", "울산":"ulsan", "서울":"seoul", "대전":"daejeon", "충남":"chungnam", "충북":"chungbuk", "강원":"gangwon", "광주":"gwangju", "인천":"incheon", "전남":"jeonnam", "경남":"gyeongnam", "경북":"gyeongbuk", "전북":"jeonbuk"}


connect = pymysql.connect(host = "ec2-43-201-28-227.ap-northeast-2.compute.amazonaws.com", user = "kuno", passwd = "1111", db = "payhere2", charset = "utf8")
cursor = connect.cursor()

folder_Q = os.listdir('./CAI')
for folder_A in folder_Q:
    file_list= os.listdir(f'./CAI/{folder_A}')
    for file in file_list:
        if file == ".DS_Store":
            continue
        AreaQdate = area_conv[str(file.split('_')[2])] + str(file[-10:-4])
        print(AreaQdate)
        create_table_query = f"CREATE TABLE CAI{AreaQdate} ({At_dt});"
        try: 
            cursor.execute(create_table_query) 
        except: 
            pass
        connect.commit()
        with open(f'./CAI/{folder_A}/{file}') as f:
            header = f.readline()
            line = f.readline()
            while line:
                line_replace = line.replace(',,', ',"",')
                replace = True
                while replace:
                    line, line_replace = line_replace, line_replace.replace(',,', ',"",') 
                    if line_replace == line:
                        replace = False
                insert_data_query = f"INSERT INTO CAI{AreaQdate} ({At}) VALUES ({line});"
                cursor.execute(insert_data_query)
                line = f.readline()
            connect.commit()
cursor.close()
connect.close()



