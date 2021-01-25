# cx_Oracle 연동한 진료 EMR (admission, op, discharge) 임상과별 1건 샘플 CSV 적재 @ 2021.01.19

import cx_Oracle

# oracle instant client 연동 및 path 지정 (참조: https://willbesoon.tistory.com/120)
import os
LOCATION = r"C:\oracle\instantclient_11_2" # oracle db를 쓰기위한 유틸파일
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"] # 환경변수 등록

import pandas as pd

# 과별 EMR 종류별 샘플 1건씩 누적해서 담을 최종 변수 <-- 미사용으로 주석 @ 2021.01.19
#import collections as col
#totalResults = col.deque([])

# 접속해야할 DB서버의 아이피 주소 혹은 서버이름, 포트번호, SID 정보를 입력
dsn = cx_Oracle.makedsn("YOUR_DB_IP", 9999, "YOUR_DB_SID")

#print(dsn)

# 데이터 베이스의 이름, 비밀번호, dsn으로 데이터 베이스에 연결
db = cx_Oracle.connect("YOUR_EMR_ID", "YOUR_EMR_PW", dsn)

#print(db)

# SQL문 실행 메모리 영역(cursor)를 열고 확인

cursor = db.cursor()

query4deptlist = """select deptcd
                    from 부서마스터
                    where locate = '병원구분'                    
                    and consyn = 'Y'
                    and siyn = 'Y'
                    and supyn = 'Y'                    
                    and docuyn = 'Y'
                    """

cursor.execute(query4deptlist)

dept_cd_tuple = cursor.fetchall()

#print(type(dept_cd_tuple))
#print("length of dept_cd = ", str(len(dept_cd_tuple)))

for i in range(len(dept_cd_tuple)):

    query4acptno = """select acptno
                    from 입원접수마스터
                    where rejtdate is null
                    and admdate between trunc(sysdate) - 30 and trunc(sysdate) - 23
                    and meddept = :deptcd
                    and dschdate is not null
                    and rownum = 1
                    """
    cursor.execute(query4acptno, deptcd=str(dept_cd_tuple[i]).replace(',','').replace('(', '').replace(')', '').replace("'", ''))

    result4acptno = cursor.fetchone()

    print("#######################")
    #print(type(dept_cd_tuple[0]))
    #print(type(str(dept_cd_tuple[0])))
    #print(result4acptno)

    if result4acptno != None:
        print(str(dept_cd_tuple[i]).replace(',','').replace('(', '').replace(')', '') + " starts !")

        queryTerm = """select
                               a.PATNO		
                            ,  a.FSTDEPT	
                            ,  a.ADMDATE 	
                            ,  d.CPNT_NM	
                            ,  nvl(g.TRMN_NM, d.MNDT_ITEM_NM)	
                            ,  f.TRMN_NM	                                                        
                            ,  c.LFSD_VALE_VL || nvl(c.VALE_VL, dbms_lob.substr(c.EMR_SRCH_ITM_CD_CTN, 3600, 1)) || c.RGSD_VALE_VL 
                        from   입원접수마스터  a
                            ,  진료이력 b
                            ,  EMR검색 c
                            ,  EMR아이템 d
                            ,  용어(val) e
                            ,  용어(atrb) f
                            ,  용어(entt) g
                        where  1=1                        
                        and  a.REJTDATE         is null 
                        and	b.MDRP_NO 			= :acptno
                        and  b.PTNO              = a.PATNO
                        and  b.MDRP_NO           = a.ACPTNO
                        and  b.DLTN_YN           = 'N'
                        and  b.RCKI_CD           = '999' -- admission note                        
                        and  c.RCRD_NO           = b.RCRD_NO
                        and  d.SPRN_DPRT_CD      = 'MRD'
                        and  d.FORM_NO           = c.FORM_NO
                        and  d.FORM_ITM_ID       = c.EMR_SRCH_ITM_ID
                        and  e.TRMN_ID           = d.TRMN_ID                        
                        and  f.TRMN_ID(+)        = d.EAV_ATRB_TRMN_ID
                        and  g.TRMN_ID(+)        = d.EAV_ENTT_TRMN_ID
        """

        cursor.execute(queryTerm, acptno=int(result4acptno[0]))

        result = cursor.fetchall()

        df = pd.DataFrame(result)

        # 컬럼 붙여주기 
        df.columns = ['PAT_ID', 'DEPT_CD', 'ADM_DATE', 'CPMT', 'ENTITY', 'ATTR', 'VALUE']

        #print(df)

        # 참조: https://hogni.tistory.com/10
        try: 
            if not os.path.exists('./output.csv'):
                df.to_csv("./output.csv", index=False, mode='w', encoding="utf-8-sig")     
                print('first write finished successfuly')                 
            else:
                df.to_csv("./output.csv", index=False, mode='a', encoding="utf-8-sig", header=False)     
                print('concatanation finished successfuly') 
            
        except Exception as e: 
            df.to_csv("./raised_error.csv", index=False, encoding="utf-8-sig") 
            print('DataFrame append error!!!! please check it out. : ', e) 
            break

    else:
        print(str(dept_cd_tuple[i]).replace(',','').replace('(', '').replace(')', '') + " skipped....")
        continue

cursor.close()
db.close()
