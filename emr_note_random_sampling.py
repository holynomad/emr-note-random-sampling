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

    result4acptno = cursor.fetchall()

    print("#######################")
    #print(type(dept_cd_tuple[0]))
    #print(type(str(dept_cd_tuple[0])))
    #print(result4acptno)

    if result4acptno != None:
        print(str(dept_cd_tuple[i]).replace(',','').replace('(', '').replace(')', '') + " starts !")

        for j in range(len(result4acptno)):

            queryTerm = """select
                                    a.PATNO
                                ,   a.FSTDEPT
                                ,   a.ADMDATE
                                ,	b.RCRD_DT
                                ,	nvl(h.CPT_NM,
                                                    (
                                                    select
                                                            listagg(ss.TRMN_NM, '->') within group(order by ss.FORM_ITM_SNO)
                                                        from  EMR서식마스터 ss
                                                    where  1=1
                                                        and  ss.FORM_ITM_ID      <> c.EMR_SRCH_ITM_ID  -- 시작 아이템 제거
                                                        and  ss.HGRN_FORM_ITM_ID is not null           -- 아이템 그룹 포함
                                                    start  with
                                                            ss.SPRN_DPRT_CD  = 'MRD'
                                                        and  ss.FORM_NO       = c.FORM_NO
                                                        and  ss.FORM_ITM_ID   = c.EMR_SRCH_ITM_ID
                                                    connect  by nocycle
                                                            ss.SPRN_DPRT_CD  = prior ss.SPRN_DPRT_CD
                                                        and  ss.FORM_NO       = prior ss.FORM_NO
                                                        and  ss.FORM_ITM_ID   = prior ss.HGRN_FORM_ITM_ID
                                                    )
                                    )                                                           --"Entity(Attr.포함)"
                                ,  c.LFSD_VALE_VL || nvl(c.VALE_VL, dbms_lob.substr(c.EMR_SRCH_ITM_CD_CTN, 4000, 1)) || c.RGSD_VALE_VL	--"Value"
                            from   입원접수마스터  a
                                ,  진료이력 b
                                ,  EMR검색 c
                                ,  EMR아이템 d
                                ,  용어(val) e
                                ,  용어(atrb) f
                                ,  용어(entt) g
                            where  1=1
                            --and  a.PATNO             = '00093962'
                            and  a.ACPTNO            = :acptno
                            and  a.REJTDATE         is null
                            and  b.PTNO              = a.PATNO
                            and  b.MDRP_NO           = a.ACPTNO
                            and  b.DLTN_YN           = 'N'
                            and  b.RCKI_CD           = '142'   -- op note
                            --and  b.RCRD_DT           between to_date('20201210', 'yyyymmdd')
                            --                                and trunc(sysdate)
                            and  c.RCRD_NO           = b.RCRD_NO
                            and  d.SPRN_DPRT_CD      = 'MRD'
                            and  d.FORM_NO           = c.FORM_NO
                            and  d.FORM_ITM_ID       = c.EMR_SRCH_ITM_ID
                            and  e.TRMN_ID			= d.TRMN_ID
                            and 	h.SPRN_DPRT_CD(+)      = 'MRD'
                            and 	h.FORM_NO(+)           = d.FORM_NO
                            and 	h.FORM_ITM_ID(+)       = d.HGRN_FORM_ITM_ID
                            and	h.HGRN_FORM_ITM_ID(+) is null
                            and 	i.SPRN_DPRT_CD(+)      = 'MRD'
                            and 	i.FORM_NO(+)           = d.FORM_NO
                            and 	i.FORM_ITM_ID(+)       = d.HGRN_FORM_ITM_ID
                            and	i.HGRN_FORM_ITM_ID(+) is not null
                            order by d.FORM_ITM_SNO                                 
            """

            try:
                cursor.execute(queryTerm, acptno=int(str(result4acptno[j]).replace(',','').replace('(', '').replace(')', '').replace("'", '')))

                result = cursor.fetchall()

                #print(str(len(result)))

            # buffer 관련 db 오류 이력남기고 pass @ 2021.04.14.
            except Exception as e:
                print(e)
                continue

            # 수술이력 진료접수번호는 존재하지만 실제 EMR 기록이 없는경우 예외처리 @ 2021.04.14.
            if len(result) != 0 :
                df = pd.DataFrame(result)

                # 컬럼 붙여주기 
                df.columns = ['PAT_ID', 'DEPT_CD', 'ADM_DATE', 'RGT_DATE', 'ENTITY', 'VALUE']

                #print(df)

                # 출처: https://hogni.tistory.com/10
                try: 
                    if not os.path.exists('./op_note_output.csv'):
                        df.to_csv("./op_note_output.csv", index=False, mode='w', encoding="utf-8-sig")     
                        print('first write finished successfuly')                 
                    else:
                        df.to_csv("./op_note_output.csv", index=False, mode='a', encoding="utf-8-sig", header=False)     
                        print('concatanation finished successfuly') 
                    
                except Exception as e: 
                    df.to_csv("./op_raised_error.csv", index=False, encoding="utf-8-sig") 
                    print('DataFrame append error!!!! please check it out. : ', e) 
                    break

    else:
        print(str(dept_cd_tuple[i]).replace(',','').replace('(', '').replace(')', '') + " skipped....")
        continue

cursor.close()
db.close()
