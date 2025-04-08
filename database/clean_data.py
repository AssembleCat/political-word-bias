import sqlite3
import pandas as pd
import re
from database import DATABASE_NAME

def clean_data():
    """
    데이터베이스의 데이터를 정제하는 함수
    1. 의원ID가 없는 행 제거
    2. 발언자 이름 정제
    """
    print("데이터베이스 정제 작업을 시작합니다...")
    
    # 데이터베이스 연결
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # 1. 현재 데이터 상태 확인
    query = "SELECT COUNT(*) FROM speeches"
    total_rows = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"정제 전 총 행 수: {total_rows:,}개")
    
    # 2. 의원ID가 없는 행 확인
    query = "SELECT COUNT(*) FROM speeches WHERE 의원ID IS NULL OR 의원ID = ''"
    empty_id_count = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"의원ID가 없는 행 수: {empty_id_count:,}개")
    
    # 3. 의원ID가 없는 행 제거
    if empty_id_count > 0:
        cursor.execute("DELETE FROM speeches WHERE 의원ID IS NULL OR 의원ID = ''")
        conn.commit()
        print(f"의원ID가 없는 {empty_id_count:,}개 행을 제거했습니다.")
    
    # 4. 제거 후 데이터 상태 확인
    query = "SELECT COUNT(*) FROM speeches"
    after_clean_id = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"의원ID 정제 후 총 행 수: {after_clean_id:,}개")
    
    # 5. 발언자 이름 정제
    print("\n=== 발언자 이름 정제 시작 ===")
    
    # 6. member_bias 테이블에서 의원 이름 가져오기
    query = "SELECT name FROM member_bias"
    member_names = pd.read_sql_query(query, conn)['name'].tolist()
    print(f"member_bias 테이블에서 {len(member_names)}명의 의원 이름을 가져왔습니다.")
    
    # 7. speeches 테이블에서 고유한 발언자 이름 가져오기
    query = "SELECT DISTINCT 발언자 FROM speeches"
    speakers = pd.read_sql_query(query, conn)['발언자'].tolist()
    print(f"speeches 테이블에서 {len(speakers)}명의 고유한 발언자를 찾았습니다.")
    
    # 8. 발언자 이름 정제 매핑 생성
    name_mapping = {}
    
    for speaker in speakers:
        if speaker in member_names:
            # 이미 정확히 일치하는 이름은 그대로 유지
            continue
        
        # 패턴 1: "XXX 위원" -> "XXX"
        if ' 위원' in speaker:
            clean_name = speaker.split(' 위원')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 2: "XXX 위원장" -> "XXX"
        if ' 위원장' in speaker:
            clean_name = speaker.split(' 위원장')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 3: "XXX 의원" -> "XXX"
        if ' 의원' in speaker:
            clean_name = speaker.split(' 의원')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 4: "XXX 장관" -> "XXX"
        if ' 장관' in speaker:
            clean_name = speaker.split(' 장관')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 5: "국방부장관 XXX" -> "XXX"
        if '장관 ' in speaker:
            clean_name = speaker.split('장관 ')[1].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 6: "부총리겸기획재정부장관 XXX" -> "XXX"
        if '부총리겸기획재정부장관 ' in speaker:
            clean_name = speaker.split('부총리겸기획재정부장관 ')[1].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 7: "국무총리 XXX" -> "XXX"
        if '국무총리 ' in speaker:
            clean_name = speaker.split('국무총리 ')[1].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 8: "XXX 부총리" -> "XXX"
        if ' 부총리' in speaker:
            clean_name = speaker.split(' 부총리')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 9: "XXX 청장" -> "XXX"
        if ' 청장' in speaker:
            clean_name = speaker.split(' 청장')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 10: "XXX 처장" -> "XXX"
        if ' 처장' in speaker:
            clean_name = speaker.split(' 처장')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 11: "XXX 실장" -> "XXX"
        if ' 실장' in speaker:
            clean_name = speaker.split(' 실장')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
        
        # 패턴 12: "XXX 국장" -> "XXX"
        if ' 국장' in speaker:
            clean_name = speaker.split(' 국장')[0].strip()
            if clean_name in member_names:
                name_mapping[speaker] = clean_name
                continue
    
    print(f"총 {len(name_mapping)}개의 발언자 이름이 정제되었습니다.")
    
    # 9. 발언자 이름 업데이트
    print("speeches 테이블 업데이트 중...")
    
    update_count = 0
    batch_size = 1000
    
    # 10. 배치 처리로 업데이트
    for speaker, clean_name in name_mapping.items():
        cursor.execute(
            "UPDATE speeches SET 발언자 = ? WHERE 발언자 = ?",
            (clean_name, speaker)
        )
        
        update_count += cursor.rowcount
        
        if update_count % 50000 == 0:
                conn.commit()
                print(f"  - {update_count:,}개 행 업데이트 완료")
    
    # 최종 커밋
    conn.commit()
    print(f"총 {update_count:,}개 행이 업데이트되었습니다.")
    
    # 11. 정제 후 통계
    query = """
    SELECT s.발언자, COUNT(*) as 발언수, m.party
    FROM speeches s
    LEFT JOIN member_bias m ON s.발언자 = m.name
    GROUP BY s.발언자
    ORDER BY 발언수 DESC
    LIMIT 10
    """
    
    top_speakers = pd.read_sql_query(query, conn)
    print("\n=== 정제 후 상위 발언자 (정당 정보 포함) ===")
    print(top_speakers)
    
    # 12. 매칭 성공률 계산
    query = """
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN m.name IS NOT NULL THEN 1 ELSE 0 END) as matched_rows
    FROM speeches s
    LEFT JOIN member_bias m ON s.발언자 = m.name
    """
    
    match_stats = pd.read_sql_query(query, conn)
    total_rows = match_stats['total_rows'][0]
    matched_rows = match_stats['matched_rows'][0]
    match_rate = (matched_rows / total_rows) * 100 if total_rows > 0 else 0
    
    print(f"\n총 {total_rows:,}개 행 중 {matched_rows:,}개 행이 member_bias와 매칭되었습니다 (매칭률: {match_rate:.2f}%)")
    
    # 13. 발언자 통계
    query = """
    SELECT 발언자, COUNT(*) as 발언수
    FROM speeches
    GROUP BY 발언자
    ORDER BY 발언수 DESC
    LIMIT 10
    """
    
    speaker_stats = pd.read_sql_query(query, conn)
    print("\n=== 발언자 통계 (상위 10명) ===")
    for index, row in speaker_stats.iterrows():
        print(f"{row['발언자']}: {row['발언수']:,}회")
    
    # 데이터베이스 연결 종료
    conn.close()
    print("\n데이터베이스 정제 작업이 완료되었습니다.")

if __name__ == "__main__":
    clean_data()
