import sqlite3
import pandas as pd
import os
import sys

# 상위 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import DATABASE_NAME

def fix_empty_member_ids():
    """
    의원ID가 공백(' ')인 행들을 찾아서 해당 발언자의 다른 행에서 정상적인 의원ID를 찾아 업데이트하는 함수
    """
    print("의원ID 공백 문제 수정 작업을 시작합니다...")
    
    # 데이터베이스 연결
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # 1. 현재 데이터 상태 확인
    query = "SELECT COUNT(*) FROM speeches"
    total_rows = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"전체 행 수: {total_rows:,}개")
    
    # 2. 의원ID가 공백(' ')인 행 수 확인
    query = "SELECT COUNT(*) FROM speeches WHERE 의원ID = ' '"
    empty_id_count = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"의원ID가 공백(' ')인 행 수: {empty_id_count:,}개")
    
    if empty_id_count == 0:
        print("의원ID가 공백인 행이 없습니다.")
        conn.close()
        return
    
    # 3. 의원ID가 공백인 행의 발언자 목록 가져오기
    query = "SELECT DISTINCT 발언자 FROM speeches WHERE 의원ID = ' '"
    speakers_with_empty_id = pd.read_sql_query(query, conn)['발언자'].tolist()
    print(f"의원ID가 공백인 발언자 수: {len(speakers_with_empty_id)}명")
    
    # 4. 각 발언자별로 정상적인 의원ID 찾기
    update_count = 0
    speakers_fixed = 0
    
    for speaker in speakers_with_empty_id:
        # 해당 발언자의 정상적인 의원ID 찾기
        query = """
        SELECT DISTINCT 의원ID 
        FROM speeches 
        WHERE 발언자 = ? AND 의원ID != ' ' AND 의원ID IS NOT NULL AND 의원ID != ''
        LIMIT 1
        """
        result = pd.read_sql_query(query, conn, params=(speaker,))
        
        if not result.empty:
            valid_member_id = result.iloc[0, 0]
            
            # 의원ID 업데이트
            cursor.execute(
                "UPDATE speeches SET 의원ID = ? WHERE 발언자 = ? AND 의원ID = ' '",
                (valid_member_id, speaker)
            )
            
            rows_updated = cursor.rowcount
            update_count += rows_updated
            speakers_fixed += 1
            
            print(f"발언자 '{speaker}': 의원ID '{valid_member_id}'로 {rows_updated:,}개 행 업데이트")
    
    # 5. 변경사항 저장
    conn.commit()
    
    # 6. 업데이트 후 상태 확인
    query = "SELECT COUNT(*) FROM speeches WHERE 의원ID = ' '"
    remaining_empty = pd.read_sql_query(query, conn).iloc[0, 0]
    
    print(f"\n총 {speakers_fixed:,}명의 발언자, {update_count:,}개 행의 의원ID를 업데이트했습니다.")
    print(f"남은 공백 의원ID 행 수: {remaining_empty:,}개")
    
    # 7. 남은 공백 의원ID가 있는 경우 상위 발언자 출력
    if remaining_empty > 0:
        query = """
        SELECT 발언자, COUNT(*) as 행수
        FROM speeches
        WHERE 의원ID = ' '
        GROUP BY 발언자
        ORDER BY 행수 DESC
        LIMIT 10
        """
        remaining_speakers = pd.read_sql_query(query, conn)
        
        print("\n=== 아직 의원ID가 공백인 상위 발언자 ===")
        for _, row in remaining_speakers.iterrows():
            print(f"{row['발언자']}: {row['행수']:,}개 행")
    
    # 데이터베이스 연결 종료
    conn.close()
    print("\n의원ID 공백 문제 수정 작업이 완료되었습니다.")

if __name__ == "__main__":
    fix_empty_member_ids()
