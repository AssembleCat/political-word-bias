import sqlite3
import pandas as pd
from database import DATABASE_NAME

def filter_speeches():
    """
    speeches 테이블에서 member_bias 테이블의 name 필드에 존재하는 발언자의 데이터만 남기고
    나머지는 삭제하는 함수
    """
    print("speeches 테이블 필터링 작업을 시작합니다...")
    
    # 데이터베이스 연결
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # 1. 현재 데이터 상태 확인
    query = "SELECT COUNT(*) FROM speeches"
    total_rows = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"필터링 전 총 행 수: {total_rows:,}개")
    
    # 2. member_bias 테이블의 name 목록 가져오기
    query = "SELECT name FROM member_bias"
    member_names = pd.read_sql_query(query, conn)['name'].tolist()
    print(f"member_bias 테이블에서 {len(member_names)}명의 의원 이름을 가져왔습니다.")
    
    # 3. 매칭되지 않는 발언자 확인
    query = """
    SELECT 발언자, COUNT(*) as 발언수
    FROM speeches
    WHERE 발언자 NOT IN (SELECT name FROM member_bias)
    GROUP BY 발언자
    ORDER BY 발언수 DESC
    LIMIT 10
    """
    non_matching = pd.read_sql_query(query, conn)
    print("\n=== 매칭되지 않는 상위 발언자 (삭제 예정) ===")
    for index, row in non_matching.iterrows():
        print(f"{row['발언자']}: {row['발언수']:,}회")
    
    # 4. 매칭되지 않는 행 수 확인
    query = """
    SELECT COUNT(*) 
    FROM speeches 
    WHERE 발언자 NOT IN (SELECT name FROM member_bias)
    """
    non_matching_count = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"\n매칭되지 않는 행 수: {non_matching_count:,}개 (삭제 예정)")
    
    # 5. 매칭되지 않는 행 삭제
    cursor.execute("""
    DELETE FROM speeches 
    WHERE 발언자 NOT IN (SELECT name FROM member_bias)
    """)
    conn.commit()
    print(f"매칭되지 않는 {non_matching_count:,}개 행을 삭제했습니다.")
    
    # 6. 필터링 후 데이터 상태 확인
    query = "SELECT COUNT(*) FROM speeches"
    after_filter = pd.read_sql_query(query, conn).iloc[0, 0]
    print(f"필터링 후 총 행 수: {after_filter:,}개")
    
    # 7. 남은 발언자 통계
    query = """
    SELECT s.발언자, COUNT(*) as 발언수, m.party
    FROM speeches s
    JOIN member_bias m ON s.발언자 = m.name
    GROUP BY s.발언자
    ORDER BY 발언수 DESC
    LIMIT 10
    """
    top_speakers = pd.read_sql_query(query, conn)
    print("\n=== 필터링 후 상위 발언자 (정당 정보 포함) ===")
    print(top_speakers)
    
    # 8. 정당별 발언 통계
    query = """
    SELECT m.party, COUNT(*) as 발언수
    FROM speeches s
    JOIN member_bias m ON s.발언자 = m.name
    GROUP BY m.party
    ORDER BY 발언수 DESC
    """
    party_stats = pd.read_sql_query(query, conn)
    print("\n=== 정당별 발언 통계 ===")
    for index, row in party_stats.iterrows():
        print(f"{row['party']}: {row['발언수']:,}회")
    
    # 데이터베이스 연결 종료
    conn.close()
    print("\nspeeches 테이블 필터링 작업이 완료되었습니다.")

if __name__ == "__main__":
    filter_speeches()
