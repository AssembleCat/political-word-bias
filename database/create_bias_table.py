import sqlite3
import pandas as pd
import os
from database import DATABASE_NAME

def create_bias_table():
    """
    의원들의 정치적 편향 정보를 담은 테이블을 생성하는 함수
    """
    print("의원 편향 정보 테이블 생성을 시작합니다...")
    
    # 데이터베이스 연결
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # 기존 테이블이 있으면 삭제
    cursor.execute("DROP TABLE IF EXISTS member_bias")
    
    # 새 테이블 생성
    cursor.execute('''
    CREATE TABLE member_bias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        party TEXT,
        name TEXT,
        coord1D REAL,
        coord2D REAL
    )
    ''')
    
    # wnominate_results.csv 파일 읽기
    csv_path = '../data/wnominate_results.csv'
    
    if not os.path.exists(csv_path):
        print(f"파일을 찾을 수 없습니다: {csv_path}")
        conn.close()
        return
    
    # CSV 파일 읽기
    df = pd.read_csv(csv_path)
    
    # 필요한 열만 선택하고 이름 변경
    df = df[['party', 'name', 'coord1D', 'coord2D']]
    
    # 데이터베이스에 삽입
    df.to_sql('member_bias', conn, if_exists='append', index=False)
    
    print(f"총 {len(df):,}개 행이 member_bias 테이블에 추가되었습니다.")
    
    # 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bias_name ON member_bias(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bias_party ON member_bias(party)')
    
    # 데이터베이스 연결 종료
    conn.commit()
    conn.close()
    
    print("\n의원 편향 정보 테이블 생성이 완료되었습니다.")

if __name__ == "__main__":
    create_bias_table()
