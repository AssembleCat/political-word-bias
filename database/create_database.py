import sqlite3
import pandas as pd
import os
import glob
from database import DATABASE_NAME

def create_database():
    """
    엑셀 파일에서 데이터를 읽어 SQLite 데이터베이스를 생성하는 함수
    """
    print("데이터베이스 생성을 시작합니다...")
    
    # 데이터베이스 연결
    conn = sqlite3.connect(DATABASE_NAME)
    
    # 기존 테이블이 있으면 삭제
    conn.execute("DROP TABLE IF EXISTS speeches")
    
    # 새 테이블 생성
    conn.execute('''
    CREATE TABLE speeches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        회의번호 TEXT,
        의원ID TEXT,
        발언자 TEXT,
        발언내용1 TEXT,
        발언내용2 TEXT,
        발언내용3 TEXT,
        발언내용4 TEXT,
        발언내용5 TEXT,
        발언내용6 TEXT,
        발언내용7 TEXT
    )
    ''')
    
    # 인덱스 생성
    conn.execute('CREATE INDEX idx_member_id ON speeches(의원ID)')
    conn.execute('CREATE INDEX idx_speaker ON speeches(발언자)')
    
    # 엑셀 파일 목록 가져오기
    excel_files = glob.glob('../data/*.xlsx')
    
    if not excel_files:
        print("데이터 폴더에 엑셀 파일이 없습니다.")
        conn.close()
        return
    
    total_rows = 0
    
    # 각 엑셀 파일 처리
    for file in excel_files:
        print(f"{os.path.basename(file)} 파일 처리 중...")
        
        # 엑셀 파일 읽기
        df = pd.read_excel(file)
        
        # 필요한 열만 선택
        if '발언내용8' in df.columns:
            cols = ['회의번호', '의원ID', '발언자', '발언내용1', '발언내용2', '발언내용3', 
                    '발언내용4', '발언내용5', '발언내용6', '발언내용7']
        else:
            cols = df.columns
        
        df = df[cols]
        
        # 데이터베이스에 삽입
        df.to_sql('speeches', conn, if_exists='append', index=False)
        
        total_rows += len(df)
        print(f"  - {len(df):,}개 행 추가됨")
    
    # 데이터베이스 연결 종료
    conn.commit()
    conn.close()
    
    print(f"\n총 {total_rows:,}개 행이 데이터베이스에 추가되었습니다.")
    print("데이터베이스 생성이 완료되었습니다.")

if __name__ == "__main__":
    create_database()
