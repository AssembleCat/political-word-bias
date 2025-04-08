import os
import pandas as pd
import sqlite3
from pathlib import Path

DATABASE_NAME = 'political_speeches.db'

def create_database():
    """
    모든 엑셀 파일을 읽어서 SQLite 데이터베이스에 저장하는 함수
    """
    # 데이터 디렉토리 경로
    data_dir = Path("data")
    
    # 데이터베이스 연결
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # 테이블 생성 (첫 실행 시에만)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS speeches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        회의번호 TEXT,
        회의록구분 TEXT,
        대수 TEXT,
        회의구분 TEXT,
        위원회 TEXT,
        회수 TEXT,
        차수 TEXT,
        기타정보 TEXT,
        회의일자 TEXT,
        안건 TEXT,
        발언자 TEXT,
        의원ID TEXT,
        발언순번 TEXT,
        발언내용1 TEXT,
        발언내용2 TEXT,
        발언내용3 TEXT,
        발언내용4 TEXT,
        발언내용5 TEXT,
        발언내용6 TEXT,
        발언내용7 TEXT
    )
    ''')
    
    # 모든 .xlsx 파일 찾기
    xlsx_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.xlsx'):
                xlsx_files.append(os.path.join(root, file))
    
    total_files = len(xlsx_files)
    print(f"총 {total_files}개의 엑셀 파일을 처리합니다.")
    
    # 각 파일 처리
    for i, file_path in enumerate(xlsx_files, 1):
        try:
            print(f"[{i}/{total_files}] 처리 중: {file_path}")
            
            df = pd.read_excel(file_path)
            
            # 컬럼 이름 확인 및 필요한 컬럼만 선택
            expected_columns = [
                '회의번호', '회의록구분', '대수', '회의구분', '위원회', '회수', '차수', 
                '기타정보', '회의일자', '안건', '발언자', '의원ID', '발언순번', 
                '발언내용1', '발언내용2', '발언내용3', '발언내용4', '발언내용5', 
                '발언내용6', '발언내용7'
            ]
            
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                print(f"경고: {file_path}에 다음 컬럼이 없습니다: {missing_columns}")
                # 없는 컬럼은 빈 값으로 추가
                for col in missing_columns:
                    df[col] = None
            
            # 순서 맞추기
            df = df[expected_columns]
            
            # 저장장
            df.to_sql('speeches', conn, if_exists='append', index=False)
            
            print(f"  - {len(df)}개의 행이 추가되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {file_path} - {str(e)}")
    
    # 인덱스 생성 (검색 성능 향상을 위해)
    print("인덱스를 생성합니다...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_meeting_id ON speeches(회의번호)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_speaker ON speeches(발언자)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_committee ON speeches(위원회)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON speeches(회의일자)')
    
    # 데이터베이스 연결 종료
    conn.commit()
    conn.close()
    
    print("데이터베이스 생성이 완료되었습니다.")
    print(f"파일명: {DATABASE_NAME}")

if __name__ == "__main__":
    create_database()
