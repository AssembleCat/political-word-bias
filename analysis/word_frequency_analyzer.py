import sqlite3
import json
import pandas as pd
from collections import Counter
import argparse

# 데이터베이스 파일 경로
DATABASE_NAME = 'political_speeches.db'

class WordFrequencyAnalyzer:
    """
    의원별 단어 사용 빈도를 분석하는 클래스
    """
    
    def __init__(self):
        """
        초기화 함수
        """
        self.conn = sqlite3.connect(DATABASE_NAME)
        
        # 의원별 단어 빈도를 저장할 테이블 생성
        self._create_frequency_tables()
    
    def _create_frequency_tables(self):
        """
        단어 빈도 분석 결과를 저장할 테이블 생성
        """
        # 의원별 단어 빈도 테이블
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS member_word_frequency (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id TEXT,
            speaker TEXT,
            party TEXT,
            word TEXT,
            tag TEXT,
            count INTEGER,
            FOREIGN KEY (speaker) REFERENCES member_bias(name)
        )
        """)
        
        self.conn.commit()
    
    def analyze_member_word_frequency(self, limit=None):
        """
        각 의원별 단어 사용 빈도 분석
        """
        # 의원 목록 가져오기
        query = """
        SELECT DISTINCT s.의원ID, s.발언자, m.party
        FROM speeches s
        JOIN member_bias m ON s.발언자 = m.name
        WHERE s.토큰화된_발언 IS NOT NULL
        """
        
        members = pd.read_sql_query(query, self.conn)
        total_members = len(members)
        
        print(f"총 {total_members}명의 의원에 대한 단어 빈도 분석을 시작합니다...")
        
        for idx, row in members.iterrows():
            member_id = row['의원ID']
            speaker = row['발언자']
            party = row['party']
            
            print(f"[{idx+1}/{total_members}] {speaker}({party}) 의원 분석 중...")
            
            # 해당 의원의 토큰화된 발언 가져오기
            query = """
            SELECT 토큰화된_발언
            FROM speeches
            WHERE 발언자 = ? AND 토큰화된_발언 IS NOT NULL
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            speeches = pd.read_sql_query(query, self.conn, params=[speaker])
            
            if speeches.empty:
                print(f"  {speaker} 의원의 토큰화된 발언이 없습니다.")
                continue
            
            # 모든 토큰 수집
            all_tokens = []
            for speech in speeches['토큰화된_발언']:
                try:
                    # 문자열을 JSON으로 파싱
                    tokens = json.loads(speech)
                    all_tokens.extend(tokens)
                except (json.JSONDecodeError, TypeError):
                    continue
            
            if not all_tokens:
                print(f"  {speaker} 의원의 토큰이 없습니다.")
                continue
            
            # 단어별 빈도 계산
            word_counts = Counter()
            for word, tag in all_tokens:
                word_counts[(word, tag)] += 1
            
            # 기존 데이터 삭제
            self.conn.execute("DELETE FROM member_word_frequency WHERE speaker = ?", (speaker,))
            
            # 새 데이터 삽입
            for (word, tag), count in word_counts.items():
                self.conn.execute(
                    """
                    INSERT INTO member_word_frequency 
                    (member_id, speaker, party, word, tag, count)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (member_id, speaker, party, word, tag, count)
                )
            
            self.conn.commit()
            print(f"  {speaker} 의원 분석 완료: {len(word_counts)}개 단어")
        
        print("모든 의원의 단어 빈도 분석이 완료되었습니다.")
    
    def get_top_words_by_member(self, speaker, limit=50):
        """
        특정 의원의 가장 많이 사용한 단어 목록 조회
        """
        query = """
        SELECT word, tag, count
        FROM member_word_frequency
        WHERE speaker = ?
        ORDER BY count DESC
        LIMIT ?
        """
        
        result = pd.read_sql_query(query, self.conn, params=[speaker, limit])
        return result
    
    def close(self):
        """
        연결 종료
        """
        if self.conn:
            self.conn.close()
            print("데이터베이스 연결이 종료되었습니다.")

if __name__ == "__main__":
    analyzer = WordFrequencyAnalyzer()
    
    try:
        # 명령행 인수 파싱
        parser = argparse.ArgumentParser(description='국회의원 단어 사용 빈도 분석 도구')
        parser.add_argument('--limit', type=int, help='의원별 분석 시 발언 수 제한')
        parser.add_argument('--speaker', type=str, help='특정 의원의 상위 단어 조회')
        parser.add_argument('--top', type=int, default=50, help='상위 단어 개수 (기본값: 50)')
        args = parser.parse_args()
        
        if args.speaker:
            # 특정 의원의 상위 단어 조회
            top_words = analyzer.get_top_words_by_member(args.speaker, args.top)
            print(f"{args.speaker} 의원의 상위 {args.top}개 단어:")
            print(top_words)
        else:
            # 의원별 단어 빈도 분석
            analyzer.analyze_member_word_frequency(limit=args.limit)
            print("단어 빈도 분석이 완료되었습니다.")
    
    finally:
        analyzer.close()
