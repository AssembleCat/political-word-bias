import sqlite3
import re
import pandas as pd
from konlpy.tag import Kkma
from collections import Counter

# 데이터베이스 파일 경로
DATABASE_NAME = 'political_speeches.db'

class SpeechTokenizer:
    """
    국회의원 발언 텍스트를 토큰화하고 불용어를 제거하는 클래스
    """
    
    def __init__(self):
        """
        초기화 함수
        """
        self.conn = sqlite3.connect(DATABASE_NAME)
        self.kkma = Kkma()
        
        # 불용어 목록 로드
        self.stopwords = self._load_stopwords()
        
        # 기존 speeches 테이블에 토큰화된 텍스트를 저장할 열 추가
        self._alter_speeches_table()
    
    def _load_stopwords(self):
        """
        불용어 목록 로드
        """
        with open('analysis/korean_stopwords.txt', 'r', encoding='utf-8') as f:
            stopwords = set(line.strip() for line in f if line.strip())
        print(f"불용어 {len(stopwords)}개를 로드했습니다.")
        return stopwords
        
    def _alter_speeches_table(self):
        """
        speeches 테이블에 토큰화된 텍스트를 저장할 열 추가
        """
        try:
            # 토큰화된 텍스트를 저장할 열 추가
            self.conn.execute("ALTER TABLE speeches ADD COLUMN 토큰화된_발언 TEXT")
            self.conn.commit()
            print("speeches 테이블에 '토큰화된_발언' 열이 추가되었습니다.")
        except sqlite3.OperationalError:
            # 이미 열이 존재하는 경우
            print("'토큰화된_발언' 열이 이미 존재합니다.")
    
    def tokenize_text(self, text):
        """
        텍스트를 형태소 분석하여 명사, 동사, 형용사 등 의미있는 품사만 추출
        """
        if not text or pd.isna(text):
            return []
        
        # 특수문자 및 숫자 제거
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\d+', ' ', text)
        
        try:
            # 형태소 분석 (Kkma는 처리 시간이 오래 걸릴 수 있음)
            pos_tagged = self.kkma.pos(text)
            
            # 의미있는 품사만 선택 (명사, 동사, 형용사)
            # Kkma 품사 태그: NNG(일반명사), NNP(고유명사), VV(동사), VA(형용사), VXV(보조동사), VXA(보조형용사)
            meaningful_tags = ['NNG', 'NNP', 'VV', 'VA', 'VXV', 'VXA']
            tokens_with_tags = [(word, tag) for word, tag in pos_tagged if tag in meaningful_tags and len(word) > 1]
            
            # 불용어 제거
            filtered_tokens = [(token, tag) for token, tag in tokens_with_tags if token not in self.stopwords]
            
            return filtered_tokens
        except Exception as e:
            print(f"형태소 분석 중 오류 발생: {e}")
            return []
    
    def update_speech(self, speech_id, tokens_with_tags):
        """
        특정 발언의 토큰화된 텍스트를 speeches 테이블에 직접 업데이트
        """
        # 토큰과 태그를 문자열로 변환 (JSON 형식)
        import json
        tokens_json = json.dumps(tokens_with_tags, ensure_ascii=False)
        
        # speeches 테이블 업데이트
        self.conn.execute(
            "UPDATE speeches SET 토큰화된_발언 = ? WHERE id = ?",
            (tokens_json, speech_id)
        )
        
        self.conn.commit()
    
    def process_speeches(self, limit=None):
        """
        모든 발언을 처리하고 토큰화하여 speeches 테이블에 직접 업데이트
        """
        # 처리할 발언 가져오기
        query = """
        SELECT id, 발언내용1, 발언내용2, 발언내용3, 발언내용4, 발언내용5, 발언내용6, 발언내용7
        FROM speeches
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        # 데이터 로드 (메모리 효율성을 위해 청크 단위로 처리)
        chunk_size = 1000
        total_processed = 0
        
        for chunk in pd.read_sql_query(query, self.conn, chunksize=chunk_size):
            print(f"발언 처리 중... ({total_processed}~{total_processed + len(chunk)})")
            
            # 발언 내용 합치기
            chunk['전체발언'] = chunk.apply(
                lambda row: ' '.join([
                    str(row['발언내용1']) if not pd.isna(row['발언내용1']) else '',
                    str(row['발언내용2']) if not pd.isna(row['발언내용2']) else '',
                    str(row['발언내용3']) if not pd.isna(row['발언내용3']) else '',
                    str(row['발언내용4']) if not pd.isna(row['발언내용4']) else '',
                    str(row['발언내용5']) if not pd.isna(row['발언내용5']) else '',
                    str(row['발언내용6']) if not pd.isna(row['발언내용6']) else '',
                    str(row['발언내용7']) if not pd.isna(row['발언내용7']) else ''
                ]), axis=1
            )
            
            # 각 발언에 대한 형태소 분석 및 토큰화
            for idx, row in chunk.iterrows():
                speech_id = row['id']
                tokens_with_tags = self.tokenize_text(row['전체발언'])
                
                # speeches 테이블 업데이트
                self.update_speech(speech_id, tokens_with_tags)
            
            total_processed += len(chunk)
            print(f"처리 완료: {total_processed}개 발언")
        
        print(f"총 {total_processed}개 발언 처리 완료")
    
    def close(self):
        """
        연결 종료
        """
        if self.conn:
            self.conn.close()
            print("데이터베이스 연결이 종료되었습니다.")

if __name__ == "__main__":
    tokenizer = SpeechTokenizer()
    
    try:
        # 명령행 인수 파싱
        import argparse
        parser = argparse.ArgumentParser(description='국회의원 발언 토큰화 도구')
        parser.add_argument('--limit', type=int, help='처리할 발언 수 제한')
        args = parser.parse_args()
        
        # 발언 처리
        print("국회의원 발언 토큰화를 시작합니다...")
        tokenizer.process_speeches(limit=args.limit)
        print("토큰화가 완료되었습니다.")
    
    finally:
        tokenizer.close()
