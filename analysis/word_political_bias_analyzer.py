import sqlite3
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LinearRegression
import argparse
import os

DATABASE_NAME = 'political_speeches.db'

class WordPoliticalBiasAnalyzer:
    """
    단어의 정치적 편향성을 분석하는 클래스
    """
    
    def __init__(self, wnominate_file):
        """
        초기화 함수
        
        Args:
            wnominate_file: 의원별 정치적 위치 정보가 담긴 CSV 파일 경로
        """
        self.conn = sqlite3.connect(DATABASE_NAME)
        self.wnominate_data = pd.read_csv(wnominate_file)
        print(f"정치적 위치 데이터 로드: {len(self.wnominate_data)}명의 의원 정보")
    
    def load_word_frequency_data(self):
        """
        의원별 단어 빈도 데이터 로드
        """
        query = """
        SELECT speaker, word, tag, count
        FROM member_word_frequency
        """
        
        word_freq_df = pd.read_sql_query(query, self.conn)
        print(f"단어 빈도 데이터 로드: {len(word_freq_df)}개 행, {word_freq_df['speaker'].nunique()}명의 의원")
        
        return word_freq_df
    
    def create_word_speaker_matrix(self, word_freq_df, min_word_count=10):
        """
        의원-단어 행렬 생성
        
        Args:
            word_freq_df: 단어 빈도 데이터프레임
            min_word_count: 최소 등장 횟수 (이 횟수 미만으로 등장한 단어는 제외)
        
        Returns:
            의원-단어 행렬, 단어 목록
        """
        print(f"의원-단어 행렬 생성 중...")
        
        # 각 의원별 총 단어 수 계산
        speaker_total_words = word_freq_df.groupby('speaker')['count'].sum().reset_index()
        speaker_total_words.columns = ['speaker', 'total_words']
        
        # 단어 빈도 데이터에 총 단어 수 정보 병합
        word_freq_df = pd.merge(word_freq_df, speaker_total_words, on='speaker')
        
        # 단어 사용 비율 계산 (정규화)
        word_freq_df['ratio'] = word_freq_df['count'] / word_freq_df['total_words']
        
        # 최소 등장 횟수 필터링
        word_counts = word_freq_df.groupby('word')['count'].sum()
        frequent_words = word_counts[word_counts >= min_word_count].index.tolist()
        word_freq_df = word_freq_df[word_freq_df['word'].isin(frequent_words)]
        
        print(f"최소 {min_word_count}회 이상 등장한 단어 {len(frequent_words)}개 선택")
        
        # 의원-단어 행렬 생성
        pivot_df = word_freq_df.pivot_table(
            index='speaker', 
            columns='word', 
            values='ratio',
            fill_value=0
        )
        
        # TF-IDF 변환 적용
        tfidf = TfidfTransformer()
        tfidf_matrix = tfidf.fit_transform(pivot_df.values)
        
        # 변환된 행렬을 데이터프레임으로 변환
        tfidf_df = pd.DataFrame(
            tfidf_matrix.toarray(),
            index=pivot_df.index,
            columns=pivot_df.columns
        )
        
        print(f"TF-IDF 변환 완료: {tfidf_df.shape[0]}명의 의원, {tfidf_df.shape[1]}개의 단어")
        
        return tfidf_df, list(tfidf_df.columns)
    
    def train_regression_model(self, word_speaker_matrix, words):
        """
        선형 회귀 모델 학습 (1차원 편향만 사용)
        
        Args:
            word_speaker_matrix: 의원-단어 행렬 (TF-IDF 적용됨)
            words: 단어 목록
        
        Returns:
            단어별 정치적 편향 점수
        """
        print("선형 회귀 모델 학습 중...")
        
        # 의원-단어 행렬과 정치적 위치 데이터 병합
        X_df = word_speaker_matrix.reset_index()
        merged_df = pd.merge(X_df, self.wnominate_data, left_on='speaker', right_on='name')
        
        if merged_df.empty:
            raise ValueError("의원-단어 행렬과 정치적 위치 데이터를 병합할 수 없습니다. 의원 이름이 일치하는지 확인하세요.")
        
        # 독립 변수 (단어 사용 비율)
        X = merged_df[words].values
        
        # 종속 변수 (정치적 위치 x)
        y = merged_df['coord1D'].values
        
        # 회귀 모델 학습
        model = LinearRegression()
        model.fit(X, y)
        
        # 단어별 정치적 편향 점수 계산
        word_bias = pd.DataFrame({
            'word': words,
            'bias_score': model.coef_
        })
        
        # 절대값이 큰 순서로 정렬
        word_bias['abs_bias'] = word_bias['bias_score'].abs()
        word_bias = word_bias.sort_values('abs_bias', ascending=False).reset_index(drop=True)
        word_bias = word_bias[['word', 'bias_score']]
        
        print(f"회귀 모델 학습 완료: {len(word_bias)}개 단어의 정치적 편향 계산")
        
        return word_bias
    
    def analyze_word_political_bias(self, min_word_count=10, output_file='word_political_bias_1d.csv'):
        """
        단어의 정치적 편향성 분석 수행
        
        Args:
            min_word_count: 최소 등장 횟수
            output_file: 결과를 저장할 CSV 파일 경로
        """
        # 단어 빈도 데이터 로드
        word_freq_df = self.load_word_frequency_data()
        
        # 의원-단어 행렬 생성
        word_speaker_matrix, words = self.create_word_speaker_matrix(word_freq_df, min_word_count)
        
        # 회귀 모델 학습 및 단어별 정치적 편향 계산
        word_bias = self.train_regression_model(word_speaker_matrix, words)
        
        # 결과 저장
        word_bias.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"단어별 정치적 편향 결과가 {output_file}에 저장되었습니다.")
        
        return word_bias
    
    def close(self):
        """
        연결 종료
        """
        if self.conn:
            self.conn.close()
            print("데이터베이스 연결이 종료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='단어의 정치적 편향성 분석 도구')
    parser.add_argument('--wnominate', type=str, default='wnominate_results.csv',
                        help='의원별 정치적 위치 정보가 담긴 CSV 파일 경로')
    parser.add_argument('--min-count', type=int, default=10,
                        help='최소 단어 등장 횟수 (기본값: 10)')
    parser.add_argument('--output', type=str, default='word_political_bias_1d.csv',
                        help='결과를 저장할 CSV 파일 경로')
    args = parser.parse_args()
    
    analyzer = WordPoliticalBiasAnalyzer(args.wnominate)
    
    try:
        analyzer.analyze_word_political_bias(min_word_count=args.min_count, output_file=args.output)
    finally:
        analyzer.close()
