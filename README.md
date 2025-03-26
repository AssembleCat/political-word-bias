# political-word-bias

## 프로젝트 개요

이 프로젝트는 21대 국회의원들의 이념 성향과 언어 사용 패턴의 상관관계를 분석하기 위한 연구입니다.

**목표:**
- 21대 국회 회의록에서 사용된 단어의 빈도수를 정치적 성향별로 분석
- 정치 이념에 따라 차이가 나는 표현을 통계적으로 감지
- 단어별 정치 이념 점수 부여 시스템 개발
- 최종적으로 뉴스 기사나 정치 문서의 정치적 편향도를 측정하는 기반 구축

본 연구는 언어 사용 패턴을 통해 정치적 편향성을 객관적으로 측정할 수 있는 방법론을 제시하는 것을 목표로 합니다.

## 작업내용
- 21대 [국회회의록](https://likms.assembly.go.kr/record/mhs-60-010.do#none) 크롤링 필요, hwp/pdf 파일제공됨.
-- python hwp 조작 도구[pyhwpx](https://pypi.org/project/pyhwpx/)
-- 국내 hwp파싱 도구[pyhwp](https://pythonhosted.org/pyhwp/) [Github](https://github.com/mete0r/pyhwp)
-- hwp to txt 변환사례[링크](https://storycompiler.tistory.com/197)

- 본회의, 상임위원회, 특별위원회, 예산결산특별위원회 회의 종류가 많은데 어떤 정보를 취할것인가?

- [khaiii](https://github.com/kakao/khaiii?tab=readme-ov-file) 형태소 분석기를 이용하여 단어추출
-- 국회의원별 사용키워드-사용횟수 확보
-- 어떤 단어 종류를 확보할것인지 의사결정필요
-- [khaiii 코퍼스](https://github.com/kakao/khaiii/wiki/%EC%BD%94%ED%8D%BC%EC%8A%A4) 문서에 따르면 체언, 용언, 수식언, 독립언, 관계언 이하의 23개로 분류됨

- 특정 횟수이상, 특정 인원이상에게 언급된 키워드를 추출하고 편향도를 부여