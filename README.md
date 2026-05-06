# pgxllm

PostgreSQL 데이터베이스를 위한 **Text-to-SQL 시스템**입니다.  
자연어 질문을 SQL로 변환하고, 쿼리 튜닝·분석·스키마 탐색을 Web UI에서 통합 제공합니다.

---

## 목차

1. [시스템 개요](#1-시스템-개요)
2. [아키텍처](#2-아키텍처)
3. [설치 및 시작](#3-설치-및-시작)
4. [설정](#4-설정)
5. [Web UI 사용 가이드](#5-web-ui-사용-가이드)
6. [LLM Provider 설정](#6-llm-provider-설정)
7. [CLI 명령어](#7-cli-명령어)
8. [REST API 레퍼런스](#8-rest-api-레퍼런스)
9. [개발 가이드](#9-개발-가이드)

---

## 1. 시스템 개요

```
자연어 질문
    ↓
┌─────────────────────────────────────┐
│         pgxllm Core Pipeline        │
│  S1: 질문 이해 → S2: 스키마 링킹    │
│  S3: SQL 생성  → S4: 검증·보정      │
└─────────────────────────────────────┘
    ↓
SQL + 설명
```

### 핵심 기능

| 기능 | 설명 |
|---|---|
| **Text-to-SQL** | 자연어 → SQL 자동 변환 (LLM 기반 4단계 파이프라인) |
| **다중 LLM 지원** | Ollama / vLLM / LM Studio / OpenAI / Anthropic / IBM watsonx.ai |
| **스키마 탐색** | pg_catalog 기반 테이블·컬럼·인덱스·통계 조회 |
| **쿼리 분석** | pg_stat_statements 수집 → 실행계획 시각화 (EXPLAIN ANALYZE) |
| **쿼리 튜닝** | LLM 기반 쿼리 최적화 제안 |
| **Graph 관계** | FK·분석 기반 테이블 간 관계 그래프 관리 |
| **Dialect Rules** | 컬럼별 SQL 작성 규칙 관리 (자동 감지 + 수동 등록) |
| **Semantic Cache** | TF-IDF 기반 유사 질문 캐시로 LLM 호출 최소화 |

---

## 2. 아키텍처

### 컴포넌트 구성

```
pgxllm/
├── src/pgxllm/
│   ├── core/                     # Core Pipeline (Text-to-SQL)
│   │   ├── llm/                  # LLM Provider 추상화
│   │   │   ├── ollama.py
│   │   │   ├── vllm.py           # vLLM / LM Studio / OpenAI 호환
│   │   │   ├── anthropic_provider.py
│   │   │   ├── watsonx.py        # IBM watsonx.ai (IAM 토큰 자동 발급)
│   │   │   └── factory.py        # config → provider 인스턴스 생성
│   │   ├── s1_understanding.py   # S1: 질문 이해 · 패턴 감지
│   │   ├── s2_schema_linking.py  # S2: 스키마 링킹 · JOIN 경로
│   │   ├── s3_generation.py      # S3: LLM SQL 생성
│   │   ├── s4_validation.py      # S4: SQL 검증 · 자동 보정
│   │   └── pipeline.py           # 파이프라인 조율
│   ├── intelligence/             # 스키마 수집 · 규칙 · 패턴
│   │   ├── refresh.py            # pg_catalog 스캔
│   │   ├── rule_engine.py        # Dialect Rules
│   │   ├── pattern_engine.py     # Dynamic Patterns
│   │   └── db_registry.py        # Target DB 등록 관리
│   ├── graph/                    # 관계 그래프 백엔드
│   │   ├── postgresql.py         # PostgreSQL 기반 (기본)
│   │   ├── age.py                # Apache AGE
│   │   └── neo4j.py              # Neo4j
│   ├── cache/                    # Semantic Cache (TF-IDF)
│   ├── parser/                   # SQL AST Parser (ANTLR4)
│   ├── web/app.py                # FastAPI REST API
│   └── cli.py                    # CLI (자동화·설정 전용)
└── frontend/                     # React + Vite Web UI
    └── src/pages/
        ├── QueryPage.jsx         # SQL 실행 + LLM 쿼리
        ├── SchemaPage.jsx        # 스키마 탐색
        ├── GraphRulesPages.jsx   # Graph · pg_stat · Rules
        ├── DbsPage.jsx           # DB 관리
        └── LLMSettingsPage.jsx   # LLM Provider 설정
```

### Core Pipeline 데이터 흐름

```
질문 입력
  │
  ├─[캐시 HIT]──→ 캐시된 SQL 즉시 반환
  │
  └─[캐시 MISS]
       │
       ▼
  S1. QuestionUnderstanding
      · DynamicPattern 감지 (TOP-N, GROUP BY 등)
      · 키워드 추출 → 후보 테이블 검색 (pg_trgm)
       │
       ▼
  S2. SchemaLinker
      · 후보 테이블 컬럼 상세 로드
      · Graph에서 JOIN 경로 탐색
      · Dialect Rules 수집
       │
       ▼
  S3. SQLGenerator (LLM 호출)
      · System Prompt: 규칙 + 패턴 + Few-shot
      · User Prompt: 스키마 + 질문
      · SQL 파싱
       │
       ▼
  S4. SQLValidator
      · EXPLAIN으로 문법 검증
      · 오류 시 LLM 재생성 (최대 3회)
       │
       ▼
  캐시 저장 → SQL 반환
```

### Internal DB 테이블 구조

pgxllm은 **별도의 PostgreSQL** 인스턴스를 내부 메타데이터 저장소로 사용합니다.

| 테이블 | 용도 |
|---|---|
| `pgxllm.db_registry` | 등록된 Target DB 목록 |
| `pgxllm.schema_catalog` | 수집된 스키마 정보 (테이블·컬럼·통계) |
| `pgxllm.graph_edges` | 테이블 간 관계 (FK / 분석 / 추론 / 수동) |
| `pgxllm.graph_paths` | 사전 계산된 JOIN 경로 |
| `pgxllm.dialect_rules` | 컬럼별 SQL 작성 규칙 |
| `pgxllm.sql_patterns` | Dynamic SQL 패턴 |
| `pgxllm.query_cache` | Semantic Cache (TF-IDF) |
| `pgxllm.query_history` | 쿼리 실행 이력 |
| `pgxllm.llm_settings` | LLM 설정 (Web UI 저장 시) |

---

## 3. 설치 및 시작

### 요구사항

- Python 3.11+
- Node.js 18+ (프론트엔드 빌드용)
- PostgreSQL 14+ (Internal DB 및 Target DB)

### 설치

```bash
# 1. 저장소 클론
git clone <repo-url> pgxllm
cd pgxllm

# 2. Python 환경 + 의존성 설치
bash setup.sh
source .venv/bin/activate

# 3. 환경 변수 설정
cp .env.example .env
# .env 파일에서 Internal DB 접속 정보 입력
```

### 첫 실행

```bash
# Target DB 등록
pgxllm db register \
  --alias mydb \
  --host localhost \
  --user postgres \
  --password mypassword \
  --dbname mydb

# 스키마 수집
pgxllm db refresh --alias mydb

# 서버 시작 (프로덕션: 프론트엔드 빌드 포함)
make serve
```

브라우저에서 `http://서버IP:8000` 접속.

### 개발 모드 (두 터미널)

```bash
# 터미널 1
make dev-backend    # FastAPI + hot-reload (포트 8000)

# 터미널 2
make dev-frontend   # Vite + HMR (포트 5173)
```

브라우저에서 `http://localhost:5173` 접속.

### 외부 접속

서버는 기본적으로 `0.0.0.0`에 바인딩됩니다. 방화벽에서 해당 포트만 열면 됩니다.

```bash
make serve PORT=80          # 포트 80으로 서비스
make serve HOST=0.0.0.0 PORT=8000
```

---

## 4. 설정

### configs/default.yaml

```yaml
# Internal DB — pgxllm 메타데이터 저장소
internal_db:
  host:     ${PGXLLM_HOST:-localhost}
  port:     ${PGXLLM_PORT:-5432}
  user:     ${PGXLLM_USER:-postgres}
  password: ${PGXLLM_PASSWORD:-}
  dbname:   ${PGXLLM_DBNAME:-pgxllm}
  schema:   pgxllm

# LLM 설정 (Web UI의 LLM 설정 페이지에서도 변경 가능)
llm:
  provider:    ollama          # ollama | vllm | lmstudio | openai | anthropic | watsonx
  base_url:    http://localhost:11434
  model:       qwen2.5-coder:7b
  timeout:     600             # 초 (LLM 응답 대기 최대 시간)
  max_tokens:  2048
  temperature: 0.0             # 0.0 = 결정적, 높을수록 다양

# Semantic Cache
cache:
  backend: tfidf
  tfidf:
    similarity_threshold: 0.75  # 0~1, 높을수록 엄격
    top_k: 5

# 관계 그래프 백엔드
graph:
  backend:    postgresql        # postgresql | age | neo4j
  max_depth:  4                 # JOIN 경로 최대 탐색 깊이

# SQL AST 파서
parser:
  max_depth: 5
```

`${VAR:-default}` 형식으로 환경 변수를 YAML 내에서 직접 참조합니다.

### 환경 변수 (.env)

```bash
# Internal DB 접속 정보
PGXLLM_HOST=localhost
PGXLLM_PORT=5432
PGXLLM_USER=postgres
PGXLLM_PASSWORD=mypassword
PGXLLM_DBNAME=pgxllm

# LLM API Keys (선택 — Web UI에서도 설정 가능)
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
WATSONX_API_KEY=...
WATSONX_PROJECT_ID=...
```

---

## 5. Web UI 사용 가이드

### 화면 레이아웃

```
┌──────────────┬────────────────────────────────────────────────┐
│   SIDEBAR    │  TOPBAR: 페이지명  🤖 현재LLM·모델  [DB 선택▼] │
│              ├────────────────────────────────────────────────┤
│ ▶ SQL 실행   │                                                │
│              │                 PAGE CONTENT                   │
│ CATALOG      │                                                │
│ 🗂 Schema탐색 │                                                │
│              │                                                │
│ GRAPH        │                                                │
│ 🔗 Graph관계  │                                                │
│   📊 pg_stat  │                                                │
│     📋 Query분석│                                              │
│     🔧 Query튜닝│                                              │
│              │                                                │
│ RULES        │                                                │
│ 📐 Dialect Rules│                                             │
│              │                                                │
│ ADMIN        │                                                │
│ 🗄 DB 관리    │                                                │
│ 🤖 LLM 설정   │                                                │
└──────────────┴────────────────────────────────────────────────┘
```

**Topbar 배지**: 모든 페이지에서 `🤖 Provider · 모델명` 형식으로 현재 LLM을 확인할 수 있습니다.

---

### 5.1 SQL 실행 (`/query`)

직접 SQL 실행과 LLM 자연어 질문을 하나의 화면에서 처리합니다.

**실행 모드 전환**

| 모드 | 설명 |
|---|---|
| **직접 실행** | SQL 에디터에 직접 입력하여 실행 |
| **LLM 쿼리** | 자연어 질문 → Core Pipeline → SQL 자동 생성 후 실행 |

**LLM 쿼리 처리 흐름**

1. 자연어 질문 입력 (예: `2023년 월별 주문 건수를 알려줘`)
2. Semantic Cache 확인 → 유사 질문이 있으면 즉시 반환
3. Cache MISS 시 S1~S4 파이프라인 실행
4. 생성된 SQL · 설명 · 단계별 로그 표시
5. 실행 결과 테이블 표시
6. 피드백: 👍(캐시 저장) / 재생성 요청 가능

**히스토리**

- 모든 실행 이력을 조회 · 재실행 · 삭제 가능
- 모드별 필터링 (직접 실행 / LLM 쿼리)
- 캐시 일괄 삭제 지원

---

### 5.2 Schema 탐색 (`/schema`)

pg_catalog에서 수집한 스키마 정보를 탐색합니다.

**조회 정보**

- 테이블 목록 (스키마 · 코멘트 · 컬럼 수 · 인덱스 수)
- 컬럼별: 이름 · 타입 · FK 참조 · 코멘트 · n_distinct · 샘플값
- 인덱스: 이름 · 유형(PK/UQ/IDX) · 컬럼 · 정의

**인덱스 배지**

| 배지 | 색상 | 의미 |
|---|---|---|
| `PK` | 노랑 | Primary Key |
| `UQ` | 보라 | Unique Index |
| `IDX` | 파랑 | 일반 Index |

**↻ 스키마 재수집**

버튼 클릭 시 pg_catalog 재스캔 실행.  
완료 후 `테이블 N개 / 소요시간s / 수집일시` 표시.

**검색**

테이블명 · 컬럼명 · 코멘트를 통합 검색합니다.

---

### 5.3 Graph 관계 (`/graph`)

테이블 간 관계를 관리하고 JOIN 경로를 탐색합니다.

**관계 유형**

| 유형 | 수집 방법 |
|---|---|
| `fk` | PostgreSQL FK 제약 조건 자동 감지 |
| `analyzed` | pg_stat_statements의 JOIN 패턴 분석 |
| `inferred` | LLM 컬럼명 추론 |
| `manual` | 사용자 수동 등록 |

**승인 워크플로우**

자동 수집 관계는 미승인 상태로 저장됩니다.  
검토 후 개별 또는 일괄 승인해야 SQL 생성의 JOIN 힌트에 활용됩니다.

**JOIN 경로 탐색**

시작 테이블 → 목적 테이블까지의 JOIN 경로를 미리 계산하여 표시합니다.  
이 경로가 S2 스키마 링킹에서 LLM에게 제공됩니다.

---

### 5.4 Query 분석 (`/pgstat/analyze`)

`pg_stat_statements`에서 수집한 실 운영 쿼리를 분석합니다.

**수집 → 분석 흐름**

```
pg_stat 수집 (기간/횟수/시간 기준 필터)
    ↓
쿼리 목록 (평균 실행시간 / 총 시간 정렬)
    ↓
개별 쿼리 선택
    ├─ 📋 분석: LLM이 자연어 설명 + 관계 추론
    ├─ 📊 Plan: EXPLAIN 실행계획 그래프
    └─ 📊 ANALYZE: EXPLAIN ANALYZE (실제 수행시간)
```

**실행계획 시각화**

- 방향성 그래프: 리프 노드 → 루트 방향 (데이터 흐름)
- 노드별 **독점 수행시간(Exclusive Time)** 표시
  - `exclusive = 자신의 총 시간 - 자식 노드 합계`
  - 모든 노드의 exclusive % 합 ≈ 100% (분석이 용이)
- 노드 열 색상 (실행 비중):

  | 색상 | 범위 | 의미 |
  |---|---|---|
  | 초록 | < 10% | 정상 |
  | 노랑 | 10~25% | 주의 |
  | 주황 | 25~50% | 병목 가능성 |
  | 빨강 | ≥ 50% | 주요 병목 |

- 노드별 조건 표시:

  | 아이콘 | 조건 유형 |
  |---|---|
  | 🔑 | Index Cond / Recheck Cond |
  | ⊕ | Hash Cond / Merge Cond |
  | ⚡ | Join Filter |
  | ▽ | Filter |
  | ↕ | Sort Key |
  | ⊞ | Group Key |

**실행계획 내보내기**

Plan 모달 하단의 통합 저장 폼에서 형식을 선택하고 파일명을 입력한 뒤 저장합니다.

| 형식 | 내용 |
|---|---|
| JSON | SQL · 타이밍 · plan 구조체 (기계 처리용) |
| TEXT | SQL 전문 + plan JSON (사람이 읽는 텍스트) |
| PNG | 플랜 그래프 이미지 (2× 고해상도) |
| PPT | 슬라이드 2장 — 1장: SQL Query / 2장: 실행계획 그래프 |

> Chrome/Edge에서는 OS 네이티브 "다른 이름으로 저장" 다이얼로그가 열립니다.

**$N 파라미터 처리**

pg_stat_statements는 리터럴 값을 `$1`, `$2`로 추상화합니다.

- `$N` 변수는 에디터에서 빨간색으로 강조
- 파라미터 입력창에 실제 값을 입력하면 Plan/ANALYZE 실행 시 자동 치환
- 타입 자동 처리: 숫자·boolean·null → 그대로 / 문자열 → `'...'`

---

### 5.5 Query 튜닝 (`/pgstat/tune`)

수집된 쿼리에 대한 LLM 최적화 제안을 받습니다.

**기능**

| 버튼 | 설명 |
|---|---|
| `🔧 튜닝 제안` | LLM이 쿼리 분석 + 인덱스 추가/재작성 등 최적화 제안 생성 |
| `📊 Plan` | EXPLAIN 실행계획 그래프 |
| `📊 ANALYZE` | EXPLAIN ANALYZE 실행 (실제 수행시간 측정) |

- 쿼리 직접 편집 가능 (수정 후 재분석)
- $N 파라미터 입력 후 Plan/ANALYZE 실행 지원

---

### 5.6 Dialect Rules (`/rules`)

컬럼별 SQL 작성 규칙을 관리합니다. 등록된 규칙은 LLM SQL 생성 시 System Prompt에 자동 주입됩니다.

**규칙 예시**

- `orders.status`: `= 'ACTIVE'` 대신 `= 1` 사용
- `products.price`: NULL 처리 시 반드시 `COALESCE(price, 0)` 적용

**자동 감지**

스키마 재수집 시 컬럼 데이터 패턴을 분석해 코드성 컬럼·FK 관계 등을 규칙으로 자동 등록합니다.

---

### 5.7 DB 관리 (`/dbs`)

Target DB 등록·관리를 Web UI에서 처리합니다.

**기능**

- 등록된 DB 목록 및 연결 상태(✅/⚠) 확인
- 신규 DB 등록 폼
- 스키마 재수집 버튼
- DB 삭제

---

### 5.8 LLM 설정 (`/llm`)

LLM Provider를 Web UI에서 변경합니다. **서버 재시작 없이** 즉시 적용됩니다.

**현재 활성 LLM 확인**

- **모든 페이지 Topbar**: `🤖 Provider · 모델명` 배지
- **LLM 설정 페이지 상단**: Provider · Model · Endpoint · API Key 여부 · 파라미터 상세 카드

**설정 항목**

| 항목 | 설명 |
|---|---|
| Provider | LLM 서비스 선택 (버튼으로 전환) |
| Endpoint URL | API 서버 주소 |
| 모델 ID | 사용할 모델명 |
| API Key | 인증 키 (저장 후 마스킹 표시) |
| Project ID | watsonx.ai 전용 프로젝트 식별자 |
| Timeout | LLM 응답 대기 최대 시간 (초) |
| Max Tokens | 최대 생성 토큰 수 |
| Temperature | 생성 다양성 (0.0 = 결정적) |

**저장 우선순위**: Web UI 저장 설정 > `configs/default.yaml`

---

## 6. LLM Provider 설정

### 6.1 Ollama (로컬, 기본값)

```bash
# Ollama 설치 후 모델 다운로드
ollama pull qwen2.5-coder:7b
```

| 항목 | 값 |
|---|---|
| Provider | `ollama` |
| Endpoint URL | `http://localhost:11434` |
| 모델 | `qwen2.5-coder:7b` / `qwen2.5-coder:32b` / `codellama:13b` |
| API Key | 불필요 |

---

### 6.2 vLLM

```bash
# vLLM 서버 실행 예시
python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2.5-Coder-7B-Instruct \
  --port 8001
```

| 항목 | 값 |
|---|---|
| Provider | `vllm` |
| Endpoint URL | `http://localhost:8001/v1` |
| 모델 | 서버에 로드된 모델명 |

---

### 6.3 LM Studio

LM Studio에서 서버 모드를 활성화합니다.

| 항목 | 값 |
|---|---|
| Provider | `lmstudio` |
| Endpoint URL | `http://localhost:1234/v1` |
| 모델 | LM Studio에서 로드한 모델명 |

---

### 6.4 OpenAI

```bash
export OPENAI_API_KEY=sk-...
```

또는 Web UI LLM 설정에서 API Key 직접 입력.

| 항목 | 권장값 |
|---|---|
| Provider | `openai` |
| 모델 | `gpt-4o`, `gpt-4-turbo`, `gpt-3.5-turbo` |

---

### 6.5 Anthropic Claude

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

| 항목 | 권장값 |
|---|---|
| Provider | `anthropic` |
| 모델 | `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229` |

---

### 6.6 IBM watsonx.ai

두 가지 배포 환경을 지원합니다.

#### 6.6.1 IBM Cloud (퍼블릭 클라우드)

IBM Cloud API Key와 watsonx.ai Project ID가 필요합니다.

**사전 준비**

1. [IBM Cloud](https://cloud.ibm.com) 로그인
2. **API 키 생성**: 관리 → IAM → API 키 → 생성
3. **Project ID 확인**: [watsonx.ai](https://dataplatform.cloud.ibm.com) → 프로젝트 → 설정 → General

**지역별 Endpoint URL**

| 지역 | Endpoint |
|---|---|
| 미국 남부 (기본) | `https://us-south.ml.cloud.ibm.com` |
| 독일 (EU) | `https://eu-de.ml.cloud.ibm.com` |
| 영국 | `https://eu-gb.ml.cloud.ibm.com` |
| 일본 | `https://jp-tok.ml.cloud.ibm.com` |
| 호주 | `https://au-syd.ml.cloud.ibm.com` |

**환경 변수**

```bash
export WATSONX_API_KEY=your-ibm-api-key
export WATSONX_PROJECT_ID=your-project-uuid
```

`configs/default.yaml`:
```yaml
llm:
  provider:  watsonx
  base_url:  https://us-south.ml.cloud.ibm.com
  model:     ibm/granite-34b-code-instruct
```

> IAM 토큰은 자동으로 발급·갱신됩니다 (만료 5분 전 자동 갱신).

---

**주요 모델 ID**

| 모델 | 특징 |
|---|---|
| `ibm/granite-34b-code-instruct`   | IBM 코드 특화 대형 모델 |
| `ibm/granite-8b-code-instruct`    | 경량 코드 모델 |
| `meta-llama/llama-3-70b-instruct` | Meta Llama 3 70B |
| `mistralai/mistral-large`         | Mistral Large |

---

## 7. CLI 명령어

CLI는 **자동화·초기 설정** 목적으로 사용합니다. 탐색·분석·관리는 Web UI를 권장합니다.

### pgxllm db register

Target DB를 pgxllm에 등록합니다.

```bash
pgxllm db register \
  --alias mydb \
  --host db.example.com \
  --port 5432 \
  --user postgres \
  --password secret \
  --dbname mydb \
  --schema-mode include \
  --schemas "public,sales,hr"
```

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--alias` | 필수 | DB 식별자 (고유) |
| `--host` | 필수 | 호스트 주소 |
| `--port` | `5432` | 포트 번호 |
| `--user` | `postgres` | 사용자명 |
| `--password` | (없음) | 비밀번호 |
| `--dbname` | alias와 동일 | 데이터베이스 이름 |
| `--schema-mode` | `exclude` | `include`: 지정 스키마만 / `exclude`: 지정 스키마 제외 |
| `--schemas` | 시스템 스키마 목록 | 포함/제외할 스키마 (콤마 구분) |
| `--overwrite` | false | 기존 등록 덮어쓰기 |

### pgxllm db refresh

pg_catalog 스캔 + 샘플 추출 + Rule 감지 + FK 그래프 수집을 실행합니다.

```bash
# 단일 DB
pgxllm db refresh --alias mydb

# 전체 DB (cron 자동화 권장)
pgxllm db refresh --all

# 수집 단계 선택적 건너뛰기
pgxllm db refresh --alias mydb --skip-samples --skip-rules --skip-graph
```

| 옵션 | 설명 |
|---|---|
| `--alias` | 대상 DB alias |
| `--all` | 등록된 모든 DB 대상 |
| `--table` | 특정 테이블만 재수집 |
| `--skip-samples` | 샘플 데이터 추출 건너뜀 |
| `--skip-rules` | Dialect Rule 감지 건너뜀 |
| `--skip-graph` | FK 그래프 수집 건너뜀 |

**cron 자동화 예시**

```bash
# 매일 새벽 2시 전체 DB 스키마 갱신
0 2 * * * cd /opt/pgxllm && .venv/bin/pgxllm db refresh --all
```

### pgxllm web

Web UI + REST API 서버를 시작합니다.

```bash
pgxllm web --host 0.0.0.0 --port 8000
pgxllm web --reload   # 개발용 hot-reload
```

### pgxllm eval

BIRD benchmark로 Text-to-SQL 품질을 평가합니다.

```bash
pgxllm eval \
  --file bird_dev.json \
  --alias mydb \
  --output results/eval.json \
  --limit 100
```

### Makefile 단축 명령

```bash
make install       # Python 가상환경 + 의존성 설치
make build         # 프론트엔드 빌드 (npm install + npm run build)
make serve         # 빌드 후 서버 시작 (프로덕션)
make serve PORT=80 # 포트 지정
make dev-backend   # FastAPI 개발 서버 (hot-reload, 포트 8000)
make dev-frontend  # Vite 개발 서버 (HMR, 포트 5173)
make test          # pytest 전체 테스트
make test-cov      # 커버리지 포함 테스트
make lint          # Ruff 코드 검사
make lint-fix      # Ruff 자동 수정
make format        # Ruff 포맷팅
```

---

## 8. REST API 레퍼런스

베이스 URL: `http://서버:8000/api`

### DB 관리

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/db/list` | 등록된 DB 목록 |
| `POST` | `/db/register` | DB 등록 |
| `POST` | `/db/refresh/{alias}` | 스키마 재수집 |
| `DELETE` | `/db/{alias}` | DB 삭제 |

### 스키마

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/schema/{alias}` | 테이블·컬럼 목록 (`?search=키워드`) |
| `GET` | `/schema/{alias}/indexes` | 인덱스 목록 |

### 쿼리

| Method | Path | 설명 |
|---|---|---|
| `POST` | `/query/run` | SQL 실행 또는 LLM 파이프라인 |
| `GET` | `/query/history` | 실행 이력 (`?alias=&limit=&mode=`) |
| `DELETE` | `/query/cache` | 특정 캐시 삭제 |
| `DELETE` | `/query/cache/all` | 전체 캐시 삭제 |
| `DELETE` | `/query/history/{id}` | 이력 삭제 |

**POST /query/run 예시**

```json
{
  "alias": "mydb",
  "sql": "2023년 월별 주문 건수는?",
  "mode": "pipeline",
  "limit": 500,
  "debug": false
}
```

`mode`: `direct` (SQL 직접 실행) | `pipeline` (LLM Text-to-SQL)

### Graph

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/graph/{alias}` | 관계 엣지 목록 |
| `GET` | `/graph/{alias}/paths` | JOIN 경로 목록 |
| `POST` | `/graph/{alias}/collect-pg-stat` | pg_stat 수집 |
| `POST` | `/graph/{alias}/refresh-paths` | 경로 재계산 |
| `POST` | `/graph/{alias}/approve/{edge_id}` | 관계 승인 |
| `POST` | `/graph/{alias}/approve-all` | 전체 일괄 승인 |
| `DELETE` | `/graph/{alias}/edge/{edge_id}` | 관계 삭제 |
| `PATCH` | `/graph/{alias}/edge/{edge_id}` | 관계 수정 |

### pg_stat 분석

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/pgstat/{alias}/queries` | 수집된 쿼리 목록 |
| `POST` | `/pgstat/{alias}/reset` | pg_stat_statements 초기화 |
| `POST` | `/pgstat/{alias}/query/infer` | LLM 관계 추론 |
| `POST` | `/pgstat/{alias}/query/tune` | LLM 튜닝 제안 |
| `POST` | `/pgstat/{alias}/query/describe` | LLM 쿼리 설명 |
| `POST` | `/pgstat/{alias}/query/plan` | EXPLAIN 실행계획 |
| `POST` | `/pgstat/{alias}/query/save-cache` | 캐시 저장 |
| `POST` | `/pgstat/{alias}/query/save-edge` | 관계 저장 |

**POST /pgstat/{alias}/query/plan 예시**

```json
{
  "sql": "SELECT * FROM orders WHERE status = $1",
  "analyze": true
}
```

응답:
```json
{
  "plan": [...],
  "execution_time": 12.3,
  "analyzed": true
}
```

### Rules

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/rules/{alias}` | Dialect Rules 목록 |
| `POST` | `/rules/{alias}` | 규칙 추가 |
| `PATCH` | `/rules/{alias}/{rule_id}` | 규칙 활성화/비활성화 |
| `DELETE` | `/rules/{alias}/{rule_id}` | 규칙 삭제 |

### LLM 설정

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/llm/providers` | 지원 Provider 목록 및 기본값 |
| `GET` | `/llm/config` | 현재 LLM 설정 (API Key 마스킹) |
| `POST` | `/llm/config` | LLM 설정 저장 |
| `POST` | `/llm/test` | 연결 테스트 |

**POST /llm/config 예시**

```json
{
  "provider": "watsonx",
  "base_url": "https://us-south.ml.cloud.ibm.com",
  "model": "ibm/granite-34b-code-instruct",
  "api_key": "your-ibm-api-key",
  "project_id": "your-project-uuid",
  "timeout": 600,
  "max_tokens": 2048,
  "temperature": 0.0
}
```

### 시스템 상태

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/status` | Internal DB 연결 상태 |

---

## 9. 개발 가이드

### 새 LLM Provider 추가

1. `src/pgxllm/core/llm/` 에 새 파일 생성:

```python
# src/pgxllm/core/llm/myprovider.py
from .base import LLMProvider, LLMResponse

class MyProvider(LLMProvider):
    def __init__(self, api_key: str, model: str, timeout: int = 600):
        self._api_key = api_key
        self._model   = model
        self._timeout = timeout

    @property
    def model_name(self) -> str:
        return self._model

    def complete(
        self, system: str, user: str,
        *, temperature: float = 0.0, max_tokens: int = 2048
    ) -> LLMResponse:
        # HTTP API 호출 구현
        ...
        return LLMResponse(
            text=response_text,
            model=self._model,
            input_tokens=0,
            output_tokens=0,
        )
```

2. `factory.py`에 케이스 추가:

```python
elif provider == "myprovider":
    from .myprovider import MyProvider
    api_key = cfg.api_key or os.environ.get("MY_API_KEY", "")
    return MyProvider(api_key=api_key, model=cfg.model, timeout=cfg.timeout)
```

3. `app.py`의 `/api/llm/providers` 목록에 추가.

### 프로젝트 의존성 추가

`pyproject.toml`의 `dependencies` 목록에 추가 후:

```bash
pip install -e ".[dev]"
```

### 테스트

```bash
make test                                   # 전체 테스트
make test-cov                              # 커버리지 포함
PYTHONPATH=src pytest tests/core/ -v       # 특정 모듈
PYTHONPATH=src pytest tests/ -k "pipeline" # 키워드 필터
```

### 코드 품질

```bash
make lint        # Ruff 검사
make lint-fix    # 자동 수정
make format      # 포맷팅
```

### Internal DB 스키마 변경

새 테이블은 앱 시작 시 `CREATE TABLE IF NOT EXISTS`로 자동 생성됩니다.  
`web/app.py`의 `_ensure_*_table()` 패턴을 참고하세요.

### 개발 환경 구성

```bash
# 터미널 1: FastAPI (hot-reload)
make dev-backend    # http://localhost:8000

# 터미널 2: Vite (HMR)
make dev-frontend   # http://localhost:5173
```

Vite 개발 서버는 `/api/*` 요청을 서버 사이드에서 `localhost:8000`으로 프록시합니다.

---

## 라이선스

사내 사용 목적으로 개발된 소프트웨어입니다.
