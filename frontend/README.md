# pgxllm Frontend

React (Vite) Query Test UI

## 개발 모드

```bash
# 터미널 1: FastAPI 백엔드
cd ..
source .venv/bin/activate
python -m pgxllm.web.app

# 터미널 2: React dev server (hot reload)
cd frontend
npm run dev
# → http://localhost:5173
```

## 프로덕션 빌드

```bash
npm run build
# → ../src/pgxllm/web/static/ 에 빌드 결과물 생성
# → python -m pgxllm.web.app 으로 서빙
```

## 페이지 구성

| 경로      | 설명                    |
|-----------|------------------------|
| /query    | SQL 직접 실행 (Ctrl+Enter) |
| /schema   | 테이블/컬럼 탐색           |
| /graph    | graph_edges 관계 조회    |
| /rules    | Dialect Rules 조회      |
| /dbs      | Target DB 등록/관리      |
