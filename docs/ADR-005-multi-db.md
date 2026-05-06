# ADR-005: Multi-DB / Multi-Schema 설계

**상태:** 확정 + 구현 완료  
**날짜:** 2025  
**관련 Phase:** Phase 3  
**구현 위치:** `src/pgxllm/config.py`, `src/pgxllm/db/connections.py`  
**테스트:** `tests/test_config.py` — 31/31 통과 ✅

---

## 결정 내용

### 1. DB 분리 구조

```
internal DB (1개)          ← pgxllm 메타데이터 전용, 완전 독립 인스턴스 가능
target DB (N개)            ← 각각 다른 host:port, 독립 connection pool
```

Internal DB와 Target DB는 같은 PG 인스턴스일 수도 있고, 완전히 다른 서버일 수도 있다.

### 2. Target DB: 완전 독립 인스턴스 지원 (옵션 B 선택)

각 target DB는 독립된 host:port:dbname을 가진다.

```yaml
target_dbs:
  - alias: mydb
    host: app-host-a        # 완전히 다른 서버
    port: 5432
    user: app_user
    dbname: mydb

  - alias: warehouse
    host: wh-host-b         # 또 다른 서버
    port: 5433
    user: wh_user
    dbname: warehouse
```

### 3. Schema 범위: include + exclude 둘 다 지원

```yaml
# include 모드: 명시된 스키마만 분석
schema_mode: include
schemas: [public, sales, hr]

# exclude 모드: 전체 스캔에서 제외
schema_mode: exclude
schemas: [pg_catalog, information_schema, pg_toast]
```

### 4. Cross-DB Relations: 허용

서로 다른 target DB 간의 JOIN 관계도 등록 가능하다.

```
mydb.public.orders → warehouse.dw.customers
```

`graph_edges`, `graph_paths` 테이블에 `is_cross_db` 플래그로 구분.

---

## TableAddress 표현 형식

```python
# alias.schema.table 형식
addr = TableAddress.parse("mydb.public.orders")
addr = TableAddress.parse("warehouse.dw.customers")

# 접근
addr.db_alias    # "mydb"
addr.schema      # "public"
addr.table       # "orders"
addr.qualified   # "mydb.public.orders"
```

---

## 설정 방법

### 1. YAML (정적 등록)

```yaml
# configs/default.yaml
internal_db:
  host: ${PGXLLM_HOST:-localhost}
  port: ${PGXLLM_PORT:-5432}
  user: ${PGXLLM_USER:-postgres}
  password: ${PGXLLM_PASSWORD:-}
  dbname: ${PGXLLM_DBNAME:-pgxllm}
  schema: pgxllm

target_dbs:
  - alias: mydb
    host: ${MYDB_HOST:-localhost}
    port: ${MYDB_PORT:-5432}
    user: ${MYDB_USER:-postgres}
    password: ${MYDB_PASSWORD:-}
    dbname: mydb
    schema_mode: include
    schemas: [public, sales]
```

### 2. CLI (동적 등록, db_registry에 저장)

```bash
pgxllm db register \
  --alias mydb \
  --host app-host \
  --port 5432 \
  --user myuser \
  --dbname mydb \
  --schema-mode include \
  --schemas public,sales,hr
```

### 3. 환경 변수 오버라이드

YAML 내 `${VAR:-default}` 패턴으로 env var 주입 가능:

```bash
export PGXLLM_HOST=prod-host
export MYDB_PASSWORD=secret
```

---

## Connection Pool

각 DB별 독립 ThreadedConnectionPool (lazy init):

```python
registry = ConnectionRegistry(config)

# Internal DB
with registry.internal.connection() as conn:
    conn.execute("SELECT * FROM pgxllm.db_registry")

# Target DB
with registry.target("mydb").connection() as conn:
    conn.execute("SELECT * FROM public.orders LIMIT 5")

# 동적 등록
registry.register_target(TargetDBConfig(alias="analytics", ...))
```

---

## Blacklist 병합 규칙

샘플 데이터 추출 시 global blacklist와 per-DB blacklist를 병합한다:

```python
merged = config.merge_blacklist(target_db)
# global.tables + target.blacklist_tables (중복 제거)
# global.columns + target.blacklist_columns
# global.patterns + target.blacklist_patterns
```

기본 global 패턴: `*_hash`, `*_token`, `*_password`, `*_secret`, `*_key`
