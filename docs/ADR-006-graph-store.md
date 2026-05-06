# ADR-006: GraphStore 설계 및 백엔드 선택

**상태:** 확정 + 구현 완료 ✅  
**날짜:** 2025  
**관련 Phase:** Phase 3, 5  
**구현 위치:** `src/pgxllm/graph/`

---

## 배경

테이블 간 관계를 표현하는 방법으로 세 가지 옵션을 검토했다:

| 옵션 | 검토 결과 |
|---|---|
| Neo4j | 외부 의존성 높음, OCP 환경에서 별도 서버 필요 → **현재 부적합** |
| Apache AGE (PG extension) | openCypher 지원 but Incubating 단계, 소스 빌드 필요 → **추후 가능** |
| PostgreSQL (relational + pgvector) | 안정적, 외부 의존성 없음 → **현재 채택** |

---

## 결정

### 핵심 원칙: 교체 가능한 추상화

```
Core Pipeline → GraphStore (interface) 만 알고 있음
              → 구현체는 설정으로 교체 가능
```

### GraphStore 인터페이스

```python
class GraphStore(ABC):
    def add_node(self, node: TableNode) -> None
    def add_edge(self, edge: TableEdge) -> None
    def find_paths(self, from_table: str, to_table: str,
                   max_depth: int = 4) -> list[GraphPath]
    def find_neighbors(self, table: str, depth: int = 1) -> list[TableNode]
    def find_related_tables(self, question_embedding: list[float],
                            top_k: int = 5) -> list[TableNode]
    def refresh_paths(self, db_alias: str) -> None
    def get_join_hint(self, tables: list[str]) -> str
```

### 구현체 구조

```
src/pgxllm/graph/
  base.py         # GraphStore ABC + 데이터 클래스
  postgresql.py   # 기본 구현 (사전 계산 방식)
  age.py          # AGE 구현 (추후)
  neo4j.py        # Neo4j 구현 (추후)
  factory.py      # GraphStoreFactory
```

### 설정으로 교체

```yaml
graph:
  backend: postgresql   # postgresql | age | neo4j
```

---

## PostgreSQL 구현 핵심: 사전 계산 전략

### 왜 WITH RECURSIVE를 쓰지 않는가?

WITH RECURSIVE는 테이블 수가 많고 관계가 복잡할수록 exponential 탐색으로 느려진다.

테이블 관계는 자주 바뀌지 않으므로 **db refresh 시 BFS로 미리 계산**해두면 된다.

### graph_paths 사전 계산

```
[db refresh 시 - 오프라인]
  graph_edges 변경 감지
      ↓
  BFS 전체 경로 사전 계산 (오래 걸려도 무방)
      ↓
  graph_paths 테이블에 저장
  + 경로별 embedding → pgvector

[런타임 - 온라인]
  pgvector → 관련 테이블 후보 (ms)
      ↓
  graph_paths SELECT → JOIN 경로 (ms, WITH RECURSIVE 없음)
      ↓
  join_hint 텍스트 → S3 LLM prompt 주입
```

### LLM에 주입되는 JOIN hint 형태

```
"관련 테이블 및 JOIN 경로:
  orders (주문)
    └─ customers (고객)  via orders.customer_id = customers.id
    └─ regions (지역)    via orders.region_code = regions.code  [비즈니스 관계]

주의: orders.region_code → regions.code 는 FK가 아닌 비즈니스 관계입니다."
```

### pgvector 역할 분담

```
pgvector  →  "어떤 테이블이 이 질문과 관련 있는가?"  (entry point 탐색)
graph     →  "그 테이블들이 어떻게 연결되는가?"       (JOIN 경로 탐색)
```

---

## Cross-DB 관계 지원

```python
# from_db_alias ≠ to_db_alias 허용
edge = TableEdge(
    from_db_alias="mydb",
    from_schema="public",
    from_table="orders",
    from_column="customer_id",
    to_db_alias="warehouse",  # 다른 DB
    to_schema="dw",
    to_table="customers",
    to_column="id",
    is_cross_db=True,
)
```

---

## AGE/Neo4j 전환 시 주의 사항

인터페이스가 동일하므로 Core Pipeline 코드는 변경 없이 전환 가능.
단, AGE는:
- PostgreSQL 소스 빌드 필요
- `apache-age` Python 드라이버 불안정
- OCP 환경에서 커스텀 이미지 필요

Neo4j는:
- 별도 서버 운영 필요
- `pgxllm db` 명령어로 등록된 target DB와 완전히 별개 관리
