"""
游戏内分布式事务与两阶段提交系统 - 核心模块

修复内容：
- begin_transaction(): 事务ID + 隔离(锁/未提交不可见) + WAL日志(崩溃可恢复)
- prepare_phase(): 资源检查 + 持久化(写WAL) + 失败即中止
- commit_phase(): 仅在全体prepare成功后执行，原子提交 + 通知参与者
- rollback_phase(): 补偿已执行操作 + 幂等(重复回滚安全)
- saga_pattern(): 编排(顺序执行) + 状态机 + 失败补偿(反向补偿)
"""
import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional


class TxnStatus(str, Enum):
    ACTIVE = "ACTIVE"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    ROLLED_BACK = "ROLLED_BACK"


class SagaStatus(str, Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"


@dataclass
class Participant:
    """跨服事务参与者（如 A服道具服 / B服支付服）。"""
    name: str
    resources: Dict[str, int] = field(default_factory=dict)
    committed_log: List[str] = field(default_factory=list)
    notifications: List[str] = field(default_factory=list)
    fail_commit: bool = False  # 测试注入：commit 时模拟故障

    def notify(self, message: str):
        self.notifications.append(message)


@dataclass
class Operation:
    participant: str
    resource: str
    delta: int
    compensate: Optional[Callable[[], None]] = None


@dataclass
class SagaStep:
    name: str
    action: Callable[[], None]
    compensate: Optional[Callable[[], None]] = None


class DistributedTransaction:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self._state = {}
        self._history = []
        self._lock = threading.RLock()
        self.participants: Dict[str, Participant] = {}
        self._transactions: Dict[str, Dict] = {}
        self._resource_locks: Dict[str, str] = {}  # (participant, resource) -> txn_id
        self._wal_path = self.config.get("wal_path")
        self._wal_events: List[Dict] = []
        if self._wal_path and os.path.exists(self._wal_path):
            self._recover_from_wal()

    # ------------------------------------------------------------------ #
    # 原有占位接口（保持兼容）
    # ------------------------------------------------------------------ #
    def update(self, dt: float):
        pass

    def reset(self):
        with self._lock:
            self._state = {}
            self._history = []
            self.participants = {}
            self._transactions = {}
            self._resource_locks = {}
            self._wal_events = []
            if self._wal_path and os.path.exists(self._wal_path):
                os.remove(self._wal_path)

    # ------------------------------------------------------------------ #
    # WAL 日志（崩溃恢复）
    # ------------------------------------------------------------------ #
    def _append_wal(self, event: Dict):
        with self._lock:
            self._wal_events.append(event)
            if self._wal_path:
                with open(self._wal_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _recover_from_wal(self):
        with open(self._wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                self._wal_events.append(event)
                self._apply_wal_event(event)

    def _apply_wal_event(self, event: Dict):
        etype = event.get("type")
        txn_id = event.get("txn_id")
        if etype == "begin":
            txn = self._transactions.setdefault(txn_id, self._new_txn(txn_id))
            txn["status"] = TxnStatus.ACTIVE
        elif etype == "stage":
            txn = self._transactions.setdefault(txn_id, self._new_txn(txn_id))
            op = event["operation"]
            txn["operations"].append(
                Operation(op["participant"], op["resource"], op["delta"])
            )
            self._resource_locks[(op["participant"], op["resource"])] = txn_id
        elif etype == "prepared":
            txn = self._transactions.setdefault(txn_id, self._new_txn(txn_id))
            txn["status"] = TxnStatus.PREPARED
        elif etype == "commit":
            txn = self._transactions.setdefault(txn_id, self._new_txn(txn_id))
            txn["status"] = TxnStatus.COMMITTED
            for op in txn["operations"]:
                self._apply_operation(op)
                self._resource_locks.pop((op.participant, op.resource), None)
        elif etype in ("abort", "rollback"):
            txn = self._transactions.setdefault(txn_id, self._new_txn(txn_id))
            txn["status"] = (
                TxnStatus.ABORTED if etype == "abort" else TxnStatus.ROLLED_BACK
            )
            for op in txn["operations"]:
                self._resource_locks.pop((op.participant, op.resource), None)

    @staticmethod
    def _new_txn(txn_id: str) -> Dict:
        return {"id": txn_id, "status": TxnStatus.ACTIVE, "operations": []}

    # ------------------------------------------------------------------ #
    # 参与者管理
    # ------------------------------------------------------------------ #
    def add_participant(self, participant: Participant):
        self.participants[participant.name] = participant

    # ------------------------------------------------------------------ #
    # 事务开始：事务ID + 隔离 + 日志
    # ------------------------------------------------------------------ #
    def begin_transaction(self, txn_id: Optional[str] = None) -> str:
        with self._lock:
            txn_id = txn_id or "txn-" + uuid.uuid4().hex
            if txn_id in self._transactions:
                raise ValueError("事务ID已存在: %s" % txn_id)
            self._transactions[txn_id] = self._new_txn(txn_id)
            self._append_wal({"type": "begin", "txn_id": txn_id})
            self._history.append(("begin", txn_id))
            return txn_id

    # ------------------------------------------------------------------ #
    # 暂存操作（带隔离：资源加锁，未提交对其他事务不可见）
    # ------------------------------------------------------------------ #
    def stage_operation(
        self,
        txn_id: str,
        participant: str,
        resource: str,
        delta: int,
        compensate: Optional[Callable[[], None]] = None,
    ):
        with self._lock:
            txn = self._require_txn(txn_id)
            if txn["status"] != TxnStatus.ACTIVE:
                raise RuntimeError("事务不在活动状态，无法暂存操作: %s" % txn_id)
            if participant not in self.participants:
                raise KeyError("未知参与者: %s" % participant)
            key = (participant, resource)
            owner = self._resource_locks.get(key)
            if owner is not None and owner != txn_id:
                raise RuntimeError(
                    "资源 %s/%s 已被事务 %s 锁定" % (participant, resource, owner)
                )
            self._resource_locks[key] = txn_id
            txn["operations"].append(
                Operation(participant, resource, delta, compensate)
            )
            self._append_wal(
                {
                    "type": "stage",
                    "txn_id": txn_id,
                    "operation": {
                        "participant": participant,
                        "resource": resource,
                        "delta": delta,
                    },
                }
            )

    # ------------------------------------------------------------------ #
    # 隔离读取：默认看不到其他事务未提交的暂存变更
    # ------------------------------------------------------------------ #
    def get_balance(self, participant: str, resource: str,
                    txn_id: Optional[str] = None) -> int:
        with self._lock:
            base = self.participants[participant].resources.get(resource, 0)
            if txn_id is None:
                return base
            txn = self._require_txn(txn_id)
            staged = sum(
                op.delta
                for op in txn["operations"]
                if op.participant == participant and op.resource == resource
            )
            return base + staged

    # ------------------------------------------------------------------ #
    # prepare 阶段：资源检查 + 持久化
    # ------------------------------------------------------------------ #
    def prepare_phase(self, txn_id: str) -> bool:
        with self._lock:
            txn = self._require_txn(txn_id)
            if txn["status"] != TxnStatus.ACTIVE:
                raise RuntimeError("事务不在活动状态，无法prepare: %s" % txn_id)
            # 资源检查：任一参与者资源不足则 prepare 失败并中止事务
            for op in txn["operations"]:
                p = self.participants[op.participant]
                if p.resources.get(op.resource, 0) + op.delta < 0:
                    self._abort(txn_id)
                    return False
            txn["status"] = TxnStatus.PREPARED
            # 持久化：崩溃后可恢复 prepare 状态
            self._append_wal({"type": "prepared", "txn_id": txn_id})
            self._history.append(("prepared", txn_id))
            return True

    # ------------------------------------------------------------------ #
    # commit 阶段：仅在全体 prepare 成功后执行，原子提交 + 通知
    # ------------------------------------------------------------------ #
    def commit_phase(self, txn_id: str) -> bool:
        with self._lock:
            txn = self._require_txn(txn_id)
            if txn["status"] != TxnStatus.PREPARED:
                # prepare 未成功，禁止 commit（修复"prepare失败也commit"）
                raise RuntimeError(
                    "事务 %s 未成功prepare，禁止commit" % txn_id
                )
            # 原子性：任一参与者失败则全体不提交
            for op in txn["operations"]:
                if self.participants[op.participant].fail_commit:
                    self._append_wal({"type": "abort", "txn_id": txn_id})
                    txn["status"] = TxnStatus.ABORTED
                    for op2 in txn["operations"]:
                        self._resource_locks.pop(
                            (op2.participant, op2.resource), None
                        )
                    self._history.append(("aborted", txn_id))
                    return False
            for op in txn["operations"]:
                self._apply_operation(op)
                self._resource_locks.pop((op.participant, op.resource), None)
            txn["status"] = TxnStatus.COMMITTED
            self._append_wal({"type": "commit", "txn_id": txn_id})
            self._history.append(("committed", txn_id))
            # 通知所有参与者
            for name in {op.participant for op in txn["operations"]}:
                self.participants[name].notify("committed:%s" % txn_id)
            return True

    def _apply_operation(self, op: Operation):
        p = self.participants[op.participant]
        p.resources[op.resource] = p.resources.get(op.resource, 0) + op.delta
        p.committed_log.append(
            "%s:%s:%+d" % (op.participant, op.resource, op.delta)
        )

    # ------------------------------------------------------------------ #
    # 回滚阶段：补偿已执行操作 + 幂等
    # ------------------------------------------------------------------ #
    def rollback_phase(self, txn_id: str) -> bool:
        with self._lock:
            txn = self._require_txn(txn_id)
            # 幂等：已回滚/已中止的事务重复回滚直接成功返回
            if txn["status"] in (TxnStatus.ROLLED_BACK, TxnStatus.ABORTED):
                return True
            if txn["status"] == TxnStatus.COMMITTED:
                # 补偿：撤销已提交的操作（反向应用 + 自定义补偿回调）
                for op in reversed(txn["operations"]):
                    p = self.participants[op.participant]
                    p.resources[op.resource] = (
                        p.resources.get(op.resource, 0) - op.delta
                    )
                    if op.compensate is not None:
                        op.compensate()
            else:
                # ACTIVE/PREPARED：尚未提交，释放锁并中止
                for op in txn["operations"]:
                    self._resource_locks.pop(
                        (op.participant, op.resource), None
                    )
            txn["status"] = TxnStatus.ROLLED_BACK
            self._append_wal({"type": "rollback", "txn_id": txn_id})
            self._history.append(("rolled_back", txn_id))
            for name in {op.participant for op in txn["operations"]}:
                self.participants[name].notify("rolled_back:%s" % txn_id)
            return True

    def _abort(self, txn_id: str):
        txn = self._transactions[txn_id]
        txn["status"] = TxnStatus.ABORTED
        for op in txn["operations"]:
            self._resource_locks.pop((op.participant, op.resource), None)
        self._append_wal({"type": "abort", "txn_id": txn_id})
        self._history.append(("aborted", txn_id))

    # ------------------------------------------------------------------ #
    # 查询接口
    # ------------------------------------------------------------------ #
    def get_transaction_status(self, txn_id: str) -> TxnStatus:
        return self._require_txn(txn_id)["status"]

    def get_wal_events(self) -> List[Dict]:
        return list(self._wal_events)

    def _require_txn(self, txn_id: str) -> Dict:
        if txn_id not in self._transactions:
            raise KeyError("未知事务: %s" % txn_id)
        return self._transactions[txn_id]

    # ------------------------------------------------------------------ #
    # Saga 模式：编排(顺序执行) + 状态机 + 失败补偿
    # ------------------------------------------------------------------ #
    def saga_pattern(self, steps: List) -> "SagaResult":
        """按编排顺序执行长事务步骤；任一步骤失败则反向补偿已完成步骤。

        steps: SagaStep 或 dict(name=, action=, compensate=)。
        返回 SagaResult（状态机记录每一步状态与当前进度）。
        """
        normalized: List[SagaStep] = []
        for i, step in enumerate(steps):
            if isinstance(step, SagaStep):
                normalized.append(step)
            elif isinstance(step, dict):
                normalized.append(
                    SagaStep(
                        name=step.get("name", "step-%d" % i),
                        action=step["action"],
                        compensate=step.get("compensate"),
                    )
                )
            else:
                raise TypeError("不支持的Saga步骤类型: %r" % type(step))

        result = SagaResult([s.name for s in normalized])
        completed: List[SagaStep] = []
        for step in normalized:  # 编排：严格按声明顺序执行
            result.step_states[step.name] = "RUNNING"
            result.current_step = step.name
            try:
                step.action()
            except Exception as exc:  # noqa: BLE001
                result.step_states[step.name] = "FAILED"
                result.error = exc
                # 状态机：进入补偿阶段，反向补偿已完成步骤
                result.status = SagaStatus.COMPENSATING
                for done in reversed(completed):
                    if done.compensate is not None:
                        try:
                            done.compensate()
                            result.step_states[done.name] = "COMPENSATED"
                        except Exception:  # noqa: BLE001
                            result.step_states[done.name] = "COMPENSATE_FAILED"
                result.status = (
                    SagaStatus.COMPENSATED
                    if all(
                        result.step_states[d.name] == "COMPENSATED"
                        for d in completed
                        if d.compensate is not None
                    )
                    else SagaStatus.FAILED
                )
                result.current_step = None
                self._history.append(("saga", result.status.value))
                return result
            completed.append(step)
            result.step_states[step.name] = "DONE"
        result.status = SagaStatus.COMPLETED
        result.current_step = None
        self._history.append(("saga", result.status.value))
        return result


class SagaResult:
    """Saga 执行结果 / 状态机。"""

    def __init__(self, step_names: List[str]):
        self.status = SagaStatus.RUNNING
        self.step_states: Dict[str, str] = {
            name: "PENDING" for name in step_names
        }
        self.current_step: Optional[str] = None
        self.error: Optional[Exception] = None

    def is_completed(self) -> bool:
        return self.status == SagaStatus.COMPLETED

    def is_compensated(self) -> bool:
        return self.status == SagaStatus.COMPENSATED

    def __repr__(self):
        return "SagaResult(status=%s, steps=%r)" % (
            self.status.value,
            self.step_states,
        )
