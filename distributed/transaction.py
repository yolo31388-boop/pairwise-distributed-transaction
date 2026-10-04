"""
游戏内分布式事务与两阶段提交系统 - 核心模块

修复内容：
- begin_transaction(): 事务ID + 隔离级别 + 预写日志(WAL)
- prepare_phase():     资源检查 + prepare记录持久化
- commit_phase():      原子提交 + 参与者通知
- rollback_phase():    补偿执行 + 幂等回滚
- saga_pattern():      步骤编排 + 状态机 + 反向补偿
"""
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class IsolationLevel(Enum):
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"


class TransactionState(Enum):
    ACTIVE = "active"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"


class SagaStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"


@dataclass
class LogRecord:
    txn_id: str
    action: str
    timestamp: float
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SagaStep:
    name: str
    action: Callable[[], Any]
    compensation: Optional[Callable[[], Any]] = None


@dataclass
class SagaResult:
    saga_id: str
    status: SagaStatus
    current_step: int
    executed_steps: List[str] = field(default_factory=list)
    compensated_steps: List[str] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.status == SagaStatus.COMPLETED


class Participant:
    """事务参与者（如某个游戏服的交易服务）。

    各钩子均可选：
    - on_prepare(txn) -> bool: 资源检查/预留，返回 False 表示资源不足
    - on_commit(txn): 提交本地事务
    - on_rollback(txn): 补偿/撤销已执行操作
    - on_notify(txn): 收到协调者的 commit 通知
    """

    def __init__(self, name: str,
                 on_prepare: Optional[Callable] = None,
                 on_commit: Optional[Callable] = None,
                 on_rollback: Optional[Callable] = None,
                 on_notify: Optional[Callable] = None):
        self.name = name
        self._on_prepare = on_prepare
        self._on_commit = on_commit
        self._on_rollback = on_rollback
        self._on_notify = on_notify
        self.prepared: List[str] = []
        self.committed: List[str] = []
        self.rolled_back: List[str] = []
        self.notified: List[str] = []

    def prepare(self, txn: "Transaction") -> bool:
        if self._on_prepare is not None and not self._on_prepare(txn):
            return False
        self.prepared.append(txn.txn_id)
        return True

    def commit(self, txn: "Transaction") -> None:
        if self._on_commit is not None:
            self._on_commit(txn)
        self.committed.append(txn.txn_id)

    def rollback(self, txn: "Transaction") -> None:
        if self._on_rollback is not None:
            self._on_rollback(txn)
        self.rolled_back.append(txn.txn_id)

    def notify(self, txn: "Transaction") -> None:
        if self._on_notify is not None:
            self._on_notify(txn)
        self.notified.append(txn.txn_id)


@dataclass
class Transaction:
    txn_id: str
    isolation: IsolationLevel
    state: TransactionState = TransactionState.ACTIVE
    participants: List[Participant] = field(default_factory=list)
    writes: Dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)


class DistributedTransaction:
    """分布式事务协调者：两阶段提交 + Saga 长事务。"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self._transactions: Dict[str, Transaction] = {}
        self._log: List[LogRecord] = []
        self._store: Dict[str, Any] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # 日志（WAL）：先写日志再改状态，崩溃后可恢复
    # ------------------------------------------------------------------ #
    def _append_log(self, txn_id: str, action: str, **data) -> LogRecord:
        record = LogRecord(txn_id=txn_id, action=action,
                           timestamp=time.time(), data=data)
        self._log.append(record)
        return record

    @property
    def transaction_log(self) -> List[LogRecord]:
        return list(self._log)

    # ------------------------------------------------------------------ #
    # 事务开始：唯一事务ID + 隔离级别 + 日志
    # ------------------------------------------------------------------ #
    def begin_transaction(
        self,
        isolation: IsolationLevel = IsolationLevel.READ_COMMITTED,
    ) -> str:
        with self._lock:
            txn_id = "txn-" + uuid.uuid4().hex
            txn = Transaction(txn_id=txn_id, isolation=isolation)
            self._transactions[txn_id] = txn
            self._append_log(txn_id, "BEGIN", isolation=isolation.value)
            return txn_id

    def get_transaction(self, txn_id: str) -> Transaction:
        if txn_id not in self._transactions:
            raise KeyError(f"未知事务: {txn_id}")
        return self._transactions[txn_id]

    def get_state(self, txn_id: str) -> TransactionState:
        return self.get_transaction(txn_id).state

    # ------------------------------------------------------------------ #
    # 隔离的读写：未提交写入对其他事务不可见
    # ------------------------------------------------------------------ #
    def write(self, txn_id: str, key: str, value: Any) -> None:
        txn = self.get_transaction(txn_id)
        if txn.state != TransactionState.ACTIVE:
            raise RuntimeError(f"事务 {txn_id} 不在活动状态，无法写入")
        txn.writes[key] = value

    def read(self, key: str, txn_id: Optional[str] = None) -> Any:
        with self._lock:
            if txn_id is not None:
                txn = self.get_transaction(txn_id)
                if key in txn.writes:  # 读自己的未提交写入
                    return txn.writes[key]
            return self._store.get(key)  # 其他事务只能读到已提交数据

    # ------------------------------------------------------------------ #
    # 参与者注册
    # ------------------------------------------------------------------ #
    def register_participant(self, txn_id: str, participant: Participant) -> None:
        txn = self.get_transaction(txn_id)
        if txn.state != TransactionState.ACTIVE:
            raise RuntimeError(f"事务 {txn_id} 不在活动状态，无法注册参与者")
        txn.participants.append(participant)

    # ------------------------------------------------------------------ #
    # 两阶段提交 - prepare：资源检查 + 持久化
    # ------------------------------------------------------------------ #
    def prepare_phase(self, txn_id: str) -> bool:
        with self._lock:
            txn = self.get_transaction(txn_id)
            if txn.state != TransactionState.ACTIVE:
                return txn.state == TransactionState.PREPARED
            txn.state = TransactionState.PREPARING
            self._append_log(txn_id, "PREPARING")

            for participant in txn.participants:
                try:
                    ok = participant.prepare(txn)
                except Exception as exc:
                    ok = False
                    self._append_log(txn_id, "PREPARE_ERROR",
                                     participant=participant.name, error=str(exc))
                if not ok:
                    self._append_log(txn_id, "PREPARE_FAILED",
                                     participant=participant.name)
                    self.rollback_phase(txn_id)
                    return False

            # 持久化 prepare 记录：崩溃恢复时知道哪些事务已 prepare
            self._append_log(txn_id, "PREPARED",
                             participants=[p.name for p in txn.participants])
            txn.state = TransactionState.PREPARED
            return True

    # ------------------------------------------------------------------ #
    # 两阶段提交 - commit：仅在全员 prepare 成功后提交，原子 + 通知
    # ------------------------------------------------------------------ #
    def commit_phase(self, txn_id: str) -> bool:
        with self._lock:
            txn = self.get_transaction(txn_id)
            if txn.state == TransactionState.COMMITTED:
                return True
            if txn.state != TransactionState.PREPARED:
                # prepare 未成功（或根本没 prepare）不允许 commit
                self._append_log(txn_id, "COMMIT_REJECTED",
                                 state=txn.state.value)
                return False

            txn.state = TransactionState.COMMITTING
            self._append_log(txn_id, "COMMITTING")

            committed: List[Participant] = []
            try:
                for participant in txn.participants:
                    participant.commit(txn)
                    committed.append(participant)
                for key, value in txn.writes.items():
                    self._store[key] = value
                txn.writes.clear()
            except Exception as exc:
                # 原子性：任一参与者提交失败，补偿已提交部分，整体回滚
                self._append_log(txn_id, "COMMIT_FAILED", error=str(exc))
                for participant in reversed(committed):
                    try:
                        participant.rollback(txn)
                    except Exception:
                        pass
                txn.state = TransactionState.ABORTED
                self._append_log(txn_id, "ABORTED", reason="commit_failed")
                return False

            txn.state = TransactionState.COMMITTED
            self._append_log(txn_id, "COMMITTED")

            # 通知所有参与者：commit 已完成
            for participant in txn.participants:
                try:
                    participant.notify(txn)
                except Exception:
                    pass
            return True

    # ------------------------------------------------------------------ #
    # 回滚：补偿已执行操作 + 幂等
    # ------------------------------------------------------------------ #
    def rollback_phase(self, txn_id: str) -> bool:
        with self._lock:
            txn = self.get_transaction(txn_id)
            if txn.state == TransactionState.ABORTED:
                return True  # 幂等：重复回滚直接成功，不重复补偿
            if txn.state == TransactionState.COMMITTED:
                return False  # 已提交的事务不能回滚

            txn.state = TransactionState.ABORTING
            self._append_log(txn_id, "ROLLBACK")

            # 反向顺序补偿：撤销参与者已执行的操作
            for participant in reversed(txn.participants):
                try:
                    participant.rollback(txn)
                except Exception as exc:
                    self._append_log(txn_id, "COMPENSATION_ERROR",
                                     participant=participant.name, error=str(exc))
            txn.writes.clear()
            txn.state = TransactionState.ABORTED
            self._append_log(txn_id, "ABORTED")
            return True

    # ------------------------------------------------------------------ #
    # 崩溃恢复：从日志重建，返回处于 in-doubt（已prepare未commit）的事务
    # ------------------------------------------------------------------ #
    def recover(self) -> List[str]:
        last_action: Dict[str, str] = {}
        for record in self._log:
            last_action[record.txn_id] = record.action
        in_doubt = [tid for tid, action in last_action.items()
                    if action == "PREPARED"]
        for tid in in_doubt:
            txn = self._transactions.get(tid)
            if txn is not None:
                txn.state = TransactionState.PREPARED
        return in_doubt

    # ------------------------------------------------------------------ #
    # Saga 模式：编排 + 状态机 + 失败反向补偿
    # ------------------------------------------------------------------ #
    def saga_pattern(self, steps: List, name: Optional[str] = None) -> SagaResult:
        saga_id = "saga-" + uuid.uuid4().hex
        normalized: List[SagaStep] = []
        for step in steps:
            if isinstance(step, SagaStep):
                normalized.append(step)
            else:  # (name, action, compensation) 三元组
                step_name, action, compensation = step
                normalized.append(SagaStep(step_name, action, compensation))

        result = SagaResult(saga_id=saga_id, status=SagaStatus.RUNNING,
                            current_step=-1)
        self._append_log(saga_id, "SAGA_BEGIN", name=name,
                         steps=[s.name for s in normalized])

        completed: List[SagaStep] = []
        for index, step in enumerate(normalized):
            result.current_step = index  # 状态机：记录当前执行到哪一步
            self._append_log(saga_id, "SAGA_STEP_START", step=step.name)
            try:
                step.action()
            except Exception as exc:
                result.error = f"步骤 {step.name} 失败: {exc}"
                result.status = SagaStatus.COMPENSATING
                self._append_log(saga_id, "SAGA_STEP_FAILED",
                                 step=step.name, error=str(exc))
                # 反向补偿已完成的步骤
                for done in reversed(completed):
                    if done.compensation is None:
                        continue
                    try:
                        done.compensation()
                        result.compensated_steps.append(done.name)
                        self._append_log(saga_id, "SAGA_COMPENSATED",
                                         step=done.name)
                    except Exception as comp_exc:
                        self._append_log(saga_id, "SAGA_COMPENSATION_ERROR",
                                         step=done.name, error=str(comp_exc))
                result.status = SagaStatus.COMPENSATED
                self._append_log(saga_id, "SAGA_ABORTED", saga=name)
                return result
            completed.append(step)
            result.executed_steps.append(step.name)
            self._append_log(saga_id, "SAGA_STEP_DONE", step=step.name)

        result.current_step = len(normalized)
        result.status = SagaStatus.COMPLETED
        self._append_log(saga_id, "SAGA_COMPLETED", saga=name)
        return result
