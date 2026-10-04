"""
游戏内分布式事务与两阶段提交系统 - 验收测试
运行方式：python -m pytest tests/test_distributed_transaction.py -q
共 16 个测试用例
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distributed.transaction import (
    DistributedTransaction,
    Participant,
    SagaStep,
    TxnStatus,
    SagaStatus,
)


def make_trading_system(wal_path=None):
    """跨服交易场景：A服道具服 + B服支付服。"""
    config = {"wal_path": wal_path} if wal_path else {}
    system = DistributedTransaction(config)
    system.add_participant(Participant("server_a", resources={"item_sword": 1}))
    system.add_participant(Participant("server_b", resources={"gold": 1000}))
    return system


class TestDistributedTransaction:
    def setup_method(self):
        self.system = make_trading_system()

    # ---------------- 事务开始：ID / 隔离 / 日志 ---------------- #

    def test_case_01(self):
        """事务开始生成唯一事务ID"""
        txn1 = self.system.begin_transaction()
        txn2 = self.system.begin_transaction()
        assert txn1 and txn2
        assert txn1 != txn2
        assert self.system.get_transaction_status(txn1) == TxnStatus.ACTIVE
        assert self.system.get_transaction_status(txn2) == TxnStatus.ACTIVE

    def test_case_02(self):
        """隔离：未提交的暂存变更对其他事务不可见"""
        txn1 = self.system.begin_transaction()
        self.system.stage_operation(txn1, "server_b", "gold", -100)
        # 事务外（其他事务视角）看不到未提交的扣款
        assert self.system.get_balance("server_b", "gold") == 1000
        # 本事务内可见自己的暂存变更
        assert self.system.get_balance("server_b", "gold", txn_id=txn1) == 900

    def test_case_03(self):
        """事务开始写WAL日志，崩溃后可恢复"""
        txn_id = self.system.begin_transaction()
        events = self.system.get_wal_events()
        assert any(
            e["type"] == "begin" and e["txn_id"] == txn_id for e in events
        )

    # ---------------- prepare 阶段：检查 / 持久化 ---------------- #

    def test_case_04(self):
        """prepare 检查资源：资源不足时 prepare 失败"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn1 := txn_id, "server_b", "gold", -2000)
        assert self.system.prepare_phase(txn1) is False
        assert self.system.get_transaction_status(txn1) == TxnStatus.ABORTED

    def test_case_05(self):
        """prepare 成功：资源充足且状态推进为 PREPARED"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        assert self.system.prepare_phase(txn_id) is True
        assert self.system.get_transaction_status(txn_id) == TxnStatus.PREPARED

    def test_case_06(self):
        """prepare 结果持久化到WAL"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        self.system.prepare_phase(txn_id)
        events = self.system.get_wal_events()
        assert any(
            e["type"] == "prepared" and e["txn_id"] == txn_id for e in events
        )

    # ---------------- commit 阶段：原子性 / 通知 ---------------- #

    def test_case_07(self):
        """commit 原子性：跨服操作全部生效"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_a", "item_sword", -1)
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        assert self.system.prepare_phase(txn_id) is True
        assert self.system.commit_phase(txn_id) is True
        assert self.system.get_balance("server_a", "item_sword") == 0
        assert self.system.get_balance("server_b", "gold") == 900
        assert self.system.get_transaction_status(txn_id) == TxnStatus.COMMITTED

    def test_case_08(self):
        """prepare 失败的事务禁止 commit"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_b", "gold", -2000)
        assert self.system.prepare_phase(txn_id) is False
        with pytest.raises(RuntimeError):
            self.system.commit_phase(txn_id)
        # 未提交：余额不变
        assert self.system.get_balance("server_b", "gold") == 1000

    def test_case_09(self):
        """commit 原子性：部分参与者失败则全体不提交"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_a", "item_sword", -1)
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        assert self.system.prepare_phase(txn_id) is True
        self.system.participants["server_b"].fail_commit = True
        assert self.system.commit_phase(txn_id) is False
        # 全体未提交，不存在部分提交
        assert self.system.get_balance("server_a", "item_sword") == 1
        assert self.system.get_balance("server_b", "gold") == 1000
        assert self.system.get_transaction_status(txn_id) == TxnStatus.ABORTED

    def test_case_10(self):
        """commit 后通知所有参与者"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_a", "item_sword", -1)
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        self.system.prepare_phase(txn_id)
        self.system.commit_phase(txn_id)
        expected = "committed:%s" % txn_id
        assert expected in self.system.participants["server_a"].notifications
        assert expected in self.system.participants["server_b"].notifications

    # ---------------- 回滚阶段：补偿 / 幂等 ---------------- #

    def test_case_11(self):
        """回滚补偿：已提交的操作被撤销"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_a", "item_sword", -1)
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        self.system.prepare_phase(txn_id)
        self.system.commit_phase(txn_id)
        assert self.system.rollback_phase(txn_id) is True
        assert self.system.get_balance("server_a", "item_sword") == 1
        assert self.system.get_balance("server_b", "gold") == 1000
        assert self.system.get_transaction_status(txn_id) == TxnStatus.ROLLED_BACK

    def test_case_12(self):
        """回滚幂等：重复回滚不报错、不重复补偿"""
        txn_id = self.system.begin_transaction()
        self.system.stage_operation(txn_id, "server_b", "gold", -100)
        self.system.prepare_phase(txn_id)
        self.system.commit_phase(txn_id)
        assert self.system.rollback_phase(txn_id) is True
        assert self.system.rollback_phase(txn_id) is True
        assert self.system.rollback_phase(txn_id) is True
        # 只补偿一次
        assert self.system.get_balance("server_b", "gold") == 1000

    # ---------------- Saga 模式：编排 / 状态机 / 补偿 ---------------- #

    def test_case_13(self):
        """Saga 编排：步骤按声明顺序执行"""
        order = []
        steps = [
            SagaStep("list_item", lambda: order.append("list_item")),
            SagaStep("pay", lambda: order.append("pay")),
            SagaStep("deliver", lambda: order.append("deliver")),
        ]
        result = self.system.saga_pattern(steps)
        assert order == ["list_item", "pay", "deliver"]
        assert result.is_completed()

    def test_case_14(self):
        """Saga 状态机：记录每一步状态与整体状态"""
        steps = [
            SagaStep("s1", lambda: None),
            SagaStep("s2", lambda: None),
        ]
        result = self.system.saga_pattern(steps)
        assert result.status == SagaStatus.COMPLETED
        assert result.step_states == {"s1": "DONE", "s2": "DONE"}

    def test_case_15(self):
        """Saga 补偿：步骤失败时反向补偿已完成步骤"""
        compensated = []

        def fail():
            raise RuntimeError("支付失败")

        steps = [
            SagaStep("list_item", lambda: None,
                     compensate=lambda: compensated.append("list_item")),
            SagaStep("pay", fail,
                     compensate=lambda: compensated.append("pay")),
            SagaStep("deliver", lambda: None,
                     compensate=lambda: compensated.append("deliver")),
        ]
        result = self.system.saga_pattern(steps)
        assert result.is_compensated()
        assert compensated == ["list_item"]  # 只补偿已完成的步骤
        assert result.step_states["list_item"] == "COMPENSATED"
        assert result.step_states["pay"] == "FAILED"
        assert result.step_states["deliver"] == "PENDING"

    # ---------------- 崩溃恢复 ---------------- #

    def test_case_16(self):
        """崩溃恢复：WAL持久化的事务状态可恢复"""
        import tempfile
        wal_path = os.path.join(tempfile.mkdtemp(), "txn.wal")
        system = make_trading_system(wal_path)
        txn_id = system.begin_transaction()
        system.stage_operation(txn_id, "server_b", "gold", -100)
        system.prepare_phase(txn_id)
        # 模拟崩溃：丢弃内存对象，从WAL重建
        recovered = make_trading_system(wal_path)
        assert recovered.get_transaction_status(txn_id) == TxnStatus.PREPARED
        # 恢复后可以继续提交，最终一致
        assert recovered.commit_phase(txn_id) is True
        assert recovered.get_balance("server_b", "gold") == 900


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
