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
    IsolationLevel,
    Participant,
    SagaStatus,
    TransactionState,
)


class TestDistributedTransaction:
    def setup_method(self):
        self.system = DistributedTransaction()

    # ---- begin_transaction：事务ID / 隔离 / 日志 ----

    def test_case_01(self):
        """事务开始生成全局唯一的事务ID"""
        tid1 = self.system.begin_transaction()
        tid2 = self.system.begin_transaction()
        assert tid1 and tid2
        assert tid1 != tid2
        assert self.system.get_state(tid1) == TransactionState.ACTIVE

    def test_case_02(self):
        """事务开始写入BEGIN日志，崩溃后可恢复依据"""
        tid = self.system.begin_transaction()
        actions = [r.action for r in self.system.transaction_log
                   if r.txn_id == tid]
        assert "BEGIN" in actions

    def test_case_03(self):
        """隔离性：未提交的写入对其他事务不可见"""
        tid1 = self.system.begin_transaction(IsolationLevel.READ_COMMITTED)
        tid2 = self.system.begin_transaction(IsolationLevel.READ_COMMITTED)
        self.system.write(tid1, "item:1001", "屠龙刀")
        assert self.system.read("item:1001", txn_id=tid2) is None
        assert self.system.read("item:1001") is None

    def test_case_04(self):
        """事务可以读到自己的未提交写入（read-your-writes）"""
        tid = self.system.begin_transaction()
        self.system.write(tid, "item:1001", "屠龙刀")
        assert self.system.read("item:1001", txn_id=tid) == "屠龙刀"

    # ---- prepare_phase：检查 / 持久化 ----

    def test_case_05(self):
        """prepare成功：所有参与者通过检查，状态变为PREPARED"""
        tid = self.system.begin_transaction()
        pa, pb = Participant("serverA"), Participant("serverB")
        self.system.register_participant(tid, pa)
        self.system.register_participant(tid, pb)
        assert self.system.prepare_phase(tid) is True
        assert self.system.get_state(tid) == TransactionState.PREPARED
        assert tid in pa.prepared and tid in pb.prepared

    def test_case_06(self):
        """prepare检查：资源不足时prepare失败并自动回滚"""
        tid = self.system.begin_transaction()
        ok_participant = Participant("serverA")
        broke_participant = Participant("serverB",
                                        on_prepare=lambda txn: False)
        self.system.register_participant(tid, ok_participant)
        self.system.register_participant(tid, broke_participant)
        assert self.system.prepare_phase(tid) is False
        assert self.system.get_state(tid) == TransactionState.ABORTED
        assert tid in ok_participant.rolled_back  # 已prepare的被补偿

    def test_case_07(self):
        """prepare持久化：PREPARED记录写入日志，崩溃恢复可识别"""
        tid = self.system.begin_transaction()
        self.system.register_participant(tid, Participant("serverA"))
        self.system.prepare_phase(tid)
        in_doubt = self.system.recover()
        assert tid in in_doubt
        assert self.system.get_state(tid) == TransactionState.PREPARED

    # ---- commit_phase：原子性 / 通知 ----

    def test_case_08(self):
        """commit把缓冲的写入原子地应用到全局存储"""
        tid = self.system.begin_transaction()
        self.system.write(tid, "item:1001", "屠龙刀")
        self.system.write(tid, "gold:uid396", 500)
        self.system.register_participant(tid, Participant("serverA"))
        assert self.system.prepare_phase(tid) is True
        assert self.system.commit_phase(tid) is True
        assert self.system.read("item:1001") == "屠龙刀"
        assert self.system.read("gold:uid396") == 500
        assert self.system.get_state(tid) == TransactionState.COMMITTED

    def test_case_09(self):
        """prepare失败的事务不允许commit（修复假两阶段提交）"""
        tid = self.system.begin_transaction()
        self.system.register_participant(
            tid, Participant("serverB", on_prepare=lambda txn: False))
        assert self.system.prepare_phase(tid) is False
        assert self.system.commit_phase(tid) is False
        assert self.system.get_state(tid) != TransactionState.COMMITTED

    def test_case_10(self):
        """未prepare直接commit被拒绝"""
        tid = self.system.begin_transaction()
        assert self.system.commit_phase(tid) is False
        assert self.system.get_state(tid) == TransactionState.ACTIVE

    def test_case_11(self):
        """commit原子性：某参与者提交失败时整体回滚，已提交部分被补偿"""
        tid = self.system.begin_transaction()

        def boom(txn):
            raise RuntimeError("B服提交时宕机")

        pa = Participant("serverA")
        pb = Participant("serverB", on_commit=boom)
        self.system.register_participant(tid, pa)
        self.system.register_participant(tid, pb)
        self.system.prepare_phase(tid)
        assert self.system.commit_phase(tid) is False
        assert self.system.get_state(tid) == TransactionState.ABORTED
        assert tid in pa.rolled_back  # 已提交的A服被补偿撤销

    def test_case_12(self):
        """commit成功后通知所有参与者"""
        tid = self.system.begin_transaction()
        pa, pb = Participant("serverA"), Participant("serverB")
        self.system.register_participant(tid, pa)
        self.system.register_participant(tid, pb)
        self.system.prepare_phase(tid)
        assert self.system.commit_phase(tid) is True
        assert tid in pa.notified and tid in pb.notified

    # ---- rollback_phase：补偿 / 幂等 ----

    def test_case_13(self):
        """回滚执行补偿：撤销参与者已执行的操作并丢弃未提交写入"""
        tid = self.system.begin_transaction()
        self.system.write(tid, "item:1001", "屠龙刀")
        pa = Participant("serverA")
        self.system.register_participant(tid, pa)
        assert self.system.rollback_phase(tid) is True
        assert self.system.get_state(tid) == TransactionState.ABORTED
        assert tid in pa.rolled_back
        assert self.system.read("item:1001") is None

    def test_case_14(self):
        """回滚幂等：重复回滚不重复补偿、不报错"""
        tid = self.system.begin_transaction()
        pa = Participant("serverA")
        self.system.register_participant(tid, pa)
        assert self.system.rollback_phase(tid) is True
        assert self.system.rollback_phase(tid) is True
        assert pa.rolled_back.count(tid) == 1

    # ---- saga_pattern：编排 / 状态机 / 补偿 ----

    def test_case_15(self):
        """Saga按编排顺序执行全部步骤，状态机到达COMPLETED"""
        order = []
        steps = [
            ("扣A服道具", lambda: order.append("扣道具"), None),
            ("扣B服金币", lambda: order.append("扣金币"), None),
            ("发B服道具", lambda: order.append("发道具"), None),
        ]
        result = self.system.saga_pattern(steps, name="跨服交易")
        assert result.success is True
        assert result.status == SagaStatus.COMPLETED
        assert order == ["扣道具", "扣金币", "发道具"]
        assert result.executed_steps == [s[0] for s in steps]
        assert result.current_step == 3

    def test_case_16(self):
        """Saga中途失败：状态机记录失败步骤，反向补偿已完成步骤"""
        events = []

        def fail():
            raise RuntimeError("B服库存不足")

        steps = [
            ("扣A服道具", lambda: events.append("do:扣道具"),
             lambda: events.append("undo:扣道具")),
            ("扣B服金币", lambda: events.append("do:扣金币"),
             lambda: events.append("undo:扣金币")),
            ("发B服道具", fail, None),
        ]
        result = self.system.saga_pattern(steps, name="跨服交易")
        assert result.success is False
        assert result.status == SagaStatus.COMPENSATED
        assert result.current_step == 2  # 状态机停在失败步骤
        assert events == ["do:扣道具", "do:扣金币",
                          "undo:扣金币", "undo:扣道具"]
        assert result.compensated_steps == ["扣B服金币", "扣A服道具"]
        assert result.error is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
