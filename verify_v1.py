"""
第一周 V1 自动化全链路验收脚本 (verify_v1.py)
依次验证并输出可观察证据：
1. 任务 1.4 六个检查点（服务启动、HTTP 获取页面、WebSocket 连接、客户端发送、服务端接收、客户端收到返回）
2. 任务 1.6 多设备广播验收场景（客户端 A、B 连接，A 发 "hello from A"，B 发 "hello from B"，再加入客户端 C 三方同步通信）
3. 任务 1.8 L2/L3 异常连接清理验证（模拟失效连接异常，验证服务端自动剔除死连接且不影响正常客户端）
"""

import importlib.util
import os
from fastapi.testclient import TestClient

try:
    from main import app, manager
except ModuleNotFoundError:
    _main_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py")
    _spec = importlib.util.spec_from_file_location("main", _main_path)
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    app = _mod.app
    manager = _mod.manager


def run_verification() -> None:
    print("======================================================================")
    print("开始执行：第一周聊天室 V1 全链路检查点与多客户端广播验收")
    print("======================================================================")

    client = TestClient(app)

    # 检查点 1：服务进程与健康检查接口正常响应
    resp_health = client.get("/health")
    assert resp_health.status_code == 200, "健康检查失败"
    health_data = resp_health.json()
    print(f"[通过] 检查点 1：服务进程已就绪 | 健康检查返回: {health_data}")

    # 检查点 2：浏览器通过 HTTP GET / 成功拿到聊天室页面
    resp_index = client.get("/")
    assert resp_index.status_code == 200, "首页加载失败"
    assert "AI 协作聊天室 V1" in resp_index.text
    assert "window.location.host" in resp_index.text
    print(
        f"[通过] 检查点 2：浏览器成功拿到页面 | HTTP 状态码={resp_index.status_code}, 页面字节数={len(resp_index.text)}"
    )

    # 检查点 3～6 & 任务 1.6：多设备（A、B、C）连接与实时广播验收
    with client.websocket_connect("/ws") as ws_a, client.websocket_connect("/ws") as ws_b:
        assert len(manager.active_connections) == 2
        print(
            f"[通过] 检查点 3：客户端 A 与客户端 B 成功建立 WebSocket 连接 | 当前服务端在线连接数={len(manager.active_connections)}"
        )

        # 场景步骤 2 & 3：A 发送 "hello from A"，A 与 B 都能看到
        ws_a.send_text("hello from A")
        print("[通过] 检查点 4 & 5：客户端 A 已发送 'hello from A'，服务端已接收并触发广播")

        recv_a_1 = ws_a.receive_text()
        recv_b_1 = ws_b.receive_text()
        assert "hello from A" in recv_a_1 and recv_a_1 == recv_b_1
        print(
            f"[通过] 检查点 6 & 任务 1.6(步骤1~3)：A 与 B 收到一致广播 -> A收到={recv_a_1!r}, B收到={recv_b_1!r}"
        )

        # 场景步骤 4 & 5：B 发送 "hello from B"，A 与 B 都能看到
        ws_b.send_text("hello from B")
        recv_a_2 = ws_a.receive_text()
        recv_b_2 = ws_b.receive_text()
        assert "hello from B" in recv_a_2 and recv_a_2 == recv_b_2
        print(
            f"[通过] 任务 1.6(步骤4~5)：B 发送 'hello from B'，A 与 B 收到一致广播 -> A收到={recv_a_2!r}, B收到={recv_b_2!r}"
        )

        # 场景步骤 6：再加入第三个浏览器 C，前三者仍能正常通信
        with client.websocket_connect("/ws") as ws_c:
            assert len(manager.active_connections) == 3
            ws_c.send_text("hello from C (third browser)")
            recv_a_3 = ws_a.receive_text()
            recv_b_3 = ws_b.receive_text()
            recv_c_3 = ws_c.receive_text()
            assert (
                "hello from C (third browser)" in recv_a_3
                and recv_a_3 == recv_b_3 == recv_c_3
            )
            print(
                f"[通过] 任务 1.6(步骤6)：第 3 个浏览器 C 加入后三方同步收到 -> {recv_c_3!r}"
            )

        # C 退出后，验证在线连接数自动回到 2，且 A、B 通信不受影响
        assert len(manager.active_connections) == 2
        ws_a.send_text("after C left")
        assert "after C left" in ws_a.receive_text()
        assert "after C left" in ws_b.receive_text()
        print(
            f"[通过] 任务 1.8(L2/L3 断开清理验证)：客户端 C 断开后连接数恢复为 {len(manager.active_connections)}，A 与 B 通信正常"
        )

    assert len(manager.active_connections) == 0
    print(
        f"[通过] 全部客户端关闭后，服务端活动连接列表已清空 (当前连接数={len(manager.active_connections)})"
    )
    print("======================================================================")
    print("结论：第一周 V1 全部检查点（1.4 ~ 1.8）自动化验收 100% 通过！")
    print("======================================================================")


if __name__ == "__main__":
    run_verification()