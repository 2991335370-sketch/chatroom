"""
四周 AI 协作聊天室工程训练 —— 第一周 V1 服务端实现 (main.py)
技术路线：浏览器页面 + Python/FastAPI + WebSocket

涵盖任务：
- 任务 1.4：打通第一条连接（提供 HTML 页面、建立 WebSocket、收发消息并输出服务端日志证据）
- 任务 1.5：让另一台设备找到服务器（监听 0.0.0.0:8000，自动检测本机局域网 IP，前端动态使用 window.location.host）
- 任务 1.6：从“回声”升级为“广播”（维护在线连接列表 active_connections，向所有在线客户端广播消息）
- 任务 1.8：完成 V1 验收（L1 基础通信 + L2 连接/消息/断开完整日志 + L3 异常连接容错清理、消息时间戳、基础页面布局与测试数据生成）
"""

from datetime import datetime
import logging
import socket
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# 配置可读性强的服务端结构化日志（满足 L2：服务端能记录连接、消息和断开事件）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("chatroom-v1")

app = FastAPI(
    title="AI 协作聊天室 V1",
    description="第一周任务包：让多人聊天真正跑起来（多设备局域网 WebSocket 实时广播）",
    version="1.0.0",
)


def get_lan_ip() -> str:
    """获取本机在局域网中的 IP 地址，方便第二台设备通过浏览器直接访问。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 无需真正连通外网，仅通过 UDP 路由表查询本机默认网卡 IP
        sock.connect(("10.255.255.255", 1))
        ip = sock.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        sock.close()
    return ip


class ConnectionManager:
    """
    管理当前所有活动的 WebSocket 连接（对应任务 1.6 讨论问题 1～4）：
    1. 用 self.active_connections 列表记录当前有哪些浏览器已连接；
    2. 新连接建立时 accept 并加入列表；
    3. 连接正常断开时从列表移除；
    4. 广播时若遇到异常断开的“死连接”，单独捕获异常并清理，不阻塞其他正常用户收消息（L3 挑战）。
    """

    def __init__(self) -> None:
        self.active_connections: List[WebSocket] = []

    @staticmethod
    def _format_client(websocket: WebSocket) -> str:
        if websocket.client:
            return f"{websocket.client.host}:{websocket.client.port}"
        return "unknown-client"

    async def connect(self, websocket: WebSocket) -> str:
        await websocket.accept()
        self.active_connections.append(websocket)
        client_id = self._format_client(websocket)
        logger.info(
            "[EVENT=CONNECT] 新连接已建立 | 客户端=%s | 当前在线连接数=%d",
            client_id,
            len(self.active_connections),
        )
        return client_id

    def disconnect(self, websocket: WebSocket, reason: str = "客户端主动断开") -> None:
        client_id = self._format_client(websocket)
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(
            "[EVENT=DISCONNECT] 连接已移除 | 客户端=%s | 原因=%s | 当前在线连接数=%d",
            client_id,
            reason,
            len(self.active_connections),
        )

    async def broadcast(self, message: str, sender_id: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"[{timestamp}] {message}"
        logger.info(
            "[EVENT=MESSAGE] 收到消息并开始广播 | 来源=%s | 内容=%r | 目标连接数=%d",
            sender_id,
            message,
            len(self.active_connections),
        )

        stale_connections: List[WebSocket] = []
        success_count = 0

        # 遍历副本，防止遍历期间列表长度变化；逐个捕获异常避免单个失效连接中断整个广播（任务 1.6 问题 4 & L3 挑战）
        for connection in list(self.active_connections):
            try:
                await connection.send_text(formatted_msg)
                success_count += 1
            except Exception as exc:
                bad_client = self._format_client(connection)
                logger.warning(
                    "[EVENT=SEND_ERROR] 向客户端发送失败，标记为失效连接 | 客户端=%s | 错误=%s",
                    bad_client,
                    exc,
                )
                stale_connections.append(connection)

        for stale in stale_connections:
            self.disconnect(stale, reason="广播时检测到异常失效连接，自动清理")

        logger.info(
            "[EVENT=BROADCAST_DONE] 广播完成 | 成功送达=%d/%d",
            success_count,
            success_count + len(stale_connections),
        )


manager = ConnectionManager()


def build_html_page(lan_ip: str) -> str:
    """构建简洁、清晰且带可观察日志证据的浏览器前端页面。"""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>AI 协作聊天室 V1（第一周：多人实时广播）</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif;
            background-color: #f4f6f9;
            color: #1f2937;
            padding: 20px;
            line-height: 1.5;
        }}
        .container {{
            max-width: 760px;
            margin: 0 auto;
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
        }}
        header {{
            border-bottom: 1px solid #e5e7eb;
            padding-bottom: 12px;
            margin-bottom: 14px;
        }}
        h1 {{
            font-size: 1.25rem;
            color: #111827;
            margin-bottom: 6px;
        }}
        .status-bar {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            font-size: 0.875rem;
            color: #4b5563;
            background: #f9fafb;
            padding: 8px 12px;
            border-radius: 6px;
            border: 1px solid #e5e7eb;
        }}
        .badge {{
            font-weight: 600;
            padding: 1px 8px;
            border-radius: 999px;
            font-size: 0.8rem;
        }}
        .badge-online {{ background: #dcfce7; color: #166534; }}
        .badge-offline {{ background: #fee2e2; color: #991b1b; }}
        .badge-connecting {{ background: #fef3c7; color: #92400e; }}
        .lan-tip {{
            margin-top: 8px;
            font-size: 0.82rem;
            color: #374151;
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            padding: 6px 10px;
            border-radius: 6px;
        }}
        #messages {{
            height: 320px;
            overflow-y: auto;
            border: 1px solid #d1d5db;
            border-radius: 8px;
            padding: 12px;
            background: #fafafa;
            margin-bottom: 14px;
            list-style: none;
        }}
        #messages li {{
            padding: 6px 10px;
            margin-bottom: 6px;
            border-radius: 6px;
            background: #ffffff;
            border: 1px solid #e5e7eb;
            font-size: 0.92rem;
            word-break: break-all;
        }}
        #messages li.sys-msg {{
            background: #f3f4f6;
            color: #6b7280;
            font-size: 0.84rem;
            border-style: dashed;
        }}
        .input-row {{
            display: flex;
            gap: 8px;
            margin-bottom: 10px;
        }}
        #messageInput {{
            flex: 1;
            padding: 9px 12px;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            font-size: 0.95rem;
        }}
        button {{
            padding: 9px 16px;
            border: none;
            border-radius: 6px;
            font-size: 0.9rem;
            font-weight: 600;
            cursor: pointer;
            background: #2563eb;
            color: #ffffff;
        }}
        button:hover {{ background: #1d4ed8; }}
        button.secondary {{
            background: #4b5563;
            font-size: 0.82rem;
            padding: 6px 12px;
        }}
        button.secondary:hover {{ background: #374151; }}
        .toolbar {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>AI 协作聊天室 V1（第一周：多人实时广播）</h1>
            <div class="status-bar">
                <span>WebSocket 状态：<span id="wsStatus" class="badge badge-connecting">正在连接...</span></span>
                <span>当前通道：<code id="wsUrlText">-</code></span>
            </div>
            <div class="lan-tip">
                <strong>同局域网多设备访问提示：</strong>其他手机或电脑连接同一热点/路由器后，在浏览器打开
                <code>http://{lan_ip}:8000</code> 即可直接进入聊天室（请勿在其他设备上输入 127.0.0.1）。
            </div>
        </header>

        <ul id="messages"></ul>

        <div class="input-row">
            <input id="messageInput" type="text" placeholder="输入聊天消息（例如：你好 / hello from A），按回车或点击发送..." autocomplete="off" />
            <button id="sendBtn" onclick="sendMessage()">发送消息</button>
        </div>

        <div class="toolbar">
            <button class="secondary" onclick="sendQuickTest()">一键发送测试消息 (L3)</button>
            <button class="secondary" onclick="reconnectWs()">手动重连 WebSocket</button>
        </div>
    </div>

    <script>
        const messagesEl = document.getElementById("messages");
        const inputEl = document.getElementById("messageInput");
        const statusEl = document.getElementById("wsStatus");
        const wsUrlTextEl = document.getElementById("wsUrlText");

        // 动态使用 window.location.host，确保本机与局域网其他设备均能连接到正确的服务器 IP 与端口
        const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${{wsProtocol}}//${{window.location.host}}/ws`;
        wsUrlTextEl.textContent = wsUrl;

        let ws = null;
        let testSeq = 1;

        function appendLine(text, isSystem = false) {{
            const li = document.createElement("li");
            if (isSystem) li.className = "sys-msg";
            li.textContent = text;
            messagesEl.appendChild(li);
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }}

        function setStatus(text, cls) {{
            statusEl.textContent = text;
            statusEl.className = "badge " + cls;
        }}

        function connectWebSocket() {{
            if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {{
                ws.close();
            }}
            setStatus("正在连接...", "badge-connecting");
            console.log("[Browser Checkpoint] 正在发起 WebSocket 连接:", wsUrl);
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {{
                setStatus("已连接 (OPEN)", "badge-online");
                console.log("[Browser Checkpoint 3] WebSocket 连接建立成功:", wsUrl);
                appendLine("[系统提示] 已成功连接至聊天服务器 (" + wsUrl + ")", true);
            }};

            ws.onmessage = (event) => {{
                console.log("[Browser Checkpoint 6] 收到服务器广播消息:", event.data);
                appendLine(event.data, false);
            }};

            ws.onclose = (event) => {{
                setStatus("已断开 (CLOSED)", "badge-offline");
                console.warn("[Browser Event] WebSocket 连接已关闭, code=", event.code);
                appendLine("[系统提示] 与服务器的连接已断开，可点击“手动重连 WebSocket”恢复连接。", true);
            }};

            ws.onerror = (err) => {{
                console.error("[Browser Error] WebSocket 发生异常:", err);
            }};
        }}

        function sendMessage() {{
            const text = inputEl.value.trim();
            if (!text) return;
            if (!ws || ws.readyState !== WebSocket.OPEN) {{
                appendLine("[系统提示] 当前未连接服务器，无法发送消息，请先重连。", true);
                return;
            }}
            console.log("[Browser Checkpoint 4] 浏览器正在发送消息:", text);
            ws.send(text);
            inputEl.value = "";
            inputEl.focus();
        }}

        function sendQuickTest() {{
            inputEl.value = `测试消息 #${{testSeq++}} (来自 ${{window.location.hostname}})`;
            sendMessage();
        }}

        function reconnectWs() {{
            appendLine("[系统提示] 正在尝试重新连接服务器...", true);
            connectWebSocket();
        }}

        inputEl.addEventListener("keydown", (event) => {{
            if (event.key === "Enter") {{
                sendMessage();
            }}
        }});

        // 页面加载完成后立即建立 WebSocket 连接
        connectWebSocket();
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def get_chat_page() -> HTMLResponse:
    """检查点 2：浏览器通过 HTTP GET / 拿到聊天室网页。"""
    lan_ip = get_lan_ip()
    logger.info("[EVENT=HTTP_GET_INDEX] 浏览器请求加载聊天室首页 | 本机局域网IP=%s", lan_ip)
    return HTMLResponse(content=build_html_page(lan_ip))


@app.get("/health", response_class=JSONResponse)
async def health_check() -> JSONResponse:
    """健康检查与在线连接数查看接口。"""
    return JSONResponse(
        {
            "status": "ok",
            "version": "v1.0",
            "lan_ip": get_lan_ip(),
            "active_connections": len(manager.active_connections),
        }
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    检查点 3～6 & 任务 1.6：
    - 接收客户端 WebSocket 握手并加入在线列表；
    - 循环接收客户端发来的文本消息，并广播给当前所有在线客户端；
    - 客户端断开时从在线列表移除并记录日志。
    """
    client_id = await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            cleaned = data.strip()
            if not cleaned:
                logger.info("[EVENT=IGNORE_EMPTY] 忽略空白消息 | 客户端=%s", client_id)
                continue
            await manager.broadcast(cleaned, sender_id=client_id)
    except WebSocketDisconnect:
        manager.disconnect(websocket, reason="WebSocketDisconnect 正常关闭")
    except Exception as exc:
        manager.disconnect(websocket, reason=f"未预期连接异常: {exc}")


if __name__ == "__main__":
    lan_ip = get_lan_ip()
    logger.info("============================================================")
    logger.info("AI 协作聊天室 V1 启动中 (监听 0.0.0.0:8000)")
    logger.info("1. 本机浏览器自测地址: http://127.0.0.1:8000")
    logger.info("2. 同局域网其他设备访问地址: http://%s:8000", lan_ip)
    logger.info("============================================================")
    uvicorn.run(app, host="0.0.0.0", port=8000, ws="wsproto")