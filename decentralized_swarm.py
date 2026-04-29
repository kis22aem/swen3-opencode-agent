#!/usr/bin/env python3
"""
DecentralizedSwarm — полностью децентрализованный рой без Zenoh роутера
Peer-to-peer discovery через multicast, heartbeat каждые 10 сек
"""

import json
import time
import uuid
import socket
import struct
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field, asdict

import zenoh
from zenoh import handlers as zenoh_handlers

# ── Configuration ───────────────────────────────────────────────────────────

MULTICAST_GROUP = "224.0.0.251"  # mDNS multicast group
MULTICAST_PORT = 5353            # mDNS port
HEARTBEAT_INTERVAL = 10          # секунды
WORKER_TIMEOUT = 30              # секунды — считать воркера мёртвым
SWARM_PREFIX = "swen_v3"         # префикс для идентификации наших воркеров

# ── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class WorkerInfo:
    """Информация о воркере в рое"""
    worker_id: str
    role: str
    model: str
    host: str
    zenoh_endpoint: str  # tcp/10.15.x.x:7447
    capabilities: List[str] = field(default_factory=list)
    status: str = "online"
    last_seen: float = field(default_factory=time.time)
    latency_ms: int = 0
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "WorkerInfo":
        return cls(**data)
    
    @property
    def is_alive(self) -> bool:
        return (time.time() - self.last_seen) < WORKER_TIMEOUT


@dataclass
class SwarmMessage:
    """Сообщение в рое"""
    msg_type: str  # heartbeat, discover, request, response
    sender_id: str
    payload: dict
    timestamp: float = field(default_factory=time.time)
    
    def to_json(self) -> str:
        return json.dumps({
            "msg_type": self.msg_type,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "swarm_prefix": SWARM_PREFIX
        })
    
    @classmethod
    def from_json(cls, data: str) -> Optional["SwarmMessage"]:
        try:
            obj = json.loads(data)
            if obj.get("swarm_prefix") != SWARM_PREFIX:
                return None  # Не наше сообщение
            return cls(
                msg_type=obj["msg_type"],
                sender_id=obj["sender_id"],
                payload=obj["payload"],
                timestamp=obj["timestamp"]
            )
        except:
            return None

# ── Multicast Discovery ─────────────────────────────────────────────────────

class MulticastDiscovery:
    """Multicast discovery для поиска воркеров в локальной сети"""
    
    def __init__(self, worker_id: str, role: str, model: str, zenoh_endpoint: str):
        self.worker_id = worker_id
        self.role = role
        self.model = model
        self.zenoh_endpoint = zenoh_endpoint
        self.running = False
        self.socket: Optional[socket.socket] = None
        self.discovered_workers: Dict[str, WorkerInfo] = {}
        self._lock = threading.Lock()
        
    def start(self):
        """Запускает multicast listener и sender"""
        self.running = True
        
        # Создаём UDP socket для multicast
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass  # Windows may not have SO_REUSEPORT
        self.socket.bind(("", MULTICAST_PORT))
        
        # Присоединяемся к multicast group
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        # Запускаем listener в фоне
        listener = threading.Thread(target=self._listen, daemon=True)
        listener.start()
        
        # Запускаем heartbeat sender
        sender = threading.Thread(target=self._send_heartbeat, daemon=True)
        sender.start()
        
        # Запускаем cleaner для удаления мёртвых воркеров
        cleaner = threading.Thread(target=self._clean_dead_workers, daemon=True)
        cleaner.start()
        
        print(f"🔍 Multicast discovery started")
        print(f"   Group: {MULTICAST_GROUP}:{MULTICAST_PORT}")
        print(f"   Worker: {self.worker_id}")
        
    def stop(self):
        """Останавливает discovery"""
        self.running = False
        if self.socket:
            self.socket.close()
            
    def _listen(self):
        """Слушает multicast сообщения от других воркеров"""
        while self.running:
            try:
                data, addr = self.socket.recvfrom(4096)
                msg = SwarmMessage.from_json(data.decode())
                if not msg:
                    continue
                    
                if msg.msg_type == "heartbeat":
                    self._handle_heartbeat(msg, addr[0])
                elif msg.msg_type == "discover":
                    self._handle_discover_request(addr[0])
                    
            except Exception as e:
                if self.running:
                    print(f"❌ Multicast listener error: {e}")
                    
    def _handle_heartbeat(self, msg: SwarmMessage, host: str):
        """Обрабатывает heartbeat от другого воркера"""
        payload = msg.payload
        worker_id = payload.get("worker_id")
        
        if worker_id == self.worker_id:
            return  # Свой heartbeat игнорируем
            
        with self._lock:
            if worker_id in self.discovered_workers:
                # Обновляем существующего воркера
                worker = self.discovered_workers[worker_id]
                worker.last_seen = time.time()
                worker.status = "online"
            else:
                # Новый воркер!
                worker = WorkerInfo(
                    worker_id=worker_id,
                    role=payload.get("role", "unknown"),
                    model=payload.get("model", "unknown"),
                    host=host,
                    zenoh_endpoint=payload.get("zenoh_endpoint", ""),
                    capabilities=payload.get("capabilities", [])
                )
                self.discovered_workers[worker_id] = worker
                print(f"🆕 New worker discovered: {worker_id} ({worker.role}) at {host}")
                
    def _handle_discover_request(self, host: str):
        """Отвечает на запрос обнаружения"""
        self._send_heartbeat_to(host)
        
    def _send_heartbeat(self):
        """Отправляет heartbeat каждые HEARTBEAT_INTERVAL секунд"""
        while self.running:
            try:
                self._broadcast_heartbeat()
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                if self.running:
                    print(f"❌ Heartbeat error: {e}")
                    
    def _broadcast_heartbeat(self):
        """Broadcast heartbeat на multicast group"""
        msg = SwarmMessage(
            msg_type="heartbeat",
            sender_id=self.worker_id,
            payload={
                "worker_id": self.worker_id,
                "role": self.role,
                "model": self.model,
                "zenoh_endpoint": self.zenoh_endpoint,
                "capabilities": ["chat", "reasoning"]
            }
        )
        
        self.socket.sendto(
            msg.to_json().encode(),
            (MULTICAST_GROUP, MULTICAST_PORT)
        )
        
    def _send_heartbeat_to(self, host: str):
        """Отправляет heartbeat конкретному хосту (unicast)"""
        msg = SwarmMessage(
            msg_type="heartbeat",
            sender_id=self.worker_id,
            payload={
                "worker_id": self.worker_id,
                "role": self.role,
                "model": self.model,
                "zenoh_endpoint": self.zenoh_endpoint,
                "capabilities": ["chat", "reasoning"]
            }
        )
        
        self.socket.sendto(
            msg.to_json().encode(),
            (host, MULTICAST_PORT)
        )
        
    def _clean_dead_workers(self):
        """Удаляет мёртвых воркеров каждые 10 секунд"""
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                dead_workers = [
                    wid for wid, w in self.discovered_workers.items()
                    if not w.is_alive
                ]
                for wid in dead_workers:
                    worker = self.discovered_workers.pop(wid)
                    print(f"💀 Worker dead: {wid} ({worker.role})")
                    
    def get_alive_workers(self) -> List[WorkerInfo]:
        """Возвращает список живых воркеров"""
        with self._lock:
            return [w for w in self.discovered_workers.values() if w.is_alive]
            
    def discover_all(self):
        """Отправляет discover запрос и ждёт ответов"""
        print("🔍 Sending discover request...")
        msg = SwarmMessage(
            msg_type="discover",
            sender_id=self.worker_id,
            payload={}
        )
        self.socket.sendto(
            msg.to_json().encode(),
            (MULTICAST_GROUP, MULTICAST_PORT)
        )
        # Ждём 2 секунды для сбора ответов
        time.sleep(2)

# ── Decentralized Swarm Backend ─────────────────────────────────────────────

class DecentralizedSwarm:
    """Полностью децентрализованный рой"""
    
    def __init__(self, worker_id: str, role: str, model: str, zenoh_endpoint: str):
        self.worker_id = worker_id
        self.role = role
        self.model = model
        self.zenoh_endpoint = zenoh_endpoint
        
        self.discovery: Optional[MulticastDiscovery] = None
        self.zenoh_session: Optional[zenoh.Session] = None
        self.queryable = None
        
    def start(self):
        """Запускает децентрализованный рой"""
        print("=" * 60)
        print("  Decentralized Swarm v4")
        print("  Fully P2P — No Router Required")
        print("=" * 60)
        
        # 1. Запускаем multicast discovery
        self.discovery = MulticastDiscovery(
            self.worker_id, self.role, self.model, self.zenoh_endpoint
        )
        self.discovery.start()
        
        # 2. Отправляем discover запрос
        self.discovery.discover_all()
        
        # 3. Подключаемся к Zenoh в peer mode (без роутера)
        self._connect_zenoh()
        
        # 4. Регистрируем queryable для обработки запросов
        self._register_queryable()
        
        print(f"\n🚀 Swarm node started")
        print(f"   ID: {self.worker_id}")
        print(f"   Role: {self.role}")
        print(f"   Zenoh: {self.zenoh_endpoint}")
        print(f"   Workers found: {len(self.discovery.get_alive_workers())}")
        
    def _connect_zenoh(self):
        """Подключается к Zenoh в peer mode"""
        try:
            cfg = zenoh.Config()
            cfg.insert_json5("mode", '"peer"')
            
            # Слушаем на всех интерфейсах
            cfg.insert_json5("listen/endpoints", '["tcp/0.0.0.0:0"]')
            
            # Включаем multicast scouting для P2P
            cfg.insert_json5("scouting/multicast/enabled", "true")
            cfg.insert_json5("scouting/multicast/address", f'"{MULTICAST_GROUP}"')
            cfg.insert_json5("scouting/multicast/port", str(MULTICAST_PORT))
            cfg.insert_json5("scouting/gossip/enabled", "true")
            
            self.zenoh_session = zenoh.open(cfg)
            print("✅ Zenoh P2P connected")
            
        except Exception as e:
            print(f"❌ Zenoh connection failed: {e}")
            
    def _register_queryable(self):
        """Регистрирует queryable для обработки запросов"""
        if not self.zenoh_session:
            return
            
        queryable_key = f"swen/v4/ask/{self.role}"
        
        def handle_query(query):
            try:
                payload = bytes(query.payload).decode() if query.payload else "{}"
                req = json.loads(payload)
                
                print(f"📨 Query received: {req.get('question', '')[:50]}...")
                
                # Здесь должна быть обработка запроса
                # Для примера — простой ответ
                response = {
                    "worker_id": self.worker_id,
                    "role": self.role,
                    "ok": True,
                    "answer": f"Response from {self.worker_id}",
                    "latency_ms": 0,
                    "model": self.model
                }
                
                query.reply(query.key_expr, json.dumps(response).encode())
                
            except Exception as e:
                print(f"❌ Query handling error: {e}")
                
        self.queryable = self.zenoh_session.declare_queryable(
            queryable_key,
            handle_query
        )
        print(f"✅ Queryable registered: {queryable_key}")
        
    def stop(self):
        """Останавливает рой"""
        if self.discovery:
            self.discovery.stop()
        if self.queryable:
            self.queryable.undeclare()
        if self.zenoh_session:
            self.zenoh_session.close()
        print("🛑 Swarm stopped")
        
    def get_workers(self) -> List[WorkerInfo]:
        """Возвращает список всех живых воркеров"""
        if self.discovery:
            return self.discovery.get_alive_workers()
        return []


# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    
    # Получаем параметры из аргументов или env
    worker_id = sys.argv[1] if len(sys.argv) > 1 else f"worker-{uuid.uuid4().hex[:8]}"
    role = sys.argv[2] if len(sys.argv) > 2 else "generic"
    model = sys.argv[3] if len(sys.argv) > 3 else "unknown"
    
    # Определяем локальный IP для Zenoh endpoint
    hostname = socket.gethostname()
    local_ip = socket.getaddrinfo(hostname, None, socket.AF_INET)[0][4][0]
    zenoh_endpoint = f"tcp/{local_ip}:7447"
    
    swarm = DecentralizedSwarm(worker_id, role, model, zenoh_endpoint)
    
    try:
        swarm.start()
        
        # Показываем статус каждые 10 секунд
        while True:
            time.sleep(10)
            workers = swarm.get_workers()
            print(f"\n📊 Swarm status: {len(workers)} alive workers")
            for w in workers:
                print(f"   • {w.worker_id} ({w.role}) — {w.host}")
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping...")
        swarm.stop()
