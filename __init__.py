"""
SWEN v3 initialization for Opencode
"""

import asyncio
from typing import Optional

# Import agent
from .agent import Swen3Agent, Swen3Config, get_agent
from .commands import handle_command


class Swen3Extension:
    """Opencode extension for SWEN v3"""
    
    def __init__(self):
        self.agent: Optional[Swen3Agent] = None
        self.enabled = False
    
    async def initialize(self, config: dict):
        """Initialize SWEN v3 agent"""
        swen_config = config.get("swen3", {})
        
        if not swen_config.get("enabled", True):
            return
        
        # Create config
        cfg = Swen3Config(
            enabled=swen_config.get("enabled", True),
            transport=swen_config.get("transport", "zenoh"),
            zenoh_mode=swen_config.get("zenoh", {}).get("mode", "peer"),
            zenoh_connect=swen_config.get("zenoh", {}).get("connect", [
                "tcp/diana-zenoh.kolibri-jetson1.uk:7447"
            ]),
            zenoh_listen=swen_config.get("zenoh", {}).get("listen", [
                "tcp/0.0.0.0:7447"
            ]),
            workers=swen_config.get("workers", [
                {"name": "kimi-coder", "type": "remote", "host": "diana", "model": "kimi-k2-6"},
                {"name": "lm-studio", "type": "remote", "host": "diana", "model": "qwen2.5-32b"},
            ]),
            judge_model=swen_config.get("judge", {}).get("model", "kimi-k2-6"),
            auto_select=swen_config.get("judge", {}).get("auto_select", True),
            fallback_enabled=swen_config.get("fallback", {}).get("enabled", True),
        )
        
        # Create agent
        self.agent = Swen3Agent(cfg)
        self.enabled = True
        
        # Try to connect
        try:
            success = await self.agent.connect()
            if success:
                print("✅ SWEN v3: Connected to mesh")
            else:
                print(f"⚠️  SWEN v3: Connection failed ({self.agent.status})")
        except Exception as e:
            print(f"⚠️  SWEN v3: Connection error ({e})")
    
    async def shutdown(self):
        """Shutdown SWEN v3 agent"""
        if self.agent:
            await self.agent.disconnect()
            print("SWEN v3: Disconnected")
    
    def get_status_line(self) -> str:
        """Get status line for opencode prompt"""
        if not self.enabled or not self.agent:
            return ""
        
        if not self.agent._connected:
            return "[SWEN: ○ offline]"
        
        available = sum(1 for w in self.agent.workers.values() if w.is_available)
        total = len(self.agent.workers)
        
        return f"[SWEN: ● {available}/{total}]"


# Global extension instance
_extension: Optional[Swen3Extension] = None


def get_extension() -> Swen3Extension:
    """Get or create extension"""
    global _extension
    if _extension is None:
        _extension = Swen3Extension()
    return _extension


async def init_swen3(config: dict):
    """Initialize SWEN v3"""
    ext = get_extension()
    await ext.initialize(config)
    return ext


async def shutdown_swen3():
    """Shutdown SWEN v3"""
    ext = get_extension()
    await ext.shutdown()


# Export
__all__ = [
    "Swen3Agent",
    "Swen3Config",
    "Swen3Extension",
    "get_agent",
    "get_extension",
    "handle_command",
    "init_swen3",
    "shutdown_swen3",
]
