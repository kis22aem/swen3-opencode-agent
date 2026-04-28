# SWEN v3 Agent for Opencode

Нативный агент SWEN v3 для opencode с поддержкой Zenoh mesh.

## Возможности

- Подключение к распределённому рою через Zenoh
- Автоматическое делегирование задач нескольким агентам
- Judge для выбора лучшего ответа
- Консольный статус в реальном времени
- Fallback на локального агента при недоступности роя

## Установка

```bash
# 1. Установить зависимости
pip3 install eclipse-zenoh

# 2. Запустить setup
bash ~/.config/opencode/agents/swen3/setup.sh

# 3. Убедиться что opencode.json содержит swen3 конфиг
```

## Конфигурация

В `~/.config/opencode/opencode.json`:

```json
{
  "agents": {
    "swen3": {
      "enabled": true,
      "transport": "zenoh",
      "zenoh": {
        "mode": "peer",
        "connect": ["tcp/diana-zenoh.kolibri-jetson1.uk:7447"],
        "listen": ["tcp/0.0.0.0:7447"]
      },
      "workers": [
        {"name": "kimi-coder", "type": "remote", "host": "diana", "model": "kimi-k2-6"},
        {"name": "lm-studio", "type": "remote", "host": "diana", "model": "qwen2.5-32b"}
      ],
      "judge": {
        "model": "kimi-k2-6",
        "auto_select": true
      }
    }
  }
}
```

## Команды

| Команда | Описание |
|---------|----------|
| `/swen status` | Статус роя |
| `/swen workers` | Список воркеров |
| `/swen ask <вопрос>` | Задать вопрос рою |
| `/swen connect` | Подключиться к сети |
| `/swen disconnect` | Отключиться |
| `/swen mode <local|swarm>` | Режим работы |

## Архитектура

```
[Opencode] <-> [Zenoh Mesh] <-> [Diana]
                    ↓
            [Kimi Worker]
            [LM Studio Worker]
                    ↓
               [Judge]
```

## Статус разработки

- [x] Базовый агент
- [x] Zenoh клиент
- [x] Команды CLI
- [x] Конфигурация
- [ ] Интеграция с opencode UI
- [ ] Асинхронный режим
- [ ] Веб-интерфейс

## Лицензия

MIT
