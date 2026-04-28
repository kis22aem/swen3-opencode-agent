# SWEN v3 Agent for Opencode

Нативный агент SWEN v3 для opencode с поддержкой Zenoh mesh.

## Возможности

- Подключение к распределённому рою через Zenoh P2P
- Параллельный fanout на несколько воркеров
- Judge для выбора лучшего ответа
- Поддержка нескольких моделей на Diana (LM Studio)
- Fallback на локального агента при недоступности роя

## Установка

```bash
# 1. Установить зависимости
pip3 install eclipse-zenoh==1.9.0

# 2. Скопировать код
mkdir -p ~/.local/share/swen3
cp -r * ~/.local/share/swen3/

# 3. Установить Bun для TypeScript tools
curl -fsSL https://bun.sh/install | bash
```

## Конфигурация

### Opencode агент (`~/.config/opencode/agents/swen3.md`)

```yaml
---
description: >
  Distributed AI swarm agent (SWEN v3). Routes questions to a parallel ensemble
  of LLM workers on Diana via Zenoh P2P mesh.
mode: subagent
permission:
  edit: deny
  bash: deny
  read: allow
  glob: allow
  grep: allow
  swen3_ask: allow
---
```

### Opencode tool (`~/.config/opencode/tools/swen3_ask.ts`)

TypeScript tool для вызова роя из opencode TUI.

### Opencode config (`~/.config/opencode/opencode.json`)

```json
{
  "agent": {
    "swen3": {
      "description": "Distributed AI swarm agent (SWEN v3)",
      "mode": "subagent",
      "permission": {
        "edit": "deny",
        "bash": "deny",
        "read": "allow",
        "glob": "allow",
        "grep": "allow",
        "swen3_ask": "allow"
      }
    }
  }
}
```

## Воркеры (Workers)

### Текущий состав роя (Diana, 10.15.64.226:7447)

| Воркер | Модель | Бэкенд | Скорость | Статус |
|--------|--------|--------|----------|--------|
| `glm_flash` | GLM-4.7-Flash | LM Studio | ~30-40s | ✅ Активен |
| `qwen3_5_4b_opus` | Qwen3.5-4B-Opus | LM Studio | ~3-5s | ✅ Активен |

### Добавление нового воркера

1. Обновить профиль на Diana: `D:\swarm_v3\host\swarm_v3\profiles\diana_win.toml`
2. Добавить секцию `[[workers]]` с новой ролью
3. Перезапустить воркеров на Diana
4. Обновить `roles` в `swen3_query.py` или `Swen3Config`

## Использование

### CLI

```bash
# Запрос к рою
source ~/.venvs/swen3/bin/activate
python3 ~/.local/share/swen3/swen3_query.py "Ваш вопрос"

# Проверка статуса
python3 -c "
import sys; sys.path.insert(0, '~/.local/share/swen3')
from agent import Swen3Agent, Swen3Config
a = Swen3Agent(Swen3Config(zenoh_connect=['tcp/10.15.64.226:7447']))
a.connect()
print(a.format_status())
a.disconnect()
"
```

### В opencode TUI

```
@swen3 напиши функцию бинарного поиска на Python
```

## Архитектура

```
[MacBook / Opencode] 
    ↓ Zenoh P2P (tcp/10.15.64.226:7447)
[Diana / Windows]
    ├── LM Studio (127.0.0.1:1234)
    │   ├── glm_flash (GLM-4.7-Flash)
    │   └── qwen3_5_4b_opus (Qwen3.5-4B-Opus)
    └── Zenoh Router (0.0.0.0:7447)
```

## Известные проблемы

### Discovery не работает

**Проблема:** `discover_workers()` возвращает пустой список, хотя воркеры отвечают на запросы.

**Причина:** Воркеры на Diana публикуют карточки (`swen/v3/cards/{id}`) через `session.put()`, но не объявляют queryable для этих ключей. Zenoh `session.get()` требует queryable для получения ответа.

**Решение:** `discover_workers()` обновлён — теперь возвращает настроенные роли из `Swen3Config.roles` вместо попытки живого обнаружения.

**Workaround:** Запросы напрямую через `ask()` работают корректно, так как используют `swen/v3/ask/{role}`, где воркеры объявляют queryables.

## История изменений

### 2026-04-29
- ✅ Добавлен воркер `qwen3_5_4b_opus` (Qwen3.5-4B-Opus)
- ✅ Исправлен `discover_workers()` — возвращает настроенные роли
- ✅ Обновлена документация

### 2026-04-28
- ✅ Создан агент swen3 для opencode
- ✅ TypeScript tool `swen3_ask`
- ✅ Python backend с Zenoh подключением
- ✅ Интеграция с Diana LM Studio

## Лицензия

MIT
