# QuiniAI Snapshot Worker

Este worker está pensado para un segundo ordenador que quede encendido y vaya
subiendo un snapshot de contexto al backend de QuiniAI para que la IA avanzada
no dependa siempre de búsquedas en tiempo real.

## Variables necesarias

Crear un archivo `.env` junto a `snapshot_worker.py`:

```env
ODDS_API_KEY=tu_clave_de_the_odds_api
QUINIAI_ADMIN_KEY=tu_admin_key_del_backend
QUINIAI_BACKEND_URL=https://quiniela-backend-production-cb1a.up.railway.app
SNAPSHOT_POLL_SECONDS=900
```

## Arranque rápido

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 snapshot_worker.py
```

## Qué hace

1. Consulta cuotas de varias ligas relevantes.
2. Genera `ia_feed_snapshot.json` en local.
3. Sube el snapshot al endpoint `/admin/ia-feed` del backend.
4. Repite el proceso cada `SNAPSHOT_POLL_SECONDS`.
