# QuiniAI Snapshot Worker

Este worker esta pensado para un segundo ordenador que quede encendido y vaya
subiendo un snapshot de contexto externo enriquecido al backend de QuiniAI.

## Variables necesarias

Crear un archivo `.env` junto a `snapshot_worker.py`:

```env
QUINIAI_ADMIN_KEY=tu_admin_key_del_backend
QUINIAI_BACKEND_URL=https://quiniela-backend-production-cb1a.up.railway.app
SNAPSHOT_POLL_SECONDS=900
QUINIAI_DATA_URL=https://raw.githubusercontent.com/Macapostes/quiniai-data/main/cuotas.json
```

## Arranque rapido en Windows

1. Doble clic en `Iniciar QuiniAI Worker.cmd` para arrancarlo ahora.
2. Doble clic en `Activar Autoarranque QuiniAI Worker.cmd` para que se lance solo al iniciar sesion.

## Que hace ahora mismo

1. Descarga las cuotas publicadas en `quiniai-data`.
2. Enriquece cada partido con contexto adicional:
   - noticias recientes por equipo
   - noticias especificas de partido para los 15 focos quiniela
   - senales de lesiones, rotaciones y disciplina en titulares
   - capa estructurada de lesiones por equipo para los 15 focos
   - capa estructurada de arbitros cuando la fuente gratuita lo permite
   - clima previsto
   - contexto geografico y distancia aproximada de viaje
   - perfiles base de equipos via Wikipedia
   - historicos recientes, forma y H2H en ligas domesticamente soportadas
   - probabilidades implicitas y overround del mercado
3. Genera `ia_feed_snapshot.json` en local.
4. Sube el snapshot al endpoint `/admin/ia-feed` del backend.
5. Repite el proceso cada `SNAPSHOT_POLL_SECONDS`.

## Cobertura actual del snapshot

- Monitoriza todos los partidos del feed externo, no solo 15.
- Marca ademas 15 partidos en `quiniela_focus_matches` para tener un foco tipo quiniela.
- Ahora mismo combina cuotas, noticias, clima, viajes e historicos cuando la fuente lo soporta.
- Mantiene una base estructurada local en `cache/structured_context_db.json`.
- Esa base conserva solo los partidos foco activos y los recientes; los partidos viejos se podan automaticamente.

## Fuentes gratuitas usadas

- GitHub raw para el snapshot de cuotas
- Wikipedia Action API para perfiles de equipos
- Open-Meteo para geocodificacion y clima
- Google News RSS para noticias y senales recientes
- Football-Data CSV para historicos y forma en ligas soportadas
- TheSportsDB para IDs, venue y metadatos de evento cuando estan disponibles
