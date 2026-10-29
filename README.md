# Sensor Service

Servicio FastAPI reutilizable para generar temperaturas ficticias. La misma imagen se despliega tres veces cambiando `SENSOR_ID` y `SENSOR_ZONE`.

## Variables

- `SENSOR_ID`
- `SENSOR_ZONE`
- `AGGREGATOR_URL`
- `NORMAL_MIN_C`, `NORMAL_MAX_C`
- `HIGH_ANOMALY_C`, `LOW_ANOMALY_C`

## Ejecución independiente

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn sensor_service.app:app --host 0.0.0.0 --port 8080
```

Normalmente se ejecuta desde el repositorio `hybrid-deployment-orchestrator`.
