"""Servicio de sensor que genera y envía lecturas de temperatura."""

from __future__ import annotations

import asyncio
import json
import os
import time
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from sensor_service.observability import ObservabilityPublisher
from shared.models import SensorReading

SENSOR_ID = os.getenv("SENSOR_ID", "temperature-sensor-1")
SENSOR_ZONE = os.getenv("SENSOR_ZONE", "zone-1")
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://127.0.0.1:8000").rstrip("/")
NORMAL_MIN_C = float(os.getenv("NORMAL_MIN_C", "37.2"))
NORMAL_MAX_C = float(os.getenv("NORMAL_MAX_C", "37.8"))
HIGH_ANOMALY_C = float(os.getenv("HIGH_ANOMALY_C", "39.5"))
LOW_ANOMALY_C = float(os.getenv("LOW_ANOMALY_C", "35.5"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
AUTO_EMIT_INTERVAL_SECONDS = float(os.getenv("AUTO_EMIT_INTERVAL_SECONDS", "0"))
AUTO_ANOMALY_PROBABILITY = float(os.getenv("AUTO_ANOMALY_PROBABILITY", "0.04"))
INITIAL_ENABLED = os.getenv("SENSOR_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
SIMULATED_EXECUTION_TIME = float(os.getenv("SIMULATED_EXECUTION_TIME", "10"))
SIMULATED_RELIABILITY = float(os.getenv("SIMULATED_RELIABILITY", "0.10"))
SIMULATED_AVAILABILITY = float(os.getenv("SIMULATED_AVAILABILITY", "24"))
SIMULATED_COST = float(os.getenv("SIMULATED_COST", "0.05"))
SIMULATED_RESPONSE_TIME_MS = float(os.getenv("SIMULATED_RESPONSE_TIME_MS", "100"))
OBSERVABILITY_URL = os.getenv("OBSERVABILITY_URL", "")
APPLICATION_ID = os.getenv("APPLICATION_ID", "default")

sequence = 0
auto_task: asyncio.Task[None] | None = None
observability = ObservabilityPublisher(OBSERVABILITY_URL, SENSOR_ID)


class EmitRequest(BaseModel):
    """Define los parámetros de una emisión manual del sensor."""
    force_anomaly: bool = False
    anomaly_direction: Literal["high", "low"] = "high"


class SensorStateRequest(BaseModel):
    """Define un cambio explícito en el estado operativo del sensor."""
    enabled: bool
    reason: str | None = None


class SimulationRequest(BaseModel):
    """Define los parámetros de una simulación temporal de incumplimiento."""
    failed: bool | None = None
    execution_time: float | None = None
    reliability: float | None = None
    availability: float | None = None
    cost: float | None = None
    response_time_ms: float | None = None
    reason: str | None = None
    recover_after_seconds: float | None = None


class SensorState(BaseModel):
    """Conserva el estado mutable y las tareas en segundo plano del sensor."""
    enabled: bool = INITIAL_ENABLED
    failed: bool = False
    execution_time: float = SIMULATED_EXECUTION_TIME
    reliability: float = SIMULATED_RELIABILITY
    availability: float = SIMULATED_AVAILABILITY
    cost: float = SIMULATED_COST
    response_time_ms: float = SIMULATED_RESPONSE_TIME_MS
    reason: str | None = None
    recover_at: float | None = None


state = SensorState()


DEFAULT_SIMULATION_VALUES = {
    "failed": False,
    "execution_time": SIMULATED_EXECUTION_TIME,
    "reliability": SIMULATED_RELIABILITY,
    "availability": SIMULATED_AVAILABILITY,
    "cost": SIMULATED_COST,
    "response_time_ms": SIMULATED_RESPONSE_TIME_MS,
}


def recover_if_needed() -> None:
    """Restaura automáticamente los fallos temporales después de 10-15 segundos.

    Esto permite simular degradaciones transitorias, como ocurriría en servicios externos reales.
    El reinicio/redespliegue real sigue existiendo, pero la avería inyectada no queda permanente.
    """
    recover_at = getattr(state, "recover_at", None)
    if recover_at is None or time.time() < float(recover_at):
        return
    state.failed = False
    state.execution_time = DEFAULT_SIMULATION_VALUES["execution_time"]
    state.reliability = DEFAULT_SIMULATION_VALUES["reliability"]
    state.availability = DEFAULT_SIMULATION_VALUES["availability"]
    state.cost = DEFAULT_SIMULATION_VALUES["cost"]
    state.response_time_ms = DEFAULT_SIMULATION_VALUES["response_time_ms"]
    if hasattr(state, "failure_reason"):
        state.failure_reason = None
    if hasattr(state, "reason"):
        state.reason = None
    state.recover_at = None
    append_event({"type": "temporary_failure_recovered", "message": "Sensor vuelve automáticamente a estado correcto tras fallo temporal."})


def now_iso() -> str:
    """Devuelve la fecha y hora actual en formato ISO 8601 y zona UTC."""
    return datetime.now(timezone.utc).isoformat()


def append_event(event: dict[str, Any], *, severity: str = "info") -> None:
    """Emite el evento por stdout y lo publica en el almacén central de observabilidad."""
    payload = {"time": now_iso(), "service": SENSOR_ID, **event}
    print(f"[SENSOR] {json.dumps(payload, ensure_ascii=False)}", flush=True)
    observability.publish_later(
        category="operational",
        event_type=str(event.get("type", "sensor_event")),
        payload=payload,
        application_id=APPLICATION_ID,
        severity=severity,
        service_id=SENSOR_ID,
        timestamp=payload["time"],
    )

def fail_if_simulated() -> None:
    """Aplica el retraso o error configurado por una simulación activa."""
    recover_if_needed()
    if state.failed:
        raise HTTPException(
            status_code=503,
            detail=state.reason or f"Fallo simulado del sensor {SENSOR_ID}.",
        )


def build_temperature(request: EmitRequest) -> float:
    """Calcula la siguiente temperatura simulada por el sensor."""
    if request.force_anomaly:
        return HIGH_ANOMALY_C if request.anomaly_direction == "high" else LOW_ANOMALY_C
    return round(random.uniform(NORMAL_MIN_C, NORMAL_MAX_C), 3)


def random_auto_request() -> EmitRequest:
    """Genera los parámetros aleatorios de una emisión automática."""
    force_anomaly = random.random() < AUTO_ANOMALY_PROBABILITY
    direction: Literal["high", "low"] = random.choice(["high", "low"])
    return EmitRequest(force_anomaly=force_anomaly, anomaly_direction=direction)


async def send_reading(request: EmitRequest) -> dict[str, object]:
    """Envía una lectura al agregador y registra el resultado de la operación."""
    global sequence
    fail_if_simulated()
    # enabled representa la activación lógica dentro de un selection_group.
    # El gestor de despliegue puede cambiarlo mediante el contrato HTTP declarado.
    if not state.enabled:
        raise HTTPException(
            status_code=409,
            detail=f"El sensor {SENSOR_ID} está deshabilitado por el monitor.",
        )

    # La secuencia permite reconstruir el orden de lecturas incluso cuando sus
    # peticiones y registros se procesan de forma asíncrona.
    sequence += 1
    temperature = build_temperature(request)
    reading = SensorReading(
        sensor_id=SENSOR_ID,
        zone=SENSOR_ZONE,
        sequence=sequence,
        timestamp=datetime.now(timezone.utc),
        temperature_c=temperature,
    )

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS, trust_env=False) as client:
            response = await client.post(
                f"{AGGREGATOR_URL}/readings",
                json=reading.model_dump(mode="json"),
            )
            response.raise_for_status()
            aggregator_payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        append_event({"type": "reading_send_error", "error": str(exc)}, severity="error")
        raise HTTPException(
            status_code=503,
            detail=f"No se pudo enviar la lectura al agregador: {exc}",
        ) from exc

    return {
        "reading": reading.model_dump(mode="json"),
        "aggregator": aggregator_payload,
    }


async def auto_emit_loop() -> None:
    """Genera lecturas periódicas mientras el modo automático permanezca activo."""
    while True:
        # Se impone una espera mínima para impedir un bucle intensivo si la variable
        # de entorno se configura accidentalmente con un valor demasiado pequeño.
        await asyncio.sleep(max(AUTO_EMIT_INTERVAL_SECONDS, 0.1))
        if AUTO_EMIT_INTERVAL_SECONDS <= 0 or not state.enabled or state.failed:
            continue
        try:
            await send_reading(random_auto_request())
        except Exception as exc:  # pragma: no cover - solo logging operacional
            append_event({"type": "auto_emit_error", "error": str(exc)}, severity="error")


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Gestiona el arranque y apagado del sensor mediante la API vigente de FastAPI."""
    global auto_task
    append_event({
        "type": "service_started",
        "zone": SENSOR_ZONE,
        "enabled": state.enabled,
        "auto_emit_interval_seconds": AUTO_EMIT_INTERVAL_SECONDS,
        "auto_anomaly_probability": AUTO_ANOMALY_PROBABILITY,
    })
    if AUTO_EMIT_INTERVAL_SECONDS > 0:
        auto_task = asyncio.create_task(auto_emit_loop())
    try:
        yield
    finally:
        append_event({"type": "service_stopped"})
        if auto_task:
            auto_task.cancel()
            try:
                await auto_task
            except asyncio.CancelledError:
                pass
            auto_task = None


app = FastAPI(title=f"Sensor ficticio {SENSOR_ID}", version="1.2.0", lifespan=lifespan)


from sensor_service.qos_signal import Signal
qos_signal = Signal()

@app.get("/health")
async def health() -> dict[str, object]:
    """Devuelve el estado de salud del componente para comprobaciones externas."""
    fail_if_simulated()
    return {
        "status": "ok",
        "service": SENSOR_ID,
        "zone": SENSOR_ZONE,
        "enabled": state.enabled,
        "aggregator_url": AGGREGATOR_URL,
    }


@app.get("/metrics")
async def metrics() -> dict[str, Any]:
    """Devuelve las métricas operativas expuestas por el componente."""
    recover_if_needed()
    return qos_signal.decorate({
        "status": "failed" if state.failed else "ok",
        "mode": "CPU",
        "enabled": state.enabled,
        "execution_time": state.execution_time,
        "reliability": state.reliability,
        "availability": state.availability,
        "cost": state.cost,
        "response_time_ms": state.response_time_ms,
        "sequence": sequence,
    })


@app.post("/admin/state")
async def set_state(request: SensorStateRequest) -> dict[str, Any]:
    """Activa o desactiva la emisión del sensor mediante una petición administrativa."""
    previous = state.enabled
    state.enabled = request.enabled
    if request.reason:
        state.reason = request.reason
    append_event({
        "type": "sensor_state_changed",
        "previous_enabled": previous,
        "enabled": state.enabled,
        "reason": request.reason,
    })
    return {"status": "updated", "state": state.model_dump()}


@app.post("/admin/simulate")
async def simulate(request: SimulationRequest) -> dict[str, Any]:
    """Activa una simulación temporal con los parámetros recibidos."""
    if request.failed is not None:
        state.failed = request.failed
    if request.execution_time is not None:
        state.execution_time = request.execution_time
    if request.reliability is not None:
        state.reliability = request.reliability
    if request.availability is not None:
        state.availability = request.availability
    if request.cost is not None:
        state.cost = request.cost
    if request.response_time_ms is not None:
        state.response_time_ms = request.response_time_ms
    if request.reason is not None:
        state.reason = request.reason
    if request.recover_after_seconds is not None:
        state.recover_at = time.time() + max(0.0, float(request.recover_after_seconds))
    append_event({"type": "simulation_updated", "simulation": state.model_dump()})
    return {"status": "updated", "simulation": state.model_dump()}


@app.post("/admin/relaunch")
async def relaunch() -> dict[str, Any]:
    """Simula relanzar el servicio: limpia únicamente el fallo de disponibilidad.

    Las métricas de coste y tiempo de respuesta no se resetean para que el
    monitor pueda distinguir entre una caída puntual y un servicio que sigue
    siendo ineficiente tras relanzarlo.
    """
    state.failed = False
    state.execution_time = DEFAULT_SIMULATION_VALUES["execution_time"]
    state.reliability = DEFAULT_SIMULATION_VALUES["reliability"]
    state.availability = DEFAULT_SIMULATION_VALUES["availability"]
    state.cost = DEFAULT_SIMULATION_VALUES["cost"]
    state.response_time_ms = DEFAULT_SIMULATION_VALUES["response_time_ms"]
    state.recover_at = None
    state.reason = "Servicio relanzado por el monitor."
    append_event({"type": "service_relaunched", "state": state.model_dump()})
    return {"status": "relaunched", "state": state.model_dump()}


@app.post("/emit")
async def emit(request: EmitRequest) -> dict[str, object]:
    """Genera una lectura y la envía al servicio agregador configurado."""
    return await send_reading(request)
