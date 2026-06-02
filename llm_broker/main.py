import asyncio
import json
import logging
import os
import time
from threading import Thread

import aio_pika
import httpx
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq.rabbitmq.svc.cluster.local/")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://kbrain2:11434")
REQUEST_QUEUE = "llm_requests"
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "7200"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "14400"))

llm_request_duration = Histogram(
    "llm_broker_request_duration_seconds",
    "Ollama LLM request duration in seconds",
    ["model"],
)

llm_requests_total = Counter(
    "llm_broker_requests_total",
    "Total LLM requests processed",
    ["model", "status"],
)

llm_queue_messages_total = Counter(
    "llm_broker_queue_messages_total",
    "Total messages consumed from RabbitMQ",
)

# known broker fields — everything else is passed through to the response
BROKER_FIELDS = {"prompt", "request_id"}

connection: aio_pika.RobustConnection = None
ollama_model: str = ""


async def get_publish_channel() -> aio_pika.Channel:
    return await connection.channel()


async def call_ollama(prompt: str, model: str) -> tuple[str, float]:
    start = time.monotonic()
    async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
        response = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
        )
        response.raise_for_status()
        result = response.json()["response"]
    duration = time.monotonic() - start
    return result, duration


async def on_request(message: aio_pika.IncomingMessage) -> None:
    body = json.loads(message.body)
    prompt = body.get("prompt", "")
    model = ollama_model
    request_id = body.get("request_id", message.correlation_id)
    reply_to = message.reply_to or "llm_responses"

    # passthrough fields — anything the caller added beyond broker fields
    passthrough = {k: v for k, v in body.items() if k not in BROKER_FIELDS}

    llm_queue_messages_total.inc()
    await message.ack()
    log.info("request_id=%s model=%s prompt_len=%d", request_id, model, len(prompt))

    try:
        with llm_request_duration.labels(model=model).time():
            result, duration = await call_ollama(prompt, model)

        llm_requests_total.labels(model=model, status="success").inc()
        log.info("request_id=%s model=%s done in %.1fs", request_id, model, duration)

        response_body = json.dumps({
            "result": result,
            "request_id": request_id,
            "model_used": model,
            "duration_seconds": round(duration, 2),
            "error": None,
            **passthrough,
        })

    except Exception as e:
        llm_requests_total.labels(model=model, status="error").inc()
        log.error("request_id=%s model=%s error=%s", request_id, model, e)
        response_body = json.dumps({
            "result": None,
            "request_id": request_id,
            "model_used": model,
            "duration_seconds": None,
            "error": str(e),
            **passthrough,
        })

    try:
        ch = await get_publish_channel()
        async with ch:
            await ch.default_exchange.publish(
                aio_pika.Message(
                    body=response_body.encode(),
                    correlation_id=message.correlation_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=reply_to,
            )
    except Exception as e:
        log.error("request_id=%s publish_error=%s", request_id, e)


async def main() -> None:
    global connection, ollama_model

    log.info("starting metrics server on port %d", METRICS_PORT)
    Thread(target=start_http_server, args=(METRICS_PORT,), daemon=True).start()

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{OLLAMA_URL}/api/tags")
        resp.raise_for_status()
        ollama_model = resp.json()["models"][0]["name"]
    log.info("detected ollama model: %s", ollama_model)

    log.info("connecting to RabbitMQ at %s", RABBITMQ_URL)
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        consume_channel = await connection.channel()
        await consume_channel.set_qos(prefetch_count=1)

        queue = await consume_channel.declare_queue(REQUEST_QUEUE, durable=True)
        log.info("consuming from queue: %s", REQUEST_QUEUE)

        await queue.consume(on_request)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())