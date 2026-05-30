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
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "qwen2.5:32b-instruct-q2_K")
REQUEST_QUEUE = "llm_requests"
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

llm_request_duration = Histogram(
    "llm_broker_request_duration_seconds",
    "Ollama LLM request duration in seconds",
    ["model"],
)

llm_requests_total = Counter(
    "llm_broker_requests_total",
    "Total LLM requests processed",
    ["model", "status"],  # status: success | error
)

llm_queue_messages_total = Counter(
    "llm_broker_queue_messages_total",
    "Total messages consumed from RabbitMQ",
)


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
    async with message.process(requeue=True):
        body = json.loads(message.body)
        prompt = body.get("prompt", "")
        model = body.get("model", DEFAULT_MODEL)
        request_id = body.get("request_id", message.correlation_id)

        llm_queue_messages_total.inc()
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
            })

        reply_to = message.reply_to or "llm_responses"

        async with aio_pika.connect_robust(RABBITMQ_URL) as conn:
            async with conn.channel() as ch:
                await ch.default_exchange.publish(
                    aio_pika.Message(
                        body=response_body.encode(),
                        correlation_id=message.correlation_id,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=reply_to,
                )


async def main() -> None:
    log.info("starting metrics server on port %d", METRICS_PORT)
    Thread(target=start_http_server, args=(METRICS_PORT,), daemon=True).start()

    log.info("connecting to RabbitMQ at %s", RABBITMQ_URL)
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await channel.declare_queue(REQUEST_QUEUE, durable=True)
        log.info("consuming from queue: %s", REQUEST_QUEUE)

        await queue.consume(on_request)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())