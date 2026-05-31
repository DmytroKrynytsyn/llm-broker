# llm-broker

Consumes LLM requests from RabbitMQ, routes them to Ollama, publishes responses back.
git push → GitHub Actions → Docker Hub → ArgoCD → k3s

## Flow

Any app publishes to `llm_requests`, llm-broker calls Ollama, responds to `reply_to` queue.

## Request message

```json
{
  "prompt": "summarize this...",
  "request_id": "uuid"
}
```

## Response message

```json
{
  "result": "...",
  "request_id": "uuid",
  "duration_seconds": 12.4,
  "error": null
}
```

## Metrics (port 8000)

| Metric | Type | Description |
|---|---|---|
| `llm_broker_requests_total` | Counter | Requests by model and status (success/error) |
| `llm_broker_request_duration_seconds` | Histogram | Ollama latency by model |
| `llm_broker_queue_messages_total` | Counter | Total messages consumed from RabbitMQ |

## Bootstrap

```bash
kubectl apply -f https://raw.githubusercontent.com/DmytroKrynytsyn/llm-broker/main/argocd/application.yaml
```

## Secrets

| Secret | Value |
|---|---|
| `DOCKERHUB_USERNAME` | `dkrinitsyn` |
| `DOCKERHUB_TOKEN` | Docker Hub access token |