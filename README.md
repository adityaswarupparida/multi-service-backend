# Backend Services

A backend system with three components:
- **API** — CSV upload + fetch records (Express)
- **Consumer** — Kafka consumer that updates Redis cache
- **Shared** — Common DB, cache, and Kafka clients (`@backend/common`)

## Architecture

```
Upload CSV → API → PostgreSQL (insert)
                 → Kafka (publish event)
                        ↓
                   Consumer → PostgreSQL (read all) → Redis Cache (update)

Fetch Records → API → Redis Cache
                        │ (miss)
                        └──→ PostgreSQL (fallback)
```

## Prerequisites

- [Node.js](https://nodejs.org/) v22+
- [pnpm](https://pnpm.io/) v10+
- [Docker](https://www.docker.com/) + Docker Compose

## Getting Started

### 1. Clone and install dependencies

```bash
pnpm install
```

### 2. Set up environment variables

```bash
cp .env.example .env
cp api/.env.example api/.env
cp consumer/.env.example consumer/.env
```

Fill in the values in each `.env` file.

### 3. Start infrastructure

```bash
docker-compose up -d
```

This starts PostgreSQL (5432), Redis (6379), Kafka (9092), and Zookeeper.

### 4. Run database migrations and generate Prisma client

```bash
cd packages/src/db
npx prisma migrate dev
npx prisma generate
```

### 5. Build shared package

```bash
cd packages
pnpm build
```

### 6. Start the API

```bash
cd api
pnpm run dev
```

API runs at `http://localhost:3000`.

### 7. Start the consumer (separate terminal)

```bash
cd consumer
pnpm run dev
```

## API Endpoints

### Upload CSV

```bash
curl -X POST http://localhost:3000/api/records/upload \
  -H "Content-Type: text/csv" \
  -H "x-filename: data.csv" \
  --data-binary @data.csv
```

**Response:**
```json
{ "message": "Uploaded successfully!!" }
```

### Fetch Records

```bash
curl http://localhost:3000/api/records
```

**Response:**
```json
{ "data": [...] }
```

## Running Tests

```bash
# API tests
cd api && pnpm test

# Consumer tests
cd consumer && pnpm test

# With coverage
cd api && pnpm test:coverage
cd consumer && pnpm test:coverage
```

> Tests require all Docker services to be running.

### Coverage

**API**
| File | Stmts | Branch | Funcs | Lines |
|------|-------|--------|-------|-------|
| records.ts | 95.91% | 90.9% | 100% | 95.91% |

**Consumer**
| File | Stmts | Branch | Funcs | Lines |
|------|-------|--------|-------|-------|
| index.ts | 96% | 81.25% | 100% | 96% |


## Useful Commands

### Reset database

```bash
docker exec -it postgres psql -U postgres -d backend -c 'TRUNCATE "Record", "Upload" RESTART IDENTITY CASCADE;'
```

### Reset Redis cache

```bash
docker exec -it redis redis-cli DEL records:all
```

### Stop all services

```bash
docker-compose down
```

### Stop and remove volumes (full reset)

```bash
docker-compose down -v
```
