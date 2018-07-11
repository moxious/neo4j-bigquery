#  Neo4j BigQuery Importer

This code exports all data from a neo4j instance and writes it to Google's BigQuery service.

## Prerequisites

- You must have `gcloud` CLI utilities installed and be authenticated against them.
- You must have access to a running neo4j instance.

## Usage

```
npm install
export NEO4J_URI=bolt://my-endpoint
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=secret

node index.js
```

## Configuration

See the settings in `index.js` until more docs are written.