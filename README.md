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

## How this works

### Exporting Nodes

We fetch a list of every possible node label combination, and make each combination into a table.  We do the same for each relationship combination.

So for example if you have a graph that looks like `(:Person)-[:FRIENDS]->(:Person)`, then
you will end up with two BigQuery tables:
- `Person`
- `Person_FRIENDS_Person`

Data is exported for each label combination and not just for each label individually, because labels can overlap.  This ensures that each node is exported only once.  So for example if you have many `:Person` nodes, with some labeled `:Friend` and others `:Enemy`, you will end up with two BigQuery tables:
- `Person_Friend`
- `Person_Enemy`

This general export structure is referred to as "table for labels"

### Exporting Relationships

Relationship table names are generated as `FROMLABEL_RELTYPE_TOLABEL`.  The table will contain
whatever properties are on the relationships in neo4j, but also a `__from` and `__to` attribute
to use as a foreign key, which links to the `__id` field in any node table.  

## Limitations

BigQuery is by nature bound to a strict schema, and Neo4j isn't.  This means that certain types of data in Neo4j are going to fundamentally cause problems with BigQuery, for example if a given node label has a different set of properties for each instance.  Expect to see BigQuery load errors if your data is very chaotic and doesn't have a consistent set of properties per node label.

Additionally, right now unlabeled nodes are not exported or supported.

## Configuration

See the settings in `index.js` until more docs are written.