/**
 * Import neo4j -> BigQuery.
 * See the comments above the main() function for details on flow.
 */
const neo4j = require('neo4j-driver').v1;
const _ = require('lodash');
const Promise = require('bluebird');
const PromisePool = require('es6-promise-pool');
const fs = require('fs');
const yargs = require('yargs');
const log = require('./src/log');

const CSVFile = require('./src/CSVFile');
const bq = require('./src/bq');
const BatchResultFetcher = require('./src/BatchResultFetcher');
const cypherCommon = require('./src/cypher-common');

/**
 * Adjustable configuration that drives everything.
 */
const config = {
    concurrency: Number.isNaN(process.env.CONCURRENCY) ? Number(process.env.CONCURRENCY) : 3,
    neo4jURI: process.env.NEO4J_URI || 'bolt://localhost:7687',
    neo4jUser: process.env.NEO4J_USERNAME || 'neo4j',
    neo4jPass: process.env.NEO4J_PASSWORD || 'admin',
    writeDir: '/tmp/csv',
    datasetName: 'neo4j_export_' + Math.floor(Math.random() * 999),
    datasetId: undefined, /* Captured on create */
};

const driver = neo4j.driver(config.neo4jURI,
    neo4j.auth.basic(config.neo4jUser, config.neo4jPass), { maxTransactionRetryTime: 30000 });

const session = driver.session();

const allLabelCombos = () => {
    log.info('Fetching all label combinations');
    return session.run("MATCH (n) RETURN distinct(labels(n)) as labels")
        .then(result => result.records.map(rec => rec.get('labels')));
};

const allRelCombos = () => {
    log.info('Fetching all label/relationship combinations');
    return session.run("MATCH (n)-[r]->(m) return distinct(labels(n)) as from, type(r) as relType, labels(m) as to")
        .then(result => result.records.map(rec => ({
            from: rec.get('from'),
            to: rec.get('to'),
            relType: rec.get('relType'),
        })));
};

const nodeBatches = (labelCombo) => {
    const name = ':' + labelCombo.join(':');

    const label = labelCombo[0];
    const rest = labelCombo.slice(1);

    // If there are other labels this will require their match
    const whereClause = cypherCommon.whereCypherForLabelArray(rest, 'n');

    const countQuery = `
        MATCH (n:\`${label}\`) 
        WHERE ${whereClause}
        RETURN count(n) as count
    `;

    const batchQuery = `
        MATCH (n:\`${label}\`) 
        WHERE ${whereClause}
        RETURN n 
        ORDER BY id(n) ASC 
        SKIP $skipNumber
        LIMIT $batchSize
    `;

    // Fetcher doesn't know what data types come back, so here is where we grab/format.
    const postProcess = result => result.records.map(rec => rec.get('n'));

    const brf = new BatchResultFetcher(name, driver, countQuery, batchQuery, postProcess);
    return brf;
};

const relBatches = (relCombo) => {
    const fromName = ':' + relCombo.from.join(':');
    const toName = ':' + relCombo.to.join(':');

    const name = `(${fromName})-[:\`${relCombo.relType}\`]->(${toName})`;

    const l1 = relCombo.from[0];
    const l1rest = relCombo.from.slice(1);
    const l2 = relCombo.to[0];
    const l2rest = relCombo.to.slice(1);

    const nWhere = cypherCommon.whereCypherForLabelArray(l1rest, 'n');
    const mWhere = cypherCommon.whereCypherForLabelArray(l2rest, 'm');

    const countQuery = `
        MATCH (n:\`${l1}\`)-[r:\`${relCombo.relType}\`]-(m:\`${l2}\`)
        WHERE ${nWhere} AND ${mWhere}
        RETURN count(r) as count
    `;

    const batchQuery = `
        MATCH (n:\`${l1}\`)-[r:\`${relCombo.relType}\`]-(m:\`${l2}\`)
        WHERE ${nWhere} AND ${mWhere}
        RETURN id(n) as from, id(m) as to, r
        ORDER BY id(r) ASC
        SKIP $skipNumber
        LIMIT $batchSize
    `;

    const postProcess = result =>
        result.records.map(rec => {
            const r = rec.get('r');
            // Add from/to ID properties to rel.
            r.properties.__from = rec.get('from');
            r.properties.__to = rec.get('to');
            return r;
        })

    return new BatchResultFetcher(name, driver, countQuery, batchQuery, postProcess);
};

// Generic function that produces promises from a batch result fetcher.
// Writes batches to a file of the given name, and a bigquery table of the 
// given name.  When batches are exhaused, it returns null which terminates
// the promise pool.
const genericBackendPromiseProducer = (batchResultFetcher, table, file) => {
    if (!batchResultFetcher.hasMore()) {
        log.info('Fetcher ', batchResultFetcher.name, 'exhausted; returning null');
        return null;
    }

    return batchResultFetcher.nextBatch()
        .then(neo4jRecords => {
            if (neo4jRecords === null || neo4jRecords === undefined || neo4j.length == 0) {
                log.info('Fetcher ', batchResultFetcher.name, 'exhausted; INNER returning null');
                return null;
            }

            // When writing to bigquery, the first batch is special, 
            // (schema auto-detection) so this is how we tell.
            const isFirstBatch = batchResultFetcher.batchesComplete === 1;

            return CSVFile.writeFileFromRecords(file, neo4jRecords)
                .then(() => bq.appendBigqueryTable(config.datasetId, file, table, isFirstBatch))
                .then(results => {
                    // if (config.deleteFileWhenLoaded) {
                    //     fs.unlink(file, err => {
                    //         if (err) { throw 'Failed to delete file' + err; }
                    //     });
                    // }
                    return results;
                });
        });
};

// A backend consumes results from a batch and writes them to a location
// in this case, BigQuery.  This one is for nodes.
const nodeBackend = (labelCombo, batchResultFetcher) => {
    const conc = 1; // Keep at one unless you're sure you know what you're doing.

    const tableName = labelCombo.length > 0 ? labelCombo.join('_').replace(/[^A-Za-z0-9]/g, '_') : 'UNLABELED';

    let batch = 0;
    const promiseProducer = () => {
        if (!batchResultFetcher.hasMore()) {
            log.info(batchResultFetcher.name, 'producer exhausted', batchResultFetcher.summary());
            return null;
        }

        const file = config.writeDir + `/${labelCombo.join('_')}-batch-${batch++}.csv`;
        // log.info('BACKEND:', labelCombo, 'in', file, 'going to', tableName);
        return genericBackendPromiseProducer(batchResultFetcher, tableName, file);
    };

    // Create table data goes into
    // Produce a pool of such actions with given concurrency.
    return batchResultFetcher.getCount().then(() => new PromisePool(promiseProducer, conc));
};

// A backend consumes results from a batch and writes them to a location
// in this case, BigQuery.  This one is for relationships.
// Returns a promise pool that can be started when desired.
const relBackend = (relCombo, batchResultFetcher) => {
    const conc = 1;  // Keep at one unless you're sure you know what you're doing.

    const tableName = `${relCombo.from.join('_')}-${relCombo.relType}-${relCombo.to.join('_')}`
        .replace(/[^A-Za-z0-9]/g, '_');;

    let batch = 0;
    const promiseProducer = () => {
        if (!batchResultFetcher.hasMore()) {
            log.info(batchResultFetcher.name, 'producer exhausted', batchResultFetcher.summary());
            return null;
        }
        const file = config.writeDir + `/${tableName}-batch-${batch++}.csv`;
        return genericBackendPromiseProducer(batchResultFetcher, tableName, file);
    };

    // Create table data goes into
    // Produce a pool of such actions with given concurrency.
    return batchResultFetcher.getCount().then(() => new PromisePool(promiseProducer, conc));
};

const main = args => {
    log.info('MAIN', args);
    // OVERALL FLOW
    //
    // (SETUP) - fetch a list of every node label combination, and every nodelable
    // relationship type combination.  By fetching combinations rather than individual
    // node labels, we ensure every node is exported only once.
    // 
    // (1) Each node and relcombo needs to be broken down into a set of batches,
    // to accomodate for large data volumes.  (E.g. 1 million :Person:Friend nodes)
    //
    // (2) Each batch (whether nodes or rels) has two components -- the batch
    // result fetcher (front-end) which grabs from neo4j, and the back-end, 
    // which writes the records to BigQuery, and provides for an option to swap 
    // out the back-end later, i.e. to save to a different service.
    // 
    // (3) Each batch combined with a backend forms a promise pool with adjustable
    // concurrency.  This pool streams results from neo4j -> BigQuery.
    //
    // (4) The entire load task is then a big array of promise pools.  Each pool
    // represents a type of data to load, (e.g. a label combination)
    //
    // (5) Concurrency is adjusted in two ways: (a) how many tasks can happen within
    // a promise pool concurrently (currently 1) and (b) how many pools are running
    // at the same time, in the "pool of pools".  This is the main concurrency
    // control.
    const bqConn = bq.getInstance();
    
    return Promise.all([
        allLabelCombos(), 
        allRelCombos(), 
        bqConn.createDataset(config.datasetName)])
        .then(([labelCombos, relCombos, results]) => {
            const dataset = results[0];
            config.datasetId = dataset.id;
            log.info(`BQ Dataset ${bq.projectId}/${dataset.id} created`);

            const nodePromises = Promise.map(labelCombos.filter(lc => lc.length > 0), lc => {
                const batchResultFetcher = nodeBatches(lc);
                return batchResultFetcher.getCount().then(() => nodeBackend(lc, batchResultFetcher));
            }, { concurrency: config.concurrency });

            const relPromises = Promise.map(relCombos.filter(rc => rc.from.length > 0 && rc.to.length > 0),
                rc => {
                    const batchResultFetcher = relBatches(rc);
                    return batchResultFetcher.getCount().then(() => relBackend(rc, batchResultFetcher));
                }, { concurrency: config.concurrency });

            // Two arrays of promises which resolve to arrays of promise pools.
            // The first resolve of the arrays creates BQ tables and does setup.
            // The resulting promise pools are the heavyweight data movement.
            let nPPs;
            let rPPs;

            return Promise.all(nodePromises)
                .then(results => {
                    nPPs = results;
                    return Promise.all(relPromises);
                })
                .then(results => {
                    rPPs = results;
                    return nPPs.concat(rPPs);
                })
                .then(allPromisePools => {
                    let currentPool = 0;

                    // To produce a promise in the overall load process, let its promise resolve to
                    // the pool, and then start the pool.
                    const poolOfPoolsPromiseProducer = () => {
                        const nextPool = allPromisePools[currentPool++];

                        if (nextPool) {
                            return nextPool.start();
                        }

                        return null;
                    };

                    // Main concurrency control is here.
                    const poolOfPools = new PromisePool(poolOfPoolsPromiseProducer, config.concurrency);

                    // When pool of pools is done, entire load complete.
                    return poolOfPools.start();
                });
        })
        .catch(err => log.warn(err))
        .then(() => driver.close())
        .then(() => log.info('Exiting'));
};

main(yargs.argv);