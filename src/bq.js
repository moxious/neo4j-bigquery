/* Utilities for working with bigquery 
 * Relevant docs: https://github.com/googleapis/nodejs-bigquery
 * And API: https://googlecloudplatform.github.io/google-cloud-node/#/docs/bigquery/latest/bigquery
 */
const BigQuery = require('@google-cloud/bigquery');
const path = require('path');
const fs = require('fs');
const _ = require('lodash');
const log = require('./log');

const DEFAULT_OPTIONS = {
    sourceFormat: 'CSV',
    skipLeadingRows: 1,

    // Neo4j has no firm schema, so missing properties get treated like null
    allowJaggedRows: true,
    allowQuotedNewlines: true,

    writeDisposition: 'WRITE_APPEND',

    // Auto-create table if needed, so we don't have to check ahead of time
    // if it exists.
    createDisposition: 'CREATE_IF_NEEDED',
};

// Your Google Cloud Platform project ID
let projectId = process.env.PROJECT || 'testbed-187316';

/**
 * Get a configured instance of a bigquery object, pulling from environment
 * as appropriate.
 */
const getInstance = () => {
    const bigquery = new BigQuery({
        projectId: projectId,
    });

    return bigquery;
};

const tableExists = (datasetId, table) => {
    const bigquery = getInstance();

    // Lists all tables in the dataset
    return bigquery
        .dataset(datasetId)
        .getTables()
        .then(results => {
            const tables = results[0];
            const matches = tables.filter(t => t.id === table);
            return matches.length > 0;
        });
};

class TableManager {
    constructor() {
        this.tables = {};
        this.pending = {};
    }

    getOrCreateTable(projectId, datasetId, table) {
        const key = `${projectId}/${datasetId}/${table}`;

        if (this.tables[key]) {
            return Promise.resolve(this.tables[key]);
        }

        if (this.pending[key]) {
            return this.pending[key];
        }

        // Google's create table API isn't idempotent.  LAME.  :)
        const prom = getInstance()
            .dataset(datasetId)
            .createTable(table, { autodetect: true })
            .then(results => {
                table = results[0];
                this.tables[key] = table;
                return table;
            });

        this.pending[key] = prom;
        return prom;
    }
}

const tableManager = new TableManager();

/**
 * Append data from a CSV file to a bigquery table.
 * @param {*} file path to CSV file.
 * @param {*} options (optional) bigquery load params.
 */
const appendBigqueryTable = (datasetId, file, tableId, first=false, options=DEFAULT_OPTIONS) => {  
    let start = new Date().getTime();
    let table;

    // Schema autodetect is only available on first load, so we have to know
    // whether that's happening or not.
    const loadOptions = _.cloneDeep(options);
    if (first) {
        loadOptions.autodetect = true;
    }

    // log.info('appendBigqueryTable ', file, '->', tableId, 'first', first);
    // Streaming insert rather than load would save a lot of IO, but is not as good
    // of an option because we must know ahead of time what the schema is, auto-detection
    // isn't available.  So doing it this way allows us to use Google's schema auto-detection.
    return tableManager.getOrCreateTable(projectId, datasetId, tableId)
        .then(tbl => {
            table = tbl;
            return table.load(file, loadOptions);
        })
        .then(results => {
            const job = results[0];
            const end = new Date().getTime();

            // load() waits for the job to finish
            log.info(`BQ: ${job.status.state} Load Job ${job.id}`, file, '->', table.id, (end - start), 'ms');
      
            // Check the job's status for errors
            const errors = job.status.errors;
            if (errors && errors.length > 0) {
              throw errors;
            }

            return table;
        })
        .catch(err => {
            log.error(`BQ: Load Job FAILED`, file, '->', tableId);
            log.error(err);
            return table;
        })
};

module.exports = {
    appendBigqueryTable,
    tableExists,
    getInstance,
    projectId,
};