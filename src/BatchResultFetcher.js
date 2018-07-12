const utils = require('./cypher-common');
const log = require('./log');

/**
 * Class which fetches batches at a time from a given cypher query.
 * 
 * Example, you want to pull a lot of nodes of type :Person.
 * 
 * name='PersonFetcher'
 * driver=(myneo4jdriver)
 * countQuery="MATCH (n:Person) RETURN count(n) as count" (must expose a "count" attribute)
 * batchQuery="MATCH (n:Person) RETURN n ORDER BY id(n) ASC SKIP $skipNumber LIMIT $batchSize"
 * 
 * (In the above, $skipNumber and $batchSize will be replaced as needed)
 * 
 * batchPostProcessFn=(records)=>records.map(rec => rec.get('n'))
 * 
 * This indicates to the fetcher to pull out the node labeled 'n', since it does not parse
 * your batch query.
 */
module.exports = class BatchResultFetcher {
    /**
     * @param {string} name 
     * @param {*} driver a neo4j driver
     * @param {string} countQuery a query which returns target count
     * @param {string} batchQuery a batch query, requiring params skipNumber and batchSize
     * @param {Function} batchPostProcessFn a function to post-process a result before it is returned
     * @param {Number} batchSize a batch size.
     */
    constructor(name, driver, countQuery, batchQuery, batchPostProcessFn, batchSize=10000) {
        this.name = name;
        this.driver = driver;
        this.countQuery = countQuery;
        this.batchQuery = batchQuery;
        this.batchSize = batchSize;
        this.batchTracking = {};
        this.batchPostProcessFn = batchPostProcessFn;
        this.session = driver.session();
        this.batchIndex = 0;
        this.count = undefined;
        this.batchesComplete = 0;
    }

    /**
     * Runs the count query and returns a number corresponding to how many items there
     * are.
     */
    getCount() {
        log.info(this.name, 'counting');
        if (this.count) { return Promise.resolve(this.count); }

        return this.session.run(this.countQuery)
            .then(result => {
                const count = result.records[0].get('count').toNumber();                
                this.count = count;
                this.batches = [...utils.range(0, count, this.batchSize)];
                log.info(this.name, count, 'total to fetch, which will be divided into ', this.batches.length, 'batches');
                return count;
            });
    }

    countBatches() {
        if (!this.count) { throw new Error('You must count items first'); }
        return this.batches.length;
    }

    currentBatch() { return this.batchIndex; }

    hasMore() { return this.batchIndex < this.countBatches(); }

    summary() {
        return `${this.currentBatch()} current batch of ${this.countBatches()} and ${this.batchesComplete} complete`;
    }

    /**
     * Fetches the next batch.
     * @returns {Promise} that resolves to more records or null when exhausted.
     */
    nextBatch() {
        // Ensure count is always present before doing first batch.        
        const promiseHead = this.count ? Promise.resolve(this.count) : this.getCount();

        const bi = this.batchIndex;

        return promiseHead.then(() => {
            if (this.batchIndex >= this.batches.length) {
                // Finished.
                return null;
            }

            // Get this batch with the given SKIP parameter.
            const skipNumber = this.batches[this.batchIndex];

            log.info('FETCH ',this.name, 'batch', this.batchIndex, 'of', this.batches.length);
            // Increment to get next batch on subsequent call.
            this.batchIndex++;

            return this.session.run(this.batchQuery, {
                batchSize: this.batchSize,
                skipNumber,
            });
        })
        .then(records => records === null ? null : this.batchPostProcessFn(records))
        .then(nodes => nodes === null ? null : utils.extractPropertyContainers(nodes))
        .then(results => {
            this.batchesComplete++;
            // log.info('COMPLETE ', this.name, results.length, 'records in batch', bi, 'of', this.batches.length);
            return results;
        });
    }
}