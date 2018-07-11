const PromisePool = require('es6-promise-pool');

/**
 * Say you want to map a function across an array. The function produces promises.
 * And you want to put those promises into a promise pool, to guarantee that there
 * is always some simulatneous concurrency.  This object does that.
 * 
 * In comparison to using bluebird Promise.map (which does things a batch at a time)
 * this will keep the promise pool full, giving better performance.  For uploading
 * to bigquery, while some threads are uploading, others are pulling from neo4j,
 * and so on.
 * 
 * This is just a wrapper on es6-promise-pool to make it more convenient for mapping.
 * 
 * We have to hand-accumulate results from the map because the pool doesn't resolve
 * to the results.  Results may occur out of order.
 */
module.exports = class MappedPromisePool {
    constructor(name, mapArray, mapFunction, concurrency) {
        this.name = name;
        this.mapArray = mapArray;
        this.mapFunction = mapFunction;
        this.concurrency = concurrency;

        let i = 0;
        const promiseProducer = () => {
            const nthItem = mapArray[i++];

            if (nthItem) {
                const p = i;
                console.log('START Pool ', this.name, ' function ', p, 'of', mapArray.length);
                return mapFunction(nthItem)
                    .then(result => {
                        console.log('FINISH Pool function ', p, 'of', mapArray.length);
                        return result;
                    });
            }
            return null;
        };

        this.results = [];
        this.pool = new PromisePool(promiseProducer, concurrency);

        // Accumulate results as we go, may be out of order.
        this.pool.addEventListener('fulfilled', event => {
            // The event contains:
            // - target:    the PromisePool itself
            // - data:
            //   - promise: the Promise that got fulfilled
            //   - result:  the result of that Promise
            this.results.push(event.data.result);            
        })
    }    

    start() { 
        return this.pool.start().then(() => this.results);
    }
    getPool() { return this.pool; }
};