const bunyan = require('bunyan');

const logger = () =>
    bunyan.createLogger({ name: 'neo4j-bigquery' });

module.exports = logger();
