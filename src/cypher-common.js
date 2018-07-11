const _ = require('lodash');
const neo4j = require('neo4j-driver').v1;
const neo4jSpatial = require('neo4j-driver/lib/v1/spatial-types');

/**
 * @param {Array} arr array of string labels
 * @param {string} cypherVarName
 * @returns string WHERE clause joined with AND, to require that cypherVarName
 * has all of those labels.
 */
const whereCypherForLabelArray = (arr, cypherVarName = 'node') =>
    arr.length > 0 ? arr.map(label => `${cypherVarName}:\`${label}\``).join(' AND ') : '1=1';

function* range(start, stop, step = 1) {
    if (stop === undefined) [start, stop] = [0, start];
    if (step > 0) while (start < stop) yield start, start += step;
    else if (step < 0) while (start > stop) yield start, start += step;
    else throw new RangeError('range() step argument invalid');
}

const extractPropertyContainers = propContainers => {
    const records = propContainers.map(pc => {
        const props = cleanProperties(pc.properties);
        return _.merge({ __id: pc.identity.toString() }, props);
    });

    return records;
};

const cleanProperties = props => {
    Object.keys(props).forEach(key => {
        const val = props[key];
        if (neo4j.isInt(val)) { props[key] = val.toString(); }
        else if(neo4jSpatial.isPoint(val)) {
            // Lacking a better approach right now...
            props[key] = val.toString();
        }
    });

    return props;
};

module.exports = {
    whereCypherForLabelArray,
    range,
    extractPropertyContainers,
    cleanProperties,
};