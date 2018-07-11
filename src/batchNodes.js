const cypher = require('./cypher-common');

const batches = {};

const labelForCombo = combo => ':' + combo.join(':');

const countLabelCombo = () =>
    session.run(`
        MATCH (n:\`${l1}\`)-[r:\`${relCombo.relType}\`]-(m:\`${l2}\`)
        WHERE ${nWhere} AND ${mWhere}
        RETURN count(r) as rels
    `)
        .then(result => {
            console.log('RELCOMBO', relCombo, 'count', result.records[0].get('rels').toNumber());
            return result.records[0].get('rels').toNumber();
        });

const labelComboBatch = () => {


}

module.exports = {

};