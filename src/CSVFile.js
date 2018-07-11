const csvWriter = require('csv-write-stream');
const fs = require('fs');

module.exports = class CSVFile {
    constructor(filename) {
        this.filename = filename;
        this.writer = csvWriter();
        this.stream = fs.createWriteStream(filename);
        this.writer.pipe(this.stream);
    }

    writeBatch(someRows) {
        someRows.map(row => this.writer.write(row));
    }

    /**
     * Write a batch of records to a CSV file.
     * @param {*} filename filename to write to
     * @param {*} records array of object records
     * @returns {Promise} which resolves to the filename when complete.
     */
    static writeFileFromRecords(filename, records) {
        return new Promise((resolve, reject) => {
            // console.log('Writing CSV', filename);
            const f = new CSVFile(filename);
            f.writeBatch(records);

            // Only resolve when the fd is closed, data is written.
            f.stream.on('close', () => resolve(filename));
            f.writer.end();
        });
    }
};