"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const tslib_1 = require("tslib");
const elasticsearch_1 = require("@elastic/elasticsearch");
const split2_1 = tslib_1.__importDefault(require("split2"));
class DropDocumentError extends Error {
}
function pinoElasticSearch(opts) {
    const splitter = (0, split2_1.default)(function (line) {
        let value;
        const setDateTimeString = (value) => {
            if (typeof value === 'object' &&
                Object.prototype.hasOwnProperty.call(value, 'time')) {
                if ((typeof value.time === 'string' && value.time.length) ||
                    (typeof value.time === 'number' && value.time >= 0)) {
                    return new Date(value.time).toISOString();
                }
            }
            return new Date().toISOString();
        };
        try {
            value = JSON.parse(line);
        }
        catch (error) {
            this.emit('unknown', line, error);
            return;
        }
        if (typeof value === 'boolean') {
            // eslint-disable-next-line no-invalid-this
            this.emit('unknown', line, 'Boolean value ignored');
            return;
        }
        if (value === null) {
            this.emit('unknown', line, 'Null value ignored');
            return;
        }
        if (typeof value !== 'object') {
            value = {
                data: value,
                time: setDateTimeString(value),
            };
        }
        else if (value['@timestamp'] === undefined) {
            value.time = setDateTimeString(value);
        }
        return value;
    }, { autoDestroy: true });
    const client = new elasticsearch_1.Client({
        node: opts.node,
        auth: opts.auth,
        cloud: opts.cloud,
        ...(opts.Connection && { Connection: opts.Connection }),
    });
    const esVersion = Number(opts['es-version']) || 7;
    const index = opts.index || 'pino';
    const type = esVersion >= 7 ? undefined : opts.type || 'log';
    const opType = esVersion >= 7 ? opts.op_type : undefined;
    const getIndexName = (time = new Date().toISOString()) => {
        if (typeof index === 'function') {
            return index(time);
        }
        return index.replace('%{DATE}', time.substring(0, 10));
    };
    const b = client.helpers.bulk({
        datasource: splitter,
        flushBytes: opts['flush-bytes'] || 1000,
        flushInterval: opts['flush-interval'] || 30000,
        refreshOnCompletion: getIndexName(),
        onDocument(doc) {
            const date = doc.time || doc['@timestamp'];
            if (opType === 'create') {
                doc['@timestamp'] = date;
            }
            return {
                index: {
                    _index: getIndexName(date),
                    _type: type,
                    op_type: opType,
                },
            };
        },
        onDrop(doc) {
            const error = new DropDocumentError('Dropped document');
            error.document = doc;
            splitter.emit('insertError', error);
        },
    });
    b.then((stats) => splitter.emit('insert', stats), (err) => splitter.emit('error', err));
    splitter._destroy = function (err, cb) {
        b.then(() => cb(err), (e2) => cb(e2 || err));
    };
    return splitter;
}
module.exports =  pinoElasticSearch;