'use strict'

const { once } = require('events')
const pino = require('pino')
const IER = require('is-elasticsearch-running')
const elastic = require('../').default
const tap = require('tap')
const test = require('tap').test
const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const EcsFormat = require('@elastic/ecs-pino-format')

const index = 'pinotest'
const streamIndex = 'logs-pino-test'
const type = 'log'
const consistency = 'one'
const node = 'http://localhost:9200'
const timeout = 5000

tap.teardown(() => {
  client.close()
})

let esVersion = 7
let esMinor = 15
let es

const supportsDatastreams =
  () => esVersion > 7 || (esVersion === 7 && esMinor >= 9)

tap.beforeEach(async () => {
  if (es) {
    es = IER()
    if (!await es.isRunning()) {
      await es.waitCluster()
    }
  }
  const result = await client.info()
  esVersion = Number(result.body.version.number.split('.')[0])
  esMinor = Number(result.body.version.number.split('.')[1])
  await client.indices.delete({ index }, { ignore: [404] })
  await client.indices.create({ index })

  if (supportsDatastreams()) {
    await client.indices.deleteDataStream({ name: streamIndex }, { ignore: [404] })
    await client.indices.createDataStream({ name: streamIndex })
  }
})

test('store a log line', { timeout }, async (t) => {
  t.plan(2)

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  log.info('hello world')

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 1)
  const documents = await client.helpers.search({
    index,
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  t.equal(documents[0].msg, 'hello world')
})

test('Ignores a boolean line even though it is JSON-parseable', { timeout }, (t) => {
  t.plan(2)

  const instance = elastic({ index, type, consistency, node })

  instance.on('unknown', (obj, body) => {
    t.equal(obj, 'true', 'Object is parsed')
    t.equal(body, 'Boolean value ignored', 'Message is emitted')
  })

  instance.write('true\n')
})

test('Ignores "null" being parsed as json', { timeout }, (t) => {
  t.plan(2)

  const instance = elastic({ index, type, consistency, node })

  instance.on('unknown', (obj, body) => {
    t.equal(obj, 'null', 'Object is parsed')
    t.equal(body, 'Null value ignored', 'Message is emitted')
  })

  instance.write('null\n')
})

test('Can process number being parsed as json', { timeout }, (t) => {
  t.plan(0)

  const instance = elastic({ index, type, consistency, node })

  instance.on('unknown', (obj, body) => {
    t.error(obj, body)
  })

  instance.write('12\n')
})

test('store an deeply nested log line', { timeout }, async (t) => {
  t.plan(2)

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  log.info({
    deeply: {
      nested: {
        hello: 'world'
      }
    }
  })

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 1)
  const documents = await client.helpers.search({
    index,
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  t.equal(documents[0].deeply.nested.hello, 'world', 'obj gets linearized')
})

test('store lines in bulk', { timeout }, async (t) => {
  t.plan(6)

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  log.info('hello world')
  log.info('hello world')
  log.info('hello world')
  log.info('hello world')
  log.info('hello world')

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 5)
  const documents = await client.helpers.search({
    index,
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  for (const doc of documents) {
    t.equal(doc.msg, 'hello world')
  }
})

test('replaces date in index', { timeout }, async (t) => {
  t.plan(2)
  const index = 'pinotest-%{DATE}'

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  await client.indices.delete(
    { index: index.replace('%{DATE}', new Date().toISOString().substring(0, 10)) },
    { ignore: [404] }
  )

  log.info('hello world')
  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 1)
  const documents = await client.helpers.search({
    index: index.replace('%{DATE}', new Date().toISOString().substring(0, 10)),
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  t.equal(documents[0].msg, 'hello world')
})

test('replaces date in index during bulk insert', { timeout }, async (t) => {
  t.plan(6)

  const index = 'pinotest-%{DATE}'
  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  await client.indices.delete(
    { index: index.replace('%{DATE}', new Date().toISOString().substring(0, 10)) },
    { ignore: [404] }
  )

  log.info('hello world')
  log.info('hello world')
  log.info('hello world')
  log.info('hello world')
  log.info('hello world')

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 5)
  const documents = await client.helpers.search({
    index: index.replace('%{DATE}', new Date().toISOString().substring(0, 10)),
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  for (const doc of documents) {
    t.equal(doc.msg, 'hello world')
  }
})

test('Use ecs format', { timeout }, async (t) => {
  t.plan(2)

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const ecsFormat = EcsFormat()
  const log = pino({ ...ecsFormat }, instance)

  log.info('hello world')

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 1)
  const documents = await client.helpers.search({
    index,
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  t.type(documents[0]['@timestamp'], 'string')
})

test('dynamic index name', { timeout }, async (t) => {
  t.plan(4)

  let indexNameGenerated
  const index = function (time) {
    t.match(time, new Date().toISOString().substring(0, 10))
    indexNameGenerated = `dynamic-index-${process.pid}`
    return indexNameGenerated
  }

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  log.info('hello world')

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 1)
  const documents = await client.helpers.search({
    index: indexNameGenerated,
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  t.equal(documents[0].msg, 'hello world')
})

test('dynamic index name during bulk insert', { timeout }, async (t) => {
  t.plan(12)

  let indexNameGenerated
  const index = function (time) {
    t.match(time, new Date().toISOString().substring(0, 10))
    indexNameGenerated = `dynamic-index-${process.pid + 1}`
    return indexNameGenerated
  }

  const instance = elastic({ index, type, consistency, node, 'es-version': esVersion })
  const log = pino(instance)

  log.info('hello world')
  log.info('hello world')
  log.info('hello world')
  log.info('hello world')
  log.info('hello world')

  setImmediate(() => instance.end())

  const [stats] = await once(instance, 'insert')
  t.equal(stats.total, 5)
  const documents = await client.helpers.search({
    index: indexNameGenerated,
    type: esVersion >= 7 ? undefined : type,
    body: {
      query: { match_all: {} }
    }
  })
  for (const doc of documents) {
    t.equal(doc.msg, 'hello world')
  }
})

test('handle datastreams during bulk insert', { timeout }, async (t) => {
  if (supportsDatastreams()) {
    // Arrange
    t.plan(6)

    const instance = elastic({ index: streamIndex, type, consistency, node, 'es-version': esVersion, op_type: 'create' })
    const log = pino(instance)

    // Act
    const logEntries = [
      { time: '2021-09-01T01:01:01.732Z' },
      { time: '2021-09-01T01:01:02.400Z' },
      { time: '2021-09-01T01:01:02.948Z' },
      { time: '2021-09-02T01:01:03.731Z' },
      { time: '2021-09-02T03:00:45.704Z' }
    ]

    logEntries.forEach(x => log.info(x, 'Hello world!'))

    setImmediate(() => instance.end())

    // Assert
    const [stats] = await once(instance, 'insert')
    t.equal(stats.successful, 5)

    const documents = await client.helpers.search({
      index: streamIndex,
      type: esVersion >= 7 ? undefined : type,
      body: {
        query: { match_all: {} }
      }
    })

    for (let i = 0; i < documents.length; i++) {
      t.equal(documents[i]['@timestamp'], logEntries[i].time)
    }
  } else {
    t.comment('The current elasticsearch version does not support datastreams yet!')
  }
  t.end()
})
