import { describe, it, mock, beforeEach, afterEach } from 'node:test'
import assert from "node:assert/strict";
import fetchMock from 'fetch-mock';
import path from 'node:path';
import { mkdir, rm, stat } from 'node:fs/promises';
import { Backup } from "../src/backupClass.js";
import folderDescription from './000_folder-description.json' with { type: 'json' };

const ORIGIN = 'https://myapp.a';
const FOLDER_PATH = '/category/folder/';
const PATH1 = FOLDER_PATH + 'document1';
const PATH2 = FOLDER_PATH + 'document2';
const PATH3 = FOLDER_PATH + 'document3';
const ENDPOINT = 'https://a.b/user/';

describe("execute", function() {
  it("should, for backup of all, initially queue only '/'", async function() {
    const backup = new Backup(ORIGIN, {backupDir: '/tmp/bak', category: '', includePublic: true, simultaneous: 2}, ENDPOINT, '0.42');
    mock.method(backup, 'checkFetch', () => {
      return Promise.resolve();
    });

    await backup.execute();

    assert.deepEqual(backup.queue.get('/'), {inFlight: false, failures: 0});
    assert.equal(backup.queue.size, 1);
    assert.equal(backup.checkFetch.mock.callCount(), 1);
  });

  it("should, for backup of category foo, initially queue only '/foo/'", async function() {
    const backup = new Backup(ORIGIN, {backupDir: '/tmp/bak', category: 'foo', simultaneous: 2}, ENDPOINT, '0.42');
    mock.method(backup, 'checkFetch', () => {
      return Promise.resolve();
    });

    await backup.execute();

    assert.deepEqual(backup.queue.get('/foo/'), {inFlight: false, failures: 0});
    assert.equal(backup.queue.size, 1);
    assert.equal(backup.checkFetch.mock.callCount(), 1);
  });

  it("should, for backup of category foo with flag -p, initially queue '/foo/' and '/public/foo/'", async function() {
    const backup = new Backup(ORIGIN, {backupDir: '/tmp/bak', category: 'foo', includePublic: true, simultaneous: 2}, ENDPOINT, '0.42');
    mock.method(backup, 'checkFetch', () => {
      return Promise.resolve();
    });

    await backup.execute();

    assert.deepEqual(backup.queue.get('/foo/'), {inFlight: false, failures: 0});
    assert.deepEqual(backup.queue.get('/public/foo/'), {inFlight: false, failures: 0});
    assert.equal(backup.queue.size, 2);
    assert.equal(backup.checkFetch.mock.callCount(), 1);
  });
});

describe("enqueue", function (context) {
  it("should enqueue an item only once", function () {
    const backup = new Backup(ORIGIN, {}, 'https://server/user', '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.queue.get(PATH1).failures = 99;
    backup.enqueue(PATH1);

    assert.equal(backup.queue.get(PATH1).failures, 99);
    const queueOrder = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder, [ PATH1, PATH2 ]);

  });
});

describe("checkFetch", function (context) {
  beforeEach(() => {
    // mock.method(global, 'setImmediate');
    // mock.method(global, 'setTimeout');
  });

  afterEach(() => {
    mock.restoreAll();
    fetchMock.hardReset();
  });

  it("should fetch first not-in-flight if below limit [unit]", async function () {

    const backup = new Backup(ORIGIN, {backupDir: '/tmp', simultaneous: 2}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);
    backup.queue.get(PATH2).inFlight = true;

    mock.method(backup, 'fetchItem', () => {
      return Promise.resolve();
    });

    await backup.checkFetch();
    assert.equal(backup.queue.get(PATH1).inFlight, true);
    assert.equal(backup.fetchItem.mock.callCount(), 1);
  });

  it("should fetch first not-in-flight if below limit [HTTP]", async function () {
    const BACKUP_PATH = '/tmp/permanent-error/'
    await rm(BACKUP_PATH, { recursive: true, force: true });
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 3}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.queue.get(PATH2).inFlight = true;
    backup.enqueue(PATH3);

    fetchMock.mockGlobal().route(new URL(PATH1.slice(1), ENDPOINT), {"name":"test","@context":"http://remotestorage.io/spec/modules\
/myfavoritedrinks/drink"}
    );

    const checkPrmse = backup.checkFetch();
    await backup.pausePrms;
    assert.strictEqual(backup.queue.get(PATH1).inFlight, true);
    await checkPrmse;
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // this fetch is complete
    assert.strictEqual(backup.queue.get(PATH1), undefined);
    // the file was written
    assert.ok((await stat(path.join(backup.backupDir, ...PATH1.split('/')))).isFile());
    // the path is not marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // fetch complete, check queue
    assert.equal(setImmediate.mock.callCount(), 1);
    // simultaneous connections not maxed-out
    assert.equal(setTimeout.mock.callCount(), 1);
  });

  it("should write folder to file 000_folder-description.json", async function (t) {
    const BACKUP_PATH = '/tmp/write-folder/'
    await rm(BACKUP_PATH, { recursive: true, force: true });
    t.mock.method(global, "setImmediate");
    t.mock.method(global, "setTimeout");
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 3}, ENDPOINT, '0.42');
    backup.enqueue(FOLDER_PATH);

    fetchMock.mockGlobal().route(new URL(FOLDER_PATH.slice(1), ENDPOINT), folderDescription
    );

    const checkPrmse = backup.checkFetch();
    await backup.pausePrms;
    assert.deepEqual(backup.queue.get(FOLDER_PATH), {inFlight: true, failures: 0});
    await checkPrmse;
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(FOLDER_PATH.slice(1), ENDPOINT).href);
    // this fetch is complete
    assert.strictEqual(backup.queue.get(FOLDER_PATH), undefined);
    // the folder children were queued
    for (const childPath of Object.keys(folderDescription.items)) {
      assert.deepEqual(backup.queue.get(FOLDER_PATH + childPath), {inFlight: false, failures: 0});
    }
    assert.equal(backup.queue.size, Object.keys(folderDescription.items).length );
    // the directory was created
    assert.ok((await stat(path.join(backup.backupDir, ...FOLDER_PATH.split('/')))).isDirectory());
    // the remoteStorage folder description was saved to a file
    assert.ok((await stat(path.join(backup.backupDir, ...FOLDER_PATH.split('/'), '000_folder-description.json'))).isFile());
    // the path is not marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // fetch complete, check queue
    assert.equal(setImmediate.mock.callCount(), 1);
    // simultaneous connections not maxed-out
    assert.equal(setTimeout.mock.callCount(), 1);
  });

  it("should limit # simultaneous connections [HTTP]", async function () {
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: '/tmp', simultaneous: 2}, ENDPOINT, '0.42');
    await mkdir('/tmp/category/folder', {recursive: true});
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);

    fetchMock.mockGlobal().route('*', {"name":"test","@context":"http://remotestorage.io/spec/modules\
/myfavoritedrinks/drink"}
    );

    const checkPrmse = Promise.allSettled([backup.checkFetch(), backup.checkFetch()]);
    await backup.pausePrms;
    assert.strictEqual(backup.queue.get(PATH1).inFlight, true);
    assert.strictEqual(backup.queue.get(PATH2).inFlight, true);
    await checkPrmse;
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, 'https://a.b/user/category/folder/document1');
    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, 'https://a.b/user/category/folder/document2');
    // these fetches complete
    assert.strictEqual(backup.queue.get(PATH1), undefined);
    assert.strictEqual(backup.queue.get(PATH2), undefined);
    // neither path is marked as failure
    assert.equal(backup.failedPaths.size, 0);
    // fetches complete, check queue
    assert.equal(setImmediate.mock.callCount(), 2);
    // simultaneous connections maxed-out
    assert.equal(setTimeout.mock.callCount(), 1);
  });

  it("should, on connection error, move item to end of queue", async function () {
    const BACKUP_PATH = '/tmp/connection-error/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 3}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.queue.get(PATH2).failures = 2;
    backup.enqueue(PATH3);

    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), {throws: new TypeError(`Failed to fetch ${PATH1}`)})
        .route(new URL(PATH2.slice(1), ENDPOINT), {throws: new Error(`Request timed out on ${PATH2}`)})
    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, failures: 1});
    // The item was moved to the back of the queue.
    const queueOrder = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder, [ PATH2, PATH3, PATH1 ]);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is not yet marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);

    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, new URL(PATH2.slice(1), ENDPOINT).href);
    // After third failure, the client should not try again
    assert.deepEqual(backup.queue.get(PATH2), undefined);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH2.split('/'))), {code: 'ENOENT'});
    // After third failure, the path is marked as a failure
    assert.equal(backup.failedPaths.has(PATH2), true);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 2);
  });

  it("should, on permanent error, remove item from queue", async function () {
    const BACKUP_PATH = '/tmp/permanent-error/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 3}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);

    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), 403)
        .route(new URL(PATH2.slice(1), ENDPOINT), 404);
    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // this fetch failed
    assert.strictEqual(backup.queue.get(PATH1), undefined);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is marked as a failure
    assert.ok(backup.failedPaths.has(PATH1));
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);

    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, new URL(PATH2.slice(1), ENDPOINT).href);
    // this fetch failed
    assert.strictEqual(backup.queue.get(PATH2), undefined);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is marked as a failure
    assert.ok(backup.failedPaths.has(PATH2));
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 2);
  });

  it("should, on server error, move item to end of queue", async function () {
    const BACKUP_PATH = '/tmp/server-error/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 3}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);
    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), 500)
        .route(new URL(PATH2.slice(1), ENDPOINT), 502);

    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, failures: 1});
    // The item was moved to the back of the queue.
    const queueOrder = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder, [ PATH2, PATH3, PATH1 ]);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is not yet marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);

    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, new URL(PATH2.slice(1), ENDPOINT).href);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH2), {inFlight: false, failures: 1});
    // The item was moved to the back of the queue.
    const queueOrder2 = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder2, [PATH3, PATH1, PATH2 ]);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH2.split('/'))), {code: 'ENOENT'});
    // the path is not yet marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 2);
  });

  it("should, on status 429, pause all requests for Retry-After time, and move item to end of queue", async function () {
    const BACKUP_PATH = '/tmp/retry-after/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    mock.method(console, 'warn');
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 1}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);
    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), {status: 429, headers: {'Retry-After': '7'}});

    let oldPausePrms = backup.pausePrms;
    await backup.checkFetch();
    assert.notStrictEqual(backup.pausePrms, oldPausePrms);
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // verifies warning was logged
    const pauseMsg = console.warn.mock.calls.find(call => /pausing for 7s/.test(call.arguments[0]));
    assert(pauseMsg);
    assert.equal(backup.defaultRetryAfterMs, 1500);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, failures: 0});
    // The item was moved to the back of the queue.
    const queueOrder = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder, [ PATH2, PATH3, PATH1 ]);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is not marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);
  });

  it("should, on status 503, pause all requests until Retry-After time, and move item to end of queue", async function () {
    const RETRY_AFTER = new Date(Date.now() + 10 * 60 * 1000);   // 10 minutes
    const PAUSE_REGEXP = /pausing for ([\d.]+)s/;
    const BACKUP_PATH = '/tmp/retry-after/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    mock.method(console, 'warn');
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 1}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);
    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), {status: 503, headers: {'Retry-After': RETRY_AFTER}});

    let oldPausePrms = backup.pausePrms;
    await backup.checkFetch();
    assert.notStrictEqual(backup.pausePrms, oldPausePrms);
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // verifies warning was logged
    const pauseCall = console.warn.mock.calls.find(call => PAUSE_REGEXP.exec(call.arguments[0]));
    const pauseSec = parseFloat(PAUSE_REGEXP.exec(pauseCall.arguments[0])[1]);
    assert(pauseSec > 9*60);   // more than 9 minutes
    assert(pauseSec < 11*60);   // less than 11 minutes
    assert.equal(backup.defaultRetryAfterMs, 1500);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, failures: 0});
    // The item was moved to the back of the queue.
    const queueOrder = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder, [ PATH2, PATH3, PATH1 ]);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is not marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);
  });

  it("should, on status 503 w/o Retry-After header, pause all requests for default time, and move item to end of queue", async function () {
    const BACKUP_PATH = '/tmp/retry-after/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    mock.method(console, 'warn');
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 1}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);
    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), 503);

    let oldPausePrms = backup.pausePrms;
    await backup.checkFetch();
    assert.notStrictEqual(backup.pausePrms, oldPausePrms);
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // verifies warning was logged
    const pauseMsg = console.warn.mock.calls.find(call => /pausing for 1.5s/.test(call.arguments[0]));
    assert(pauseMsg);
    assert.equal(backup.defaultRetryAfterMs, 3000);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, failures: 0});
    // The item was moved to the back of the queue.
    const queueOrder = Array.from(backup.queue.keys());
    assert.deepEqual(queueOrder, [ PATH2, PATH3, PATH1 ]);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // the path is not marked as a failure
    assert.equal(backup.failedPaths.size, 0);
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);
  });
});
