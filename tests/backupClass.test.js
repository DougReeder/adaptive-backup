import { describe, it, mock, beforeEach, afterEach } from 'node:test'
import assert from "node:assert/strict";
import fetchMock from 'fetch-mock';
import path from 'node:path';
import { mkdir, rm, stat } from 'node:fs/promises';
import { Backup } from "../src/backupClass.js";

const ORIGIN = 'https://myapp.a';
const PATH1 = '/category/folder/document1';
const PATH2 = '/category/folder/document2';
const PATH3 = '/category/folder/document3';
const ENDPOINT = 'https://a.b/user/';

describe("execute", function() {
  it("should, for backup of all, initially queue only '/'", async function() {
    const backup = new Backup(ORIGIN, {backupDir: '/tmp/bak', category: '', includePublic: true, simultaneous: 2}, ENDPOINT, '0.42');
    mock.method(backup, 'checkFetch', () => {
      return Promise.resolve();
    });

    await backup.execute();

    assert.deepEqual(backup.queue.get('/'), {inFlight: false, tries: 0});
    assert.equal(backup.queue.size, 1);
    assert.equal(backup.checkFetch.mock.callCount(), 1);
  });

  it("should, for backup of category foo, initially queue only '/foo/'", async function() {
    const backup = new Backup(ORIGIN, {backupDir: '/tmp/bak', category: 'foo', simultaneous: 2}, ENDPOINT, '0.42');
    mock.method(backup, 'checkFetch', () => {
      return Promise.resolve();
    });

    await backup.execute();

    assert.deepEqual(backup.queue.get('/foo/'), {inFlight: false, tries: 0});
    assert.equal(backup.queue.size, 1);
    assert.equal(backup.checkFetch.mock.callCount(), 1);
  });

  it("should, for backup of category foo with flag -p, initially queue '/foo/' and '/public/foo/'", async function() {
    const backup = new Backup(ORIGIN, {backupDir: '/tmp/bak', category: 'foo', includePublic: true, simultaneous: 2}, ENDPOINT, '0.42');
    mock.method(backup, 'checkFetch', () => {
      return Promise.resolve();
    });

    await backup.execute();

    assert.deepEqual(backup.queue.get('/foo/'), {inFlight: false, tries: 0});
    assert.deepEqual(backup.queue.get('/public/foo/'), {inFlight: false, tries: 0});
    assert.equal(backup.queue.size, 2);
    assert.equal(backup.checkFetch.mock.callCount(), 1);
  });
});

describe("enqueue", function (context) {
  it("should enqueue an item only once", function () {
    const backup = new Backup(ORIGIN, {}, 'https://server/user', '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.queue.get(PATH1).tries = 99;
    backup.enqueue(PATH1);

    assert.equal(backup.queue.get(PATH1).tries, 99);
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
    assert.strictEqual(backup.queue.get(PATH1).inFlight, true);
    await checkPrmse;
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // this fetch is complete
    assert.strictEqual(backup.queue.get(PATH1), undefined);
    // the file was written
    stat(path.join(backup.backupDir, ...PATH1.split('/')));
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

    const checkPrmse = Promise.allSettled([backup.checkFetch(), backup.checkFetch()]) ;
    assert.strictEqual(backup.queue.get(PATH1).inFlight, true);
    assert.strictEqual(backup.queue.get(PATH2).inFlight, true);
    await checkPrmse;
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, 'https://a.b/user/category/folder/document1');
    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, 'https://a.b/user/category/folder/document2');
    // these fetches complete
    assert.strictEqual(backup.queue.get(PATH1), undefined);
    assert.strictEqual(backup.queue.get(PATH2), undefined);
    // fetches complete, check queue
    assert.equal(setImmediate.mock.callCount(), 2);
    // simultaneous connections maxed-out
    assert.equal(setTimeout.mock.callCount(), 1);
  });

  it("should, on connection error, leave item in queue", async function () {
    const BACKUP_PATH = '/tmp/connection-error/'
    await rm(BACKUP_PATH, {recursive: true, force: true});
    await mkdir(path.join(BACKUP_PATH, '/category/folder/'), {recursive: true});
    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: BACKUP_PATH, simultaneous: 3}, ENDPOINT, '0.42');
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.enqueue(PATH3);

    fetchMock.mockGlobal()
        .route(new URL(PATH1.slice(1), ENDPOINT), {throws: new TypeError("Failed to fetch")})
        .route(new URL(PATH2.slice(1), ENDPOINT), 999);
    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH1.slice(1), ENDPOINT).href);
    // The client should try again
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, tries: 1});
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);
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
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);

    await backup.checkFetch();
    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, new URL(PATH2.slice(1), ENDPOINT).href);
    // this fetch failed
    assert.strictEqual(backup.queue.get(PATH2), undefined);
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 2);
  });

  it("should, on server error, leave item in queue", async function () {
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
    assert.deepEqual(backup.queue.get(PATH1), {inFlight: false, tries: 1});
    // the file was not written
    await assert.rejects(stat.bind(this, path.join(backup.backupDir, ...PATH1.split('/'))), {code: 'ENOENT'});
    // when fetch is complete, checks queue
    assert.equal(setImmediate.mock.callCount(), 1);
  });
});
