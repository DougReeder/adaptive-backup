import { describe, it, mock, beforeEach, afterEach } from 'node:test'
import assert from "node:assert/strict";
import fetchMock from 'fetch-mock';
import path from 'node:path';
import { Restore } from "../src/restoreClass.js";

const ORIGIN = 'https://myapp.b';
const ENDPOINT = 'https://c.d/someuser/';
const FOLDER_A_PATH = '/category1/folderA/';
const FOLDER_B_PATH = '/category2/';
const DOCUMENT1 = 'cms-inner.csv';
const DOCUMENT2 = 'sample.ics';
const DOCUMENT3 = 'size-by-ext.awk';
const DOCUMENT_B1 = 'equation.gif';
const PATH_A1 = FOLDER_A_PATH + DOCUMENT1;
const PATH_A2 = FOLDER_A_PATH + DOCUMENT2;
const PATH_A3 = FOLDER_A_PATH + DOCUMENT3;
const PATH_B1 = FOLDER_B_PATH + DOCUMENT_B1;


describe('execute', function () {
  afterEach(() => {
    mock.reset(); // also resets timers
  });

  it ("should, for restore of all, list only '/'", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    mock.method(restore, 'listDirectory', () => {
      return Promise.resolve();
    });
    mock.method(restore, 'checkPut',() => {
      return Promise.resolve();
    });
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await restore.execute();

    assert.equal(restore.listDirectory.mock.callCount(), 1);
    assert.deepEqual(restore.listDirectory.mock.calls[0].arguments, ['/']);
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(console.error.mock.callCount(), 0);
    assert.equal(console.warn.mock.callCount(), 0);
  });

  it("should, for restore of category 'some-category', list only '/some-category/'", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: 'some-category', simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    mock.method(restore, 'listDirectory', () => {
      return Promise.resolve();
    });
    mock.method(restore, 'checkPut',() => {
      return Promise.resolve();
    });
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await restore.execute();

    assert.equal(restore.listDirectory.mock.callCount(), 1);
    assert.deepEqual(restore.listDirectory.mock.calls[0].arguments, ['/some-category/']);
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(console.error.mock.callCount(), 0);
    assert.equal(console.warn.mock.callCount(), 0);
  });

  it("should, for restore of category 'another-category' w/ public, list '/another-category/' and '/public/another-category/'", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: 'another-category', includePublic: true, simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    mock.method(restore, 'listDirectory', async folderPath => {
      if (!folderPath.startsWith('/public/')) {
        return Promise.resolve();
      } else {
        throw Object.assign(new Error("ENOENT: no such file or directory, opendir '/public/another-category'"),
            { "errno": -2, "code": "ENOENT", "syscall": "opendir", "path": "/public/another-category" });
      }
    });
    mock.method(restore, 'checkPut',() => {
      return Promise.resolve();
    });
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await restore.execute();

    assert.equal(restore.listDirectory.mock.callCount(), 2);
    assert.deepEqual(restore.listDirectory.mock.calls[0].arguments, ['/another-category/']);
    assert.deepEqual(restore.listDirectory.mock.calls[1].arguments, ['/public/another-category/']);
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(console.error.mock.callCount(), 0);
    assert.equal(console.warn.mock.callCount(), 0);
  });
});

describe("listDirectory", function() {
  afterEach(() => {
    mock.reset(); // also resets timers
  });

  it("should enqueue all the descendant files but not directories, folder descriptions, nor dotfiles", async function() {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    mock.method(restore, 'checkPut',() => {
      return Promise.resolve();
    });
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await restore.listDirectory('/');

    const metadataA1 = restore.queue.get(PATH_A1).metadata;
    assert.deepEqual(metadataA1.ETag, '"4c2d3293246cec8314698eac5acb9789-cached"');
    assert.deepEqual(metadataA1['Content-Type'], "text/csv; charset=UTF-8");
    assert.deepEqual(metadataA1['Content-Length'], 101176);

    assert.deepEqual(restore.queue.get(PATH_A2).metadata, undefined);

    const metadataA3 = restore.queue.get(PATH_A3).metadata;
    assert.deepEqual(metadataA3.ETag, '"30259989a31a3f08480c2868e006f235-cached"');
    assert.deepEqual(metadataA3['Content-Type'], "text/x-awk; charset=ascii");
    assert.deepEqual(metadataA3['Content-Length'], 10279);

    const metadataB1 = restore.queue.get(PATH_B1).metadata;
    assert.deepEqual(metadataB1.ETag, '"1cf3daf3b211512c128642b4ce5750f6-cached"');
    assert.deepEqual(metadataB1['Content-Type'], "image/gif; charset=binary");
    assert.deepEqual(metadataB1['Content-Length'], 101209);

    assert.equal(restore.queue.size, 4);
    assert.equal(restore.checkPut.mock.callCount(), 0);
    assert.equal(console.error.mock.callCount(), 0);
    assert.equal(console.warn.mock.callCount(), 1); // category1/000_folder-description.json missing
  });

  it("should enqueue nothing when upload abandoned", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    restore.isAbandoned = true;
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await restore.listDirectory('/');

    assert.equal(restore.queue.size, 0);
    assert.equal(console.error.mock.callCount(), 1);
    assert.equal(console.warn.mock.callCount(), 0);
  });

  it("should throw when category doesn't exist", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: 'nonexistent', simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await assert.rejects(restore.listDirectory('/nonexistent/'));

    assert.equal(restore.queue.size, 0);
    assert.equal(console.error.mock.callCount(), 0);
    assert.equal(console.warn.mock.callCount(), 1);
  });
});

describe("checkPut", function() {
  afterEach(() => {
    mock.reset(); // also resets timers
    fetchMock.hardReset();
  });

  it("should upload first not-in-flight if below connection limit", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 10, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    restore.enqueue(PATH_A1, {"Content-Type":"text/csv; charset=UTF-8", ETag: '999-old-value-999',"Content-Length": 101176 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"});
    const putRecordA1 = restore.queue.get(PATH_A1);
    restore.enqueue(PATH_A2, {"Content-Type":"text/calendar; charset=UTF-8", ETag: '888-old-value-888',"Content-Length": 10449 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"});
    restore.queue.get(PATH_A2).inFlight = true;
    restore.enqueue(PATH_B1, undefined);
    const putRecordB = restore.queue.get(PATH_B1);

    mock.method(restore, 'checkPut');
    mock.method(restore, 'putDocument', async function (rsPath, _) {
      this.dequeue(rsPath); // simulated success
    });
    mock.timers.enable();
    mock.method(console, 'error');
    mock.method(console, 'warn');

    await restore.checkPut();

    assert.equal(restore.putDocument.mock.callCount(), 1);
    const call = restore.putDocument.mock.calls[0];
    assert.deepStrictEqual(call.arguments, [PATH_A1, putRecordA1]);
    assert.strictEqual(call.this, restore);

    await restore.checkPut();

    assert.equal(restore.checkPut.mock.callCount(), 2); // setTimout is mocked
    assert.equal(restore.putDocument.mock.callCount(), 2);
    assert.deepStrictEqual(restore.putDocument.mock.calls[1].arguments, [PATH_B1, putRecordB]);
    assert.equal(console.error.mock.callCount(), 0);
    assert.equal(console.warn.mock.callCount(), 0);
  });

  it("should limit # simultaneous connections", async function () {
    const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 3, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');
    restore.enqueue(PATH_A1, {"Content-Type":"text/csv; charset=UTF-8", ETag: '999-old-value-999',"Content-Length": 101176 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"});
    restore.queue.get(PATH_A1).inFlight = true;
    restore.enqueue(PATH_A2, {"Content-Type":"text/calendar; charset=UTF-8", ETag: '888-old-value-888',"Content-Length": 10449 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"});
    const putRecordA2 = restore.queue.get(PATH_A2);
    restore.enqueue(PATH_A3, {"Content-Type":"text/x-shellscript; charset=us-ascii", ETag: '777-old-value-777',"Content-Length": 10449 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"});
    const putRecordA3 = restore.queue.get(PATH_A3);
    restore.enqueue(PATH_B1, undefined);

    mock.method(restore, 'checkPut');
    mock.method(restore, 'putDocument', async function (rsPath, putRecord) {
      putRecord.inFlight = true; // test as if the calls to checkPut overlap
    });
    mock.timers.enable();

    await restore.checkPut();

    assert.equal(restore.putDocument.mock.callCount(), 1);
    assert.deepStrictEqual(restore.putDocument.mock.calls[0].arguments, [PATH_A2, putRecordA2]);


    await restore.checkPut();

    assert.equal(restore.putDocument.mock.callCount(), 2);
    assert.deepStrictEqual(restore.putDocument.mock.calls[1].arguments, [PATH_A3, putRecordA3]);


    await restore.checkPut();

    assert.equal(restore.checkPut.mock.callCount(), 3); // setTimout is mocked
    assert.equal(restore.putDocument.mock.callCount(), 2);
  });
});

describe("putDocument using MD5 ETags", async function() {
  const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 2, etagAlgorithm: 'md5'}, ENDPOINT, '0.69');

  beforeEach(() => {
    restore.queue.clear();
    restore.queue.set('/some/path/to/prevent/exit', { inFlight: true, failures: 0, metadata: undefined });
    restore.failedPaths.clear();
  });

  afterEach(() => {
    mock.reset();
    fetchMock.hardReset();
  });

  it("should create file lacking metadata", async function () {
    const putRecord = { inFlight: false, failures: 0, metadata: undefined };
    restore.queue.set(PATH_A2, putRecord);
    fetchMock.mockGlobal().put(new URL(PATH_A2.slice(1), ENDPOINT), {status: 201, headers: {ETag: '"6cd034d5846c91c5aae867fd50c23d96-returned"'}});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });
    mock.method(console, 'error');
    mock.method(console, 'warn');

    const promise = restore.putDocument(PATH_A2, putRecord);
    assert.ok(putRecord.inFlight);
    const [statusCode, putETag, contentType, contentLength] = await promise;
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH_A2.slice(1), ENDPOINT).href);
    assert.equal(call.args[1].method, 'PUT');
    assert.equal(call.args[1].headers['Content-Type'], 'text/calendar');
    assert.equal(call.args[1].headers['Content-Length'], 449);
    assert.equal(call.args[1].headers['If-None-Match'], '"6cd034d5846c91c5aae867fd50c23d96"');
    assert.ok(call.args[1].body);

    assert.ok(!restore.queue.has(PATH_A2));
    assert.equal(restore.checkPut.mock.callCount(), 1);

    assert.equal(statusCode, 201);
    assert.equal(contentType, 'text/calendar');
    assert.equal(contentLength, 449);
    assert.equal(putETag, '"6cd034d5846c91c5aae867fd50c23d96-returned"');
    assert.equal(console.error.mock.callCount(), 1);
    assert.equal(console.warn.mock.callCount(), 0);
  });

  it("should update file using Content-Type from metadata", async function () {
    const putRecord = {
      inFlight: false, failures: 0, metadata: {"Content-Type":"text/csv; charset=UTF-8", ETag: '999-old-value-999',"Content-Length": 101176 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"}
    };
    restore.queue.set(PATH_A1, putRecord);
    fetchMock.mockGlobal().put(new URL(PATH_A1.slice(1), ENDPOINT), {status: 200, headers: {ETag: '"4c2d3293246cec8314698eac5acb9789-returned"'}});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const promise = restore.putDocument(PATH_A1, putRecord);
    assert.ok(putRecord.inFlight);
    const [statusCode, putETag, contentType, contentLength] = await promise;
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH_A1.slice(1), ENDPOINT).href);
    assert.equal(call.args[1].method, 'PUT');
    assert.equal(call.args[1].headers['Content-Type'], "text/csv; charset=UTF-8");
    assert.equal(call.args[1].headers['Content-Length'], 1176);
    assert.equal(call.args[1].headers['If-None-Match'], '"4c2d3293246cec8314698eac5acb9789"');
    assert.ok(call.args[1].body);

    assert.ok(!restore.queue.has(PATH_A1));
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(statusCode, 200);
    assert.equal(contentType, "text/csv; charset=UTF-8");
    assert.equal(contentLength, 1176);
    assert.equal(putETag, '"4c2d3293246cec8314698eac5acb9789-returned"');
  });

  it("should handle calculated ETag matching server ETag", async function () {
    const putRecord = { inFlight: false, failures: 0, metadata: undefined };
    restore.queue.set(PATH_B1, putRecord);
    fetchMock.mockGlobal().put(new URL(PATH_B1.slice(1), ENDPOINT), {status: 412, headers: {}});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const [statusCode, putETag, contentType, contentLength] = await restore.putDocument(PATH_B1, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH_B1.slice(1), ENDPOINT).href);
    assert.equal(call.args[1].method, 'PUT');
    assert.equal(call.args[1].headers['Content-Type'], 'image/gif');
    assert.equal(call.args[1].headers['Content-Length'], 1209);
    assert.equal(call.args[1].headers['If-None-Match'], '"1cf3daf3b211512c128642b4ce5750f6"');
    assert.ok(call.args[1].body);

    assert.ok(!restore.queue.has(PATH_B1));
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(statusCode, 412);
    assert.equal(contentType, 'image/gif');
    assert.equal(contentLength, 1209);
    assert.equal(putETag, '"1cf3daf3b211512c128642b4ce5750f6"');
  });

  it("should, on status 429 or 503, pause all requests & move record to back of queue", async function () {
    restore.enqueue(PATH_A3, {"Content-Type":"text/x-awk", ETag: '777-old-value-777',"Content-Length": 10449 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"});
    const putRecordA3 = restore.queue.get(PATH_A3);
    restore.enqueue(PATH_B1, undefined);
    const putRecordB = restore.queue.get(PATH_B1);
    assert.equal(Array.from(restore.queue).at(-1)[0], PATH_B1);
    fetchMock.mockGlobal().
      once(new URL(PATH_A3.slice(1), ENDPOINT), {status: 429, headers: {'Retry-After': '7'}}).
      once(new URL(PATH_B1.slice(1), ENDPOINT), {status: 503, headers: {'Retry-After': '10'}, body: "offline for maintenance"});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });
    mock.method(console, 'warn');

    let oldPausePrms = restore.pausePrms;
    const [statusCode] = await restore.putDocument(PATH_A3, putRecordA3);
    assert.notStrictEqual(restore.pausePrms, oldPausePrms);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);

    assert.ok(restore.queue.has(PATH_A3));
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(Array.from(restore.queue).at(-1)[0], PATH_A3); // end of queue
    assert.equal(putRecordA3.inFlight, false);
    assert.equal(putRecordA3.failures, 0);

    // verifies warning was logged
    const pauseMsg = console.warn.mock.calls.find(call => /pausing for 7s/.test(call.arguments[0]));
    assert(pauseMsg);
    assert.equal(restore.defaultRetryAfterMs, 1500);
    // the path is not marked as a failure
    assert.equal(restore.failedPaths.size, 0);

    assert.equal(statusCode, 429);


    let middlePausePrms = restore.pausePrms;
    const [statusCode2] = await restore.putDocument(PATH_B1, putRecordB);
    assert.notStrictEqual(restore.pausePrms, middlePausePrms);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 2);

    assert.ok(restore.queue.has(PATH_B1));
    assert.equal(putRecordB.inFlight, false);
    assert.equal(putRecordB.failures, 0);
    assert.equal(Array.from(restore.queue).at(-1)[0], PATH_B1); // end of queue

    assert.equal(restore.checkPut.mock.callCount(), 2);
    // verifies warning was logged
    assert(console.warn.mock.calls.find(call => /pausing for 10s/.test(call.arguments[0])));
    assert.equal(restore.defaultRetryAfterMs, 1500);
    // the path is not marked as a failure
    assert.equal(restore.failedPaths.size, 0);

    assert.equal(statusCode2, 503);
  });

  it("should fail path & remove from queue after three connection errors", async function () {
    const putRecord = {
      inFlight: false, failures: 1, metadata: {"Content-Type":"text/csv; charset=UTF-8", ETag: '999-old-value-999',"Content-Length": 101176 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"}
    };
    restore.queue.set(PATH_A1, putRecord);
    fetchMock.mockGlobal()
        .once(new URL(PATH_A1.slice(1), ENDPOINT), {throws: new TypeError(`Failed to fetch ${PATH_A1}`)})
        .once(new URL(PATH_A1.slice(1), ENDPOINT), {throws: new Error(`Request timed out on ${PATH_A1}`)});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const [statusCode1] = await restore.putDocument(PATH_A1, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);

    assert.equal(statusCode1, undefined);
    assert.ok(restore.queue.has(PATH_A1));
    assert.equal(putRecord.inFlight, false);
    assert.equal(putRecord.failures, 2);

    // the path is not yet marked as a failure
    assert.equal(restore.failedPaths.size, 0);
    assert.equal(restore.checkPut.mock.callCount(), 1);

    const [statusCode2] = await restore.putDocument(PATH_A1, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(statusCode2, undefined);
    assert.ok(!restore.queue.has(PATH_A1));
    assert.equal(putRecord.inFlight, false);
    assert.equal(putRecord.failures, 3);

    assert.equal(fetchMock.callHistory.callLogs.length, 2);
    // After third failure, the path is removed from queue marked as a failure
    assert.ok(restore.failedPaths.has(PATH_A1));
    assert.equal(restore.checkPut.mock.callCount(), 2);
  });

  it("should remove from queue after third server error", async function () {
    const putRecord = { inFlight: false, failures: 1, metadata: undefined };
    restore.queue.set(PATH_A2, putRecord);
    fetchMock.mockGlobal().
    once(new URL(PATH_A2.slice(1), ENDPOINT), {status: 500, body: "Null Pointer Exception"}).
    once(new URL(PATH_A2.slice(1), ENDPOINT), {status: 502, body: "PROXY Protocol mismatch"});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const [statusCode1] = await restore.putDocument(PATH_A2, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH_A2.slice(1), ENDPOINT).href);
    assert.equal(statusCode1, 500);
    assert.deepEqual(restore.queue.get(PATH_A2), { inFlight: false, failures: 2, metadata: undefined });
    assert.ok(! restore.failedPaths.has(PATH_A2));
    assert.equal(restore.checkPut.mock.callCount(), 1);


    const [statusCode2] = await restore.putDocument(PATH_A2, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.calls()[1].args[0].href, new URL(PATH_A2.slice(1), ENDPOINT).href);
    assert.equal(restore.checkPut.mock.callCount(), 2);
    assert.equal(statusCode2, 502);
    assert.ok(! restore.queue.has(PATH_A2));
    assert.ok(restore.failedPaths.has(PATH_A2));
  });

  it("should remove item from queue, when path not authorized", async function () {
    const putRecord = { inFlight: false, failures: 0, metadata: undefined };
    restore.queue.set(PATH_A2, putRecord);
    fetchMock.mockGlobal().
    once(new URL(PATH_A2.slice(1), ENDPOINT), 401).
    once(new URL(PATH_A2.slice(1), ENDPOINT), 403);
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const [statusCode] = await restore.putDocument(PATH_A2, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.calls()[0].args[0].href, new URL(PATH_A2.slice(1), ENDPOINT).href);
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(statusCode, 401);
    assert.ok(! restore.queue.has(PATH_A2));
    assert.ok(restore.failedPaths.has(PATH_A2));
  });
});

describe("putDocument using ETags from folder description", async function() {
  const restore = new Restore(ORIGIN, {backupDir: path.join(process.cwd(), 'testBackup'), category: '', simultaneous: 2}, ENDPOINT, '0.69');

  beforeEach(() => {
    restore.queue.clear();
    restore.queue.set('/some/path/to/prevent/exit', { inFlight: false, failures: 0, metadata: undefined });
    restore.failedPaths.clear();
  });

  afterEach(() => {
    mock.reset();
    fetchMock.hardReset();
  });

  it("should update file lacking metadata", async function () {
    const putRecord = {
      inFlight: false, failures: 0, metadata: undefined
    };
    restore.queue.set(PATH_A2, putRecord);
    fetchMock.mockGlobal().put(new URL(PATH_A2.slice(1), ENDPOINT), {status: 200, headers: {ETag: '"6cd034d5846c91c5aae867fd50c23d96-returned"'}});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const promise = restore.putDocument(PATH_A2, putRecord);
    assert.ok(putRecord.inFlight);
    const [statusCode, putETag, contentType, contentLength] = await promise;
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH_A2.slice(1), ENDPOINT).href);
    assert.equal(call.args[1].method, 'PUT');
    assert.equal(call.args[1].headers['Content-Type'], 'text/calendar');
    assert.equal(call.args[1].headers['Content-Length'], 449);
    assert.equal(call.args[1].headers['If-None-Match'], undefined);
    assert.ok(call.args[1].body);

    assert.ok(!restore.queue.has(PATH_A2));
    assert.equal(restore.checkPut.mock.callCount(), 1);

    assert.equal(statusCode, 200);
    assert.equal(contentType, 'text/calendar');
    assert.equal(contentLength, 449);
    assert.equal(putETag, '"6cd034d5846c91c5aae867fd50c23d96-returned"');
  });

  it("should create file using metadata", async function () {
    const putRecord = {
      inFlight: false, failures: 0, metadata: {"Content-Type":"text/csv; charset=UTF-8", ETag: '999-old-value-999',"Content-Length": 101176 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"}
    };
    restore.queue.set(PATH_A1, putRecord);
    fetchMock.mockGlobal().put(new URL(PATH_A1.slice(1), ENDPOINT), {status: 201, headers: {ETag: '"4c2d3293246cec8314698eac5acb9789-returned"'}});
    mock.method(restore, 'checkPut', () => {
      return Promise.resolve();
    });

    const promise = restore.putDocument(PATH_A1, putRecord);
    assert.ok(putRecord.inFlight);
    const [statusCode, putETag, contentType, contentLength] = await promise;
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH_A1.slice(1), ENDPOINT).href);
    assert.equal(call.args[1].method, 'PUT');
    assert.equal(call.args[1].headers['Content-Type'], "text/csv; charset=UTF-8");
    assert.equal(call.args[1].headers['Content-Length'], 1176);
    assert.equal(call.args[1].headers['If-None-Match'], '999-old-value-999');
    assert.ok(call.args[1].body);

    assert.ok(!restore.queue.has(PATH_A1));
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(statusCode, 201);
    assert.equal(contentType, "text/csv; charset=UTF-8");
    assert.equal(contentLength, 1176);
    assert.equal(putETag, '"4c2d3293246cec8314698eac5acb9789-returned"');
  });

  it("should dequeue when saved ETag matches server ETag", async function () {
    const putRecord = { inFlight: false, failures: 0, metadata: {"Content-Type":"text/x-shellscript; charset=us-ascii", ETag: '"30259989a31a3f08480c2868e006f235-saved"',"Content-Length": 10279 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"} };
    restore.queue.set(PATH_A3, putRecord);
    fetchMock.mockGlobal().put(new URL(PATH_A3.slice(1), ENDPOINT), {status: 412, headers: {}});
    mock.method(restore, 'checkPut',() => {
      return Promise.resolve();
    });

    const [statusCode, putETag, contentType, contentLength] = await restore.putDocument(PATH_A3, putRecord);
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(fetchMock.callHistory.callLogs.length, 1);
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, new URL(PATH_A3.slice(1), ENDPOINT).href);
    assert.equal(call.args[1].method, 'PUT');
    assert.equal(call.args[1].headers['Content-Type'], 'text/x-shellscript; charset=us-ascii');
    assert.equal(call.args[1].headers['Content-Length'], 279);
    assert.equal(call.args[1].headers['If-None-Match'], '"30259989a31a3f08480c2868e006f235-saved"');
    assert.ok(call.args[1].body);

    assert.ok(! restore.queue.has(PATH_A3));
    assert.equal(restore.checkPut.mock.callCount(), 1);
    assert.equal(statusCode, 412);
    assert.equal(contentType, 'text/x-shellscript; charset=us-ascii');
    assert.equal(contentLength, 279);
    assert.equal(putETag, '"30259989a31a3f08480c2868e006f235-saved"');
  });
});

describe("getContentType", function()  {
  const restore = new Restore(ORIGIN, {backupDir: '/tmp/bakup', category: '', simultaneous: 2}, ENDPOINT, '0.69');

  it("should identify file from magic numbers, when lacking metadata", async function () {
    const filePath = path.join(process.cwd(), 'testBackup', PATH_B1);

    const type = await restore.getContentType(filePath, {items: {}});

    assert.equal(type, 'image/gif');
  });

  it("should identify file from extension, when lacking metadata & no magic numbers", async function () {
    const filePath = path.join(process.cwd(), 'testBackup', PATH_A1)

    const type = await restore.getContentType(filePath, {items: {}});

    assert.equal(type, 'text/csv');
  });

  it("should return application/octet-stream, when extension not recognized", async function () {
    const filePath = path.join(process.cwd(), 'testBackup', PATH_A3)

    const type = await restore.getContentType(filePath, {items: {}});

    assert.equal(type, 'application/octet-stream');
  });

  it("should prefer type from metadata", async function () {
    const filePath = path.join(process.cwd(), 'testBackup', PATH_A1)
    const metadata = {"Content-Type":"text/comma-separated-values; charset=UTF-8","ETag":"7eee9b1f2c9d613c9add18c98b0aca44","Content-Length":1176 ,"Last-Modified":"Wed, 10 Nov 2021 03:55:51 GMT"};

    const type = await restore.getContentType(filePath, metadata);

    assert.equal(type, 'text/comma-separated-values; charset=UTF-8');
  });
});
