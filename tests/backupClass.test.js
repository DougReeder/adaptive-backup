import { describe, it, mock, beforeEach, afterEach } from 'node:test'
import assert from "node:assert/strict";
import fetchMock from 'fetch-mock';
import { mkdir } from 'node:fs/promises';
import { Backup } from "../src/backupClass.js";

const ORIGIN = 'https://myapp.a';

describe("enqueue", function (context) {
  it("should enqueue an item only once", function () {
    const backup = new Backup(ORIGIN, {}, 'https://server/user', '0.42');
    const PATH2 = 'category/folder/document2';
    backup.enqueue('category/folder/document1');
    backup.enqueue(PATH2);
    backup.queue.get(PATH2).tries = 99;
    backup.enqueue(PATH2);

    assert.equal(backup.queue.size, 2);
    assert.equal(backup.queue.get(PATH2).tries, 99);
  });
});

describe("checkFetch", function (context) {
  beforeEach(() => {
    mock.restoreAll();
    fetchMock.clearHistory();
  });

  afterEach(() => fetchMock.unmockGlobal());

  it("should fetch first not-in-flight if below limit [unit]", async function () {
    const PATH1 = 'category/folder/document1';
    const PATH2 = 'category/folder/document2';
    const PATH3 = 'category/folder/document3';
    const ENDPOINT = 'https://a.b';

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
    const PATH1 = 'category/folder/document1';
    const PATH2 = 'category/folder/document2';
    const PATH3 = 'category/folder/document3';
    const ENDPOINT = 'https://a.b/user/';

    global.setImmediate = mock.fn();
    global.setTimeout = mock.fn();
    const backup = new Backup(ORIGIN, {backupDir: '/tmp', simultaneous: 3}, ENDPOINT, '0.42');
    await mkdir('/tmp/category/folder', {recursive: true});
    backup.enqueue(PATH1);
    backup.enqueue(PATH2);
    backup.queue.get(PATH2).inFlight = true;
    backup.enqueue(PATH3);

    fetchMock.mockGlobal().route(ENDPOINT + PATH1, {"name":"test","@context":"http://remotestorage.io/spec/modules\
/myfavoritedrinks/drink"}
    );

    const checkPrmse = backup.checkFetch();
    assert.strictEqual(backup.queue.get(PATH1).inFlight, true);
    await checkPrmse;
    const call = fetchMock.callHistory.lastCall();
    assert.equal(call.args[0].href, 'https://a.b/user/category/folder/document1');
    // this fetch complete
    assert.strictEqual(backup.queue.get(PATH1), undefined);
    // fetch complete, check queue
    assert.equal(setImmediate.mock.callCount(), 1);
    // simultaneous connections not maxed-out
    assert.equal(setTimeout.mock.callCount(), 1);
  });

  it("should limit # simultaneous connections [HTTP]", async function () {
    const PATH1 = 'category/folder/document1';
    const PATH2 = 'category/folder/document2';
    const PATH3 = 'category/folder/document3';
    const ENDPOINT = 'https://a.b/user/';

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
});
