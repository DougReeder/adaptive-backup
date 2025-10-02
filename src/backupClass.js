import process from "node:process";
import os from "node:os";
import path from 'node:path';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import encodePath from './encodePath.js';
import colors from "colors";

const MAX_FAILURES_PER_PATH = 3;

export class Backup {
  ORIGIN;
  backupDir;
  token;
  category;
  includePublic;
  simultaneous;
  storageEndpoint;
  programVersion;

  queue = new Map();
  failedPaths = new Set();
  pausePrms = Promise.resolve(true);   // initially not paused
  defaultRetryAfterMs = 1500;
  isAbandoned = false;

  constructor(ORIGIN, options, storageEndpoint, programVersion) {
    this.ORIGIN = ORIGIN;
    this.backupDir = options.backupDir;
    this.token = options.token;
    this.category = options.category;
    this.includePublic = options.includePublic;
    this.simultaneous = options.simultaneous;
    this.storageEndpoint = storageEndpoint;
    this.programVersion = programVersion;
  }

  async execute() {
    const trashDir = path.join(os.tmpdir(), path.basename(this.backupDir) + Date.now());
    try {
      await rename(this.backupDir, trashDir);
      console.info(colors.green(`Moved old “${this.backupDir}” to “${trashDir}”`));
    } catch (err) {
      if ('ENOENT' !== err.code) {
        throw err;
      }
    }

    if (process.env.NODE_DEBUG) {
      console.time('total download time')
    }
    console.info(`Starting backup of ${this.category ? "“" + this.category + "”" : "all categories"}`);
    const initialFolder = this.category
        ? `/${this.category}/`
        : '/';
    this.enqueue(initialFolder);
    if (this.includePublic && this.category && 'public' !== this.category) {
      this.enqueue(path.posix.join('/public', initialFolder));
    }
    // Doesn't impose any delay here, since no content fetches have started.
    await this.checkFetch();
  }

  enqueue(rsPath) {
    if (this.isAbandoned) {
      console.error(colors.red(`Backup abandoned. Not queueing ${rsPath}`));
      return;
    }
    if (!this.queue.has(rsPath)) {
      this.queue.set(rsPath, {inFlight: false, failures: 0});
      console.debug(colors.gray(`Enqueued ${rsPath}`));
    } else {
      console.warn(colors.yellow(`${rsPath} was already in queue`));
    }
  }

  async checkFetch() {
    await this.pausePrms;   // waits until not paused

    let numInFlight = 0;
    let nextPath;
    for (const [rsPath, fetchRecord] of this.queue) {
      // console.log(`checking ${rsPath}: ${JSON.stringify(fetchRecord)}`);
      if (fetchRecord.inFlight) {
        if (++numInFlight >= this.simultaneous && nextPath) {
          // We know we can't launch another, so the exact number is unimportant.
          break;
        }
      } else {
        if (!nextPath) {
          nextPath = rsPath;
        }
      }
    }
    console.debug(colors.gray(`${this.queue.size} items in queue; ${numInFlight}/${this.simultaneous} connections in use`));

    if (nextPath) {
      if (numInFlight < this.simultaneous) {
        // This is initiated before fetchItem,
        // so another fetch could be started before
        // the first finishes.
        if (++numInFlight < this.simultaneous) {
          // It appears we can start another fetch
          // but waits, to appear non-abusive.
          setTimeout(this.checkFetch.bind(this), 1);
        }

        const fetchRecord = this.queue.get(nextPath);
        fetchRecord.inFlight = true;
        if ('/' === nextPath.slice(-1)) {
          await this.fetchItem(nextPath, fetchRecord, this.handleFolder.bind(this));
        } else {
          await this.fetchItem(nextPath, fetchRecord, this.handleDocument.bind(this));
        }
      } else {
        console.debug(colors.gray("connections are maxed-out"));
      }
    } else {
      console.debug(colors.gray("all queued items are being fetched"));
    }
  }

  async fetchItem(rsPath, fetchRecord, handle) {
    try {
      console.debug(colors.gray(`Fetching ${rsPath}`));
      const fetchOptions = {
        headers: {
          "Authorization": `Bearer ${this.token}`,
          "User-Agent": `AdaptiveBackup/${this.programVersion}`,
          "Origin": this.ORIGIN
        }
      };
      const res = await fetch(new URL(encodePath(rsPath.slice(1)), this.storageEndpoint), fetchOptions);

      switch (res.status) {
        case 200:
          await handle(rsPath, res);
          break;
        case 429:
        case 503:
          const retryAfterMs = this.extractRetryAfterMs(res);
          this.pausePrms = new Promise((res) => {
            setTimeout(res, retryAfterMs);
          });
          console.warn(colors.yellow(`${res.status}${res.statusText ? " " + res.statusText : ""}: pausing for ${retryAfterMs/1000}s, will retry ${rsPath}`))
          break;
        case 401:
        case 403:
          console.error(colors.red(`${res.status}${res.statusText ? " " + res.statusText : ""}: This token lacks permission to read ${rsPath}`));
          this.dequeue(rsPath);
          this.failedPaths.add(rsPath);
          break;
        case 404:
        case 410:
          console.error(colors.red(`${res.status}${res.statusText ? " " + res.statusText : ""} ${rsPath} was deleted after this backup started`));
          this.dequeue(rsPath);
          this.failedPaths.add(rsPath);
          break;
        case 500:
        case 502:
        default:
          ++fetchRecord.failures;
          // falls through
        case 504:
          console.error(colors.red(`${res.status}${res.statusText ? " " + res.statusText : ""} ${await res.text()}: will retry ${rsPath}`));
          break;
      }
    } catch (err) {
      ++fetchRecord.failures;
      console.error(colors.red(rsPath + ":", err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err));
    } finally {
      // TODO: Should the request be aborted if we don't consume the body?
      if (this.queue.has(rsPath)) {
        fetchRecord.inFlight = false;
        this.queue.delete(rsPath);  // moves to end
        this.queue.set(rsPath, fetchRecord);  // moves to end

        if (fetchRecord.failures >= MAX_FAILURES_PER_PATH) {
          console.error(colors.red(`${rsPath} ${fetchRecord.failures}/${MAX_FAILURES_PER_PATH} failures; giving up`));
          this.dequeue(rsPath);
          this.failedPaths.add(rsPath);
        }

        if (this.isAbandoned) {
          this.dequeue(rsPath);
          this.failedPaths.add(rsPath);
        }
      }
      // imposes a slight delay to allow the connection to be closed,
      // and allows the queue to be updated
      setImmediate(this.checkFetch.bind(this));
    }
  }

  async handleFolder(folderPath, res) {
    const resText = await res.text();
    const items = JSON.parse(resText).items;
    console.debug(colors.gray((folderPath + ": " + JSON.stringify(items)).slice(0, 125)));

    const directory = path.join(this.backupDir, ...folderPath.split('/'));
    console.info(`creating directory “${directory}” and folder description`);
    await mkdir(directory, {recursive: true});

    const folderDescriptionPath = path.join(this.backupDir, ...folderPath.split('/'), '000_folder-description.json');
    await writeFile(folderDescriptionPath, resText);

    for (const rsPath in items) {
      this.enqueue(folderPath + rsPath);
    }
    this.dequeue(folderPath);
  }

  async handleDocument(rsPath, res) {
    const filePath = path.join(this.backupDir, ...rsPath.split('/'));
    console.info(`writing ${rsPath} to ${this.backupDir}`);
    await writeFile(filePath, res.body);
    // console.debug(colors.gray(`wrote ${rsPath} to ${this.backupDir}`));
    this.dequeue(rsPath);
  }

  dequeue(rsPath) {
    this.queue.delete(rsPath);
    console.debug(colors.gray(`Dequeued ${rsPath}`));
    if (this.queue.size === 0) {
      this.complete();
    }
  }

  complete() {
    if (this.failedPaths.size > 0) {
      console.error(colors.red(`These downloads failed:`));
      console.error(colors.red(Array.from(this.failedPaths).join("\n")));
    }
    if (process.env.NODE_DEBUG) {
      console.timeEnd('total download time')
    }
    if (this.isAbandoned) {
      console.error(colors.red(`Backup abandoned before completion. Exiting.`));
      process.exit(2);
    } else {
      if (this.failedPaths.size === 0) {
        console.info(colors.green(`Backup completed`));
      } else {
        console.error(colors.red(`Backup completed with ${this.failedPaths.size} failed downloads.`));
      }
      process.exit(0);
    }
  }

  extractRetryAfterMs(res) {
    let retryAfterMs = parseInt(res.headers.get('retry-after')) * 1000;
    if (!(retryAfterMs > 0)) {
      retryAfterMs = Date.parse(res.headers.get('retry-after')) - Date.now();
    }
    if (!(retryAfterMs > 0)) {
      retryAfterMs = this.defaultRetryAfterMs;
      this.defaultRetryAfterMs *= 2;
    }
    if (retryAfterMs > 60 * 60 * 1000) {   // 1 hour
      console.error(colors.red(`Pausing for ${retryAfterMs/1000/60} minutes is too long.`));
      this.abandonGracefully();
    }
    return retryAfterMs;
  }

  abandonGracefully() {
    console.error(colors.red(`Abandoning all downloads except those in flight.`));
    this.isAbandoned = true;

    for (const [rsPath, fetchRecord] of this.queue) {
      if (!fetchRecord.inFlight) {
        this.queue.delete(rsPath);
      }
    }
  }
}
