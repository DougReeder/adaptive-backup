import process from "node:process";
import path from 'node:path';
import { opendir, readFile, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import encodePath from './encodePath.js';
import { createHash } from 'node:crypto';
import colors from "colors";
import {fileTypeFromFile} from 'file-type';
import mime from 'mime';

const MAX_FAILURES_PER_PATH = 3;
const FOLDER_DESCRIPTION = '000_folder-description.json';

export class Restore {
  ORIGIN;
  backupDir;
  token;
  category;
  includePublic;
  simultaneous;
  etagAlgorithm;
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
    this.etagAlgorithm = options.etagAlgorithm;
    this.storageEndpoint = storageEndpoint;
    this.programVersion = programVersion;
  }

  async execute() {
    if (process.env.NODE_DEBUG) {
      console.time('total upload time')
    }
    console.info(`Starting restore of ${this.category ? "“" + this.category + "”" : "all categories"}`);
    const initialFolder = this.category ? `/${this.category}/` : '/';
    await this.listDirectory(initialFolder);
    if (this.includePublic && this.category && 'public' !== this.category) {
      let publicFolder;
      try {
        publicFolder = path.posix.join('/public', initialFolder);
        await this.listDirectory(publicFolder);
      } catch (err) {
        if ('ENOENT' !== err.code) {
          console.error(colors.red(publicFolder + ": " + (err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err)));
        }
      }
    }
    // Doesn't impose any delay here, since no PUTs have started.
    await this.checkPut();
  }

  async listDirectory(folderPath) {
    if (this.isAbandoned) {
      console.error(colors.red(`Backup abandoned. Not listing ${folderPath}`));
      return;
    }
    console.debug(colors.gray(`Listing ${folderPath}`));

    const dirPath = path.join(this.backupDir, ...folderPath.split('/'));
    let folderDescription;
    try {
      const descriptionPath = path.join(dirPath, FOLDER_DESCRIPTION);
      folderDescription = JSON.parse(await readFile(descriptionPath, { encoding: 'utf8' }));
    } catch (err) {
      if ('ENOENT' === err.code) {
        console.warn(colors.yellow(`${path.posix.join(folderPath, FOLDER_DESCRIPTION)} missing; will calculate types from files`));
      } else {
        console.error(colors.red(folderPath + FOLDER_DESCRIPTION + ": " + (err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err)));
      }
    }
    if (!folderDescription?.items || 'object' !== typeof folderDescription?.items) {
      folderDescription = {items: {}}
    }

  const fsDir = await opendir(dirPath, {recursive: false});
    for await (const dirEnt of fsDir) {
      try {
        if (FOLDER_DESCRIPTION === dirEnt.name || '.' === dirEnt.name[0]) {
          // skip
        } else if (dirEnt.isFile()) {
          this.enqueue(folderPath + dirEnt.name, folderDescription.items[dirEnt.name]);
        } else if (dirEnt.isDirectory()) {
          await this.listDirectory(path.posix.join(folderPath, dirEnt.name, '/'));
        } // else is link, socket, pipe, etc.
      } catch (err) {
        console.error(colors.red(folderPath + dirEnt.name + ": " + (err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err)));
      }
    }
  }

  enqueue(rsPath, metadata) {
    if (this.isAbandoned) {
      console.error(colors.red(`Backup abandoned. Not queueing ${rsPath}`));
      return;
    }
    if (!this.queue.has(rsPath)) {
      this.queue.set(rsPath, {inFlight: false, failures: 0, metadata});
      console.debug(colors.gray(`Enqueued ${rsPath}`));
    } else {
      console.warn(colors.yellow(`${rsPath} was already in queue`));
    }
  }

  async checkPut() {
    await this.pausePrms;   // waits until not paused

    let numInFlight = 0;
    let nextPath;
    for (const [rsPath, putRecord] of this.queue) {
      // console.log(`checking ${rsPath}: ${JSON.stringify(putRecord)}`);
      if (putRecord.inFlight) {
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
        // This is initiated before putDocument,
        // so another put could be started before
        // the first finishes.
        if (++numInFlight < this.simultaneous) {
          // It appears we can start another put
          // but waits a millisecond, to appear non-abusive.
          setTimeout(this.checkPut.bind(this), 1);
        }

        const putRecord = this.queue.get(nextPath);
        await this.putDocument(nextPath, putRecord);
      } else {
        console.debug(colors.gray("connections are maxed-out"));
      }
    } else {
      console.debug(colors.gray("all queued items are being put"));
    }
  }

  async putDocument(rsPath, putRecord) {
    let putETag, contentType, contentLength, res;
    try {
      putRecord.inFlight = true;

      const filePath = path.join(this.backupDir, rsPath);
      contentType = await this.getContentType(filePath, putRecord.metadata);
      contentLength = (await stat(filePath)).size;
      const fileETag = this.etagAlgorithm ?
          '"' + await this.fileDigest(this.etagAlgorithm, filePath) + '"' :
          putRecord.metadata?.ETag;
      const reader = createReadStream(filePath);

      console.debug(colors.gray(`PUTing ${rsPath}`));
      const fetchOptions = {
        method: 'PUT',
        headers: {
          'Content-Type': contentType,
          'Content-Length': contentLength,
          ...(fileETag && { "If-None-Match": fileETag }),
          "Authorization": `Bearer ${this.token}`,
          "User-Agent": `AdaptiveBackup/${this.programVersion}`,
          "Origin": this.ORIGIN
        },
        body: reader,
        duplex: 'half'
      };
      res = await fetch(new URL(encodePath(rsPath.slice(1)), this.storageEndpoint), fetchOptions);

      switch (res.status) {
        case 200:
          console.info(`updated ${rsPath}`);
          this.dequeue(rsPath);
          putETag = res.headers.get('ETag');
          break;
        case 201:
          console.info(`created ${rsPath}`);
          this.dequeue(rsPath);
          putETag = res.headers.get('ETag');
          break;
        case 412:
          console.info(`correct version of ${rsPath} is already there`);
          this.dequeue(rsPath);
          putETag = fileETag;
          break;
        case 429:
        case 503:
          const retryAfterMs = this.extractRetryAfterMs(res);
          this.pausePrms = new Promise((res) => {
            setTimeout(res, retryAfterMs);
          });
          console.warn(colors.yellow(`${res.status}${res.statusText ? " " + res.statusText : ""}: pausing for ${retryAfterMs/1000}s, will retry ${rsPath}`));
          break;
        case 401:
        case 403:
          console.error(colors.red(`${res.status}${res.statusText ? " " + res.statusText : ""}: This token lacks permission to write ${rsPath}`));
          this.dequeue(rsPath);
          this.failedPaths.add(rsPath);
          break;
        case 500:
        case 502:
        default:
          ++putRecord.failures;
          // falls through
        case 504:
          console.error(colors.red(`${res.status}${res.statusText ? " " + res.statusText : ""} ${await res.text()}: will retry ${rsPath}`));
          break;
      }
    } catch(err) {
      ++putRecord.failures;
      console.error(colors.red(rsPath + ": " + (err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err)));
    } finally {
      if (this.queue.has(rsPath)) {
        putRecord.inFlight = false;
        this.queue.delete(rsPath);  // moves to end
        this.queue.set(rsPath, putRecord);  // moves to end

        if (putRecord.failures >= MAX_FAILURES_PER_PATH) {
          console.error(colors.red(`${rsPath} ${putRecord.failures}/${MAX_FAILURES_PER_PATH} failures; giving up`));
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
      setImmediate(this.checkPut.bind(this));
    }

    return [res?.status, putETag, contentType, contentLength];
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
      console.error(colors.red(`These uploads failed:`));
      console.error(colors.red(Array.from(this.failedPaths).join("\n")));
    }
    if (process.env.NODE_DEBUG) {
      console.timeEnd('total upload time')
    }
    if (this.isAbandoned) {
      console.error(colors.red(`Backup abandoned before completion. Exiting.`));
      process.exit(2);
    } else {
      if (this.failedPaths.size === 0) {
        console.info(colors.green(`Restore fully completed`));
      } else {
        console.error(colors.red(`Restore completed with ${this.failedPaths.size} failed downloads.`));
      }
      process.exit(0);
    }
  }

  async getContentType(filePath, metadata) {
    let contentType = metadata?.['Content-Type'];
    if (!contentType) {
      contentType = (await fileTypeFromFile(filePath))?.mime; // magic numbers
    }
    // const st = await stat(filePath);
    if (!contentType) {
      contentType = mime.getType(filePath); // extension
    }
    return contentType || 'application/octet-stream';
  }

  async fileDigest(algorithm, filePath) {
    const hash = createHash(algorithm);
    const reader1 = createReadStream(filePath);

    reader1.pipe(hash);
    return new Promise((resolve, reject) => {
      reader1.on('error', handleError);
      hash.on('error', handleError);
      hash.on('finish', () => {
        resolve(hash.digest('hex'));
      });

      function handleError(err) {
        reader1.end();
        hash.end();
        reject(err);
      }
    });
  }

  extractRetryAfterMs(res) {
    let retryAfterMs = parseInt(res.headers.get('retry-after')) * 1000;
    if (!(retryAfterMs > 0)) {
      retryAfterMs = Date.parse(res.headers.get('retry-after')) - Date.now();
    }
    if (!(retryAfterMs > 0)) {
      retryAfterMs = this.defaultRetryAfterMs;
      this.defaultRetryAfterMs *= 1.5;
    }
    if (retryAfterMs > 60 * 60 * 1000) {   // 1 hour
      console.error(colors.red(`Pausing for ${retryAfterMs/1000/60} minutes is too long.`));
      this.abandonGracefully();
    }
    return retryAfterMs;
  }

  abandonGracefully() {
    console.error(colors.red(`Abandoning all uploads except those in flight.`));
    this.isAbandoned = true;

    for (const [rsPath, putRecord] of this.queue) {
      if (!putRecord.inFlight) {
        this.queue.delete(rsPath);
      }
    }
  }
}
