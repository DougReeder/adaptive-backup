#!/usr/bin/env node

import {program} from "commander";
import pkg from "./package.json"  with { type: "json" };
import process from "node:process";
import colors from "colors";
import prompt from "prompt";
import addQueryParamsToURL from "./addQueryParamsToURL.js"
import WebFinger from "webfinger.js";
import opener from "opener";
import os from "node:os";
import path from 'node:path';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import encodePath from './encodePath.js';

program
    .version(pkg.version)
    .requiredOption('-o, --backup-dir <path>', 'backup directory path')
    .option('-u, --user-address <user address>', 'user address (user@host)')
    .option('-t, --token <token>', 'valid bearer token')
    .option('-c, --category <category>', 'category (base directory) to back up')
    .option('-p, --include-public', 'when backing up a single category, include the public folder of that category')
    .option('-s, --simultaneous <limit>', 'number of simultaneous connections')

program.parse(process.argv);
const options = program.opts();
console.log("options:", options);

const CLIENT_ID = 'adaptivebackup.hominidsoftware.com';
const ORIGIN = 'https://' + CLIENT_ID;
const DEFAULT_DELAY = 1500;
const backupDir     = options.backupDir;
const category      = options.category || '';
const includePublic = options.includePublic || false;
const simultaneous  = options.simultaneous || 3;
const authScope     = category.length > 0 ? category+':rw' : '*:rw';
const MAX_TRIES     = 3;

let token           = options.token;
let storageEndpoint;
let queue;   // keys are remoteStorage paths
let pausePrms       = Promise.resolve(true);   // initially not paused


const schemas = {
  userAddress: {
    name: 'userAddress',
    description: 'User address (user@host):',
    type: 'string',
    pattern: /^.+@.+$/,
    message: 'Please provide a valid user address. Example: tony@5apps.com',
    required: true
  },
  token: {
    name: 'token',
    description: 'Authorization token:',
    type: 'string',
    required: true
  }
};

const webfinger = new WebFinger({
  tls_only: true,   // Security-first: HTTPS only
  uri_fallback: false,
  // request_timeout: 10000  // 10 second timeout
});

// start
prompt.message = '';
prompt.delimiter = '';
prompt.override = program;
prompt.start();

let userAddress = options.userAddress;
let authEndpoint;
do {
  try {
    if (!userAddress) {
      const userInput = await prompt.get(schemas.userAddress);
      userAddress = userInput.userAddress;
    }

    const wfResult = await webfinger.lookup(userAddress);
    const link = wfResult.object?.links?.find?.(l => "http://tools.ietf.org/id/draft-dejong-remotestorage" === l.rel)
    authEndpoint = link?.properties?.["auth-endpoint"] || link?.properties?.["http://tools.ietf.org/html/rfc6749#section-4.2"];
    storageEndpoint = '/' === link.href.slice(-1) ? link.href : link.href + '/';
  } catch (err) {
    console.error(colors.red(err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err));
    if ("canceled" === err.message) { process.exit(1); }
    userAddress = '';
  }
} while (!storageEndpoint);
console.debug(colors.gray('authorization endpoint:', authEndpoint));
console.debug(colors.gray('storage endpoint:', storageEndpoint));

if (!token) {
  console.info(colors.cyan('No auth token set via options. A browser window will open to connect your account.'));
  const authURL = addQueryParamsToURL(authEndpoint, {
    client_id: CLIENT_ID,
    redirect_uri: ORIGIN + '/',
    response_type: 'token',
    scope: authScope
  });
  opener(authURL);

  const tokenResult = await prompt.get(schemas.token);
  token = tokenResult.token;
}

try {
  await executeBackup();
} catch (err) {
  console.error(colors.red(err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err));
}


async function executeBackup() {
  const trashed = path.join(os.tmpdir(), path.basename(backupDir) + Date.now());
  try {
    await rename(backupDir, trashed);
    console.info(`Moved old ${backupDir} to ${trashed}`);
  } catch (err) {
    if ('ENOENT' !== err.code) { throw err; }
  }

  console.info(`Starting backup of ${category ? "“"+category+"”" : "all categories"}`);
  queue = new Map();
  const initialFolder = '/' === category.slice(-1) ? category : category + '/';
  enqueue(initialFolder);
  if (includePublic && category) {
    enqueue(`public/${initialFolder}`);
  }
  // Doesn't impose any delay here, since no content fetches have started.
  await checkFetch();
}

function enqueue(rsPath) {
  if (!queue.has(rsPath)) {
    queue.set(rsPath, {inFlight: false, tries: 0});
    console.debug(colors.gray(`Enqueued ${rsPath}`));
  } else {
    console.debug(colors.gray(`${rsPath} was already in queue`));
  }
}

async function checkFetch() {
  let numInFlight = 0;
  let nextPath;
  for (const [rsPath, fetchRecord] of queue) {
    // console.log(`checking ${rsPath}: ${JSON.stringify(fetchRecord)}`);
    if (fetchRecord.inFlight) {
      if (++numInFlight >= simultaneous && nextPath) {
        // We know we can't launch another, so the exact number is unimportant.
        break;
      }
    } else {
      if (!nextPath) {
        nextPath = rsPath;
      }
    }
  }
  console.debug(colors.gray(`${numInFlight}/${queue.size} in flight`));

  if (nextPath) {
    if (numInFlight < simultaneous) {
      const fetchRecord = queue.get(nextPath);
      if (++fetchRecord.tries <= MAX_TRIES) {
        fetchRecord.inFlight = true;
        if ('/' === nextPath.slice(-1)) {
          fetchItem(nextPath, handleFolder).catch(console.error);
        } else {
          fetchItem(nextPath, handleDocument).catch(console.error);
        }
      } else {   // too many tries
        dequeue(nextPath);
      }

      if (++numInFlight < simultaneous) {
        // Caps the request rate at 1000/second
        setTimeout(checkFetch, 1);
      }
    } else {
      console.debug(colors.gray("connections are maxed-out"));
    }
  } else {
    console.debug(colors.gray("fetch started for all queued items"));
  }
}

async function fetchItem(rsPath, handle) {
  try {
    console.debug(colors.gray(`Fetching ${rsPath}`));
    const options = {
      headers: {
        "Authorization": `Bearer ${token}`,
        "User-Agent": `AdaptiveBackup/${program._version}`,
        "Origin": ORIGIN
      }
    };
    const res = await fetch(storageEndpoint + encodePath(rsPath), options);

    // queue.get(rsPath).inFlight = false;

    switch (res.status) {
      case 200:
        await handle(rsPath, res);
        break;
      case 429:
      case 503:
        const retryAfter = parseInt(res.headers.get('retry-after')) * 1000 || DEFAULT_DELAY;
        pausePrms = new Promise((res) => {
          setTimeout(res, retryAfter);
        });
        console.warn(colors.yellow(`pausing: ${res.statusText || res.status} ${rsPath}`))
        break;
      case 401:
      case 403:
        console.error(colors.red(`This token lacks permission to read ${rsPath}`));
        dequeue(rsPath);
        break;
      case 404:
      case 410:
        console.error(colors.red(`${res.statusText || res.status} ${rsPath} doesn't exist any more`));
        dequeue(rsPath);
        break;
      default:
        console.error(colors.red((res.statusText || res.status) + " " + rsPath));
        break;
    }
  } catch (err) {
    console.error(colors.red(rsPath + ":", err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err));
  } finally {
    // TODO: Should the request be aborted if we don't consume the body?
    const fetchRecord = queue.get(rsPath);
    if (fetchRecord) { fetchRecord.inFlight = false; }
    // imposes a slight delay to allow the connection to be closed,
    // and allows the queue to be updated
    setImmediate(checkFetch);
  }
}

async function handleFolder(folderPath, res) {
  const items = (await res.json()).items;
  console.debug(colors.gray((folderPath + ": " + JSON.stringify(items)).slice(0, 125)));

  const directory = path.join(backupDir, ...folderPath.split('/'));
  console.debug(colors.gray(`creating directory ${directory}`));
  await mkdir(directory, {recursive: true});

  for (const rsPath in items) {
    enqueue(folderPath + rsPath);
  }
  dequeue(folderPath);
}

async function handleDocument(rsPath, res) {
  const filePath = path.join(backupDir, ...rsPath.split('/'));
  console.debug(colors.gray(`writing ${rsPath} to ${backupDir}`));
  await writeFile(filePath, res.body);
  console.debug(colors.gray(`wrote ${rsPath} to ${backupDir}`));
  dequeue(rsPath);
}

function dequeue(rsPath) {
  queue.delete(rsPath);
  console.debug(colors.gray(`Dequeued ${rsPath}`));
  if (queue.size === 0) {
    console.info(colors.green(`Backup completed`));
    process.exit(0);
  }
}
