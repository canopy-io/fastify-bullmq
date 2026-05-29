import { createHash, timingSafeEqual } from 'node:crypto';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { FastifyAdapter } from '@bull-board/fastify';
import awsLambdaFastify from '@fastify/aws-lambda';
import { Queue } from 'bullmq';
import fastify from 'fastify';

const READY_CHECK_TIMEOUT_MS = 5000;
const AUTH_CACHE_TTL_MS = 5 * 60 * 1000;
const BUNDLED_UI_BASE_PATH = fileURLToPath(
  new URL('./bull-board-ui', import.meta.url)
);
const SOURCE_UI_BASE_PATH = fileURLToPath(
  new URL('../node_modules/@bull-board/ui', import.meta.url)
);

interface RedisSecret {
  host?: string;
  port?: string | number;
  username?: string;
  user?: string;
  password?: string;
  tls?: boolean;
}

interface DashboardAuth {
  username: string;
  password: string;
}

interface BuildAppOptions {
  env?: NodeJS.ProcessEnv;
  redisSecret: RedisSecret;
  getAuth?: () => Promise<DashboardAuth>;
}

const secretsManager = new SecretsManagerClient({});

let cachedRedisSecretPromise: Promise<RedisSecret> | undefined;
let cachedAuthPromise: Promise<DashboardAuth> | undefined;
let cachedAuthExpiresAt = 0;
let cachedHandlerPromise:
  | Promise<ReturnType<typeof awsLambdaFastify>>
  | undefined;

export function parseQueueNames(value?: string) {
  const names = String(value || '')
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean);

  if (names.length === 0) {
    throw new Error('QUEUE_NAMES is required');
  }

  return names;
}

export function buildRedisConnection(secret: RedisSecret) {
  const host = secret.host;
  const rawPort = secret.port || 6379;
  const port = Number(rawPort);
  const username = secret.username || secret.user || undefined;
  const password = secret.password;
  const tlsEnabled = secret.tls !== false;

  if (!host) {
    throw new Error('Redis host is missing');
  }

  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`Invalid Redis port: ${rawPort}`);
  }

  return {
    host,
    port,
    ...(username ? { username } : {}),
    ...(password ? { password } : {}),
    ...(tlsEnabled ? { tls: {} } : {}),
    lazyConnect: true,
    enableOfflineQueue: false,
    connectTimeout: 5000,
    maxRetriesPerRequest: null,
    retryStrategy: () => null,
  };
}

function sha256(value: string) {
  return createHash('sha256').update(value).digest();
}

function constantTimeEquals(actual: string, expected: string) {
  return timingSafeEqual(sha256(actual), sha256(expected));
}

export function parseBasicAuth(header?: string) {
  if (!header) return undefined;

  const [scheme, credentials] = String(header).split(' ');
  if (scheme?.toLowerCase() !== 'basic' || !credentials) return undefined;

  const decoded = Buffer.from(credentials, 'base64').toString('utf8');
  const separatorIndex = decoded.indexOf(':');
  if (separatorIndex < 0) return undefined;

  return {
    username: decoded.slice(0, separatorIndex),
    password: decoded.slice(separatorIndex + 1),
  };
}

export function requestNeedsAuth(method: string, url = '') {
  if (String(method || '').toUpperCase() === 'OPTIONS') return false;

  const path =
    String(url || '')
      .split('?')[0]
      .replace(/\/+$/, '') || '/';
  return path !== '/health';
}

export function isMutationMethod(method: string) {
  return !['GET', 'HEAD', 'OPTIONS'].includes(
    String(method || '').toUpperCase()
  );
}

export function isAuthorized(
  authorization: string | undefined,
  auth: DashboardAuth
) {
  const credentials = parseBasicAuth(authorization);
  if (!credentials || !auth?.username || !auth?.password) return false;

  return (
    constantTimeEquals(credentials.username, auth.username) &&
    constantTimeEquals(credentials.password, auth.password)
  );
}

function sendAuthChallenge(reply: any) {
  return reply
    .header('WWW-Authenticate', 'Basic realm="Canopy Queue Dashboard"')
    .code(401)
    .send({ error: 'Unauthorized' });
}

function getUiBasePath() {
  if (existsSync(BUNDLED_UI_BASE_PATH)) return BUNDLED_UI_BASE_PATH;
  if (existsSync(SOURCE_UI_BASE_PATH)) return SOURCE_UI_BASE_PATH;

  return BUNDLED_UI_BASE_PATH;
}

function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string
) {
  let timeout: ReturnType<typeof setTimeout>;
  const timeoutPromise = new Promise<T>((_, reject) => {
    timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
  });

  return Promise.race([promise, timeoutPromise]).finally(() =>
    clearTimeout(timeout)
  );
}

async function getSecretString(secretId: string, description: string) {
  const response = await secretsManager.send(
    new GetSecretValueCommand({
      SecretId: secretId,
    })
  );

  if (!response.SecretString) {
    throw new Error(`${description} does not contain SecretString`);
  }

  return response.SecretString;
}

export async function loadRedisSecret(env = process.env): Promise<RedisSecret> {
  if (!env.REDIS_SECRET_ARN) {
    throw new Error('REDIS_SECRET_ARN is required');
  }

  return JSON.parse(
    await getSecretString(env.REDIS_SECRET_ARN, 'Redis secret')
  );
}

export async function loadDashboardAuth(
  env = process.env
): Promise<DashboardAuth> {
  if (!env.QUEUE_DASHBOARD_AUTH_SECRET_ARN) {
    throw new Error('QUEUE_DASHBOARD_AUTH_SECRET_ARN is required');
  }

  const secretString = await getSecretString(
    env.QUEUE_DASHBOARD_AUTH_SECRET_ARN,
    'Queue dashboard auth secret'
  );
  const secret = JSON.parse(secretString);

  if (!secret.username || !secret.password) {
    throw new Error(
      'Queue dashboard auth secret requires username and password'
    );
  }

  return {
    username: secret.username,
    password: secret.password,
  };
}

async function getRedisSecret() {
  if (!cachedRedisSecretPromise) {
    cachedRedisSecretPromise = loadRedisSecret().catch((error) => {
      cachedRedisSecretPromise = undefined;
      throw error;
    });
  }

  return cachedRedisSecretPromise;
}

async function getDashboardAuth() {
  const now = Date.now();

  if (!cachedAuthPromise || now >= cachedAuthExpiresAt) {
    cachedAuthExpiresAt = now + AUTH_CACHE_TTL_MS;
    cachedAuthPromise = loadDashboardAuth().catch((error) => {
      cachedAuthPromise = undefined;
      cachedAuthExpiresAt = 0;
      throw error;
    });
  }

  return cachedAuthPromise;
}

export async function buildApp({
  env = process.env,
  redisSecret,
  getAuth = getDashboardAuth,
}: BuildAppOptions) {
  const app = fastify({
    logger: env.LOG_LEVEL === 'silent' ? false : true,
  });

  const queueNames = parseQueueNames(env.QUEUE_NAMES);
  const connection = buildRedisConnection(redisSecret);
  const serverAdapter = new FastifyAdapter();
  const queues = queueNames.map(
    (name) => new Queue(name, { connection, skipWaitingForReady: true } as any)
  );

  queues.forEach((queue) => {
    queue.on('error', (error) => {
      app.log.error(
        {
          err: error,
          queueName: queue.name,
        },
        'Queue dashboard Redis error'
      );
    });
  });

  app.addHook('onClose', async () => {
    await Promise.all(
      queues.map(async (queue) => {
        try {
          await queue.close();
        } catch (error) {
          app.log.warn(
            {
              err: error,
              queueName: queue.name,
            },
            'Failed to close queue dashboard Redis connection'
          );
          queue.disconnect();
        }
      })
    );
  });

  app.addHook('onRequest', async (request, reply) => {
    if (!requestNeedsAuth(request.method, request.url)) return;

    const auth = await getAuth();
    if (isAuthorized(request.headers.authorization, auth)) return;

    return sendAuthChallenge(reply);
  });

  app.addHook('onResponse', async (request, reply) => {
    if (!isMutationMethod(request.method)) return;

    const auth = await getAuth().catch(() => undefined);
    const credentials = parseBasicAuth(request.headers.authorization);
    const authenticated = auth
      ? isAuthorized(request.headers.authorization, auth)
      : false;

    request.log.info(
      {
        event: 'queue_dashboard_mutation',
        method: request.method,
        path: request.url.split('?')[0],
        statusCode: reply.statusCode,
        sourceIp: request.ip,
        requestId: request.id,
        authUsername: authenticated ? credentials?.username : undefined,
      },
      'Queue dashboard mutation request'
    );
  });

  createBullBoard({
    queues: queues.map((queue) => new BullMQAdapter(queue)),
    serverAdapter,
  });

  const uiBasePath = getUiBasePath();
  serverAdapter
    .setBasePath('/')
    .setViewsPath(join(uiBasePath, 'dist'))
    .setStaticPath('/static', join(uiBasePath, 'dist/static'));

  app.register(serverAdapter.registerPlugin(), {
    prefix: '/',
    basePath: '/',
  });

  app.get('/health', async () => ({ ok: true }));

  app.get('/ready', async (request, reply) => {
    const readinessQueue = new Queue(queueNames[0], {
      connection,
      skipWaitingForReady: true,
    } as any);

    try {
      const client = await withTimeout(
        readinessQueue.client,
        READY_CHECK_TIMEOUT_MS,
        'Timed out waiting for Redis client'
      );

      await withTimeout(
        client.ping(),
        READY_CHECK_TIMEOUT_MS,
        'Timed out waiting for Redis PING'
      );

      return { ok: true };
    } catch (error) {
      request.log.error(
        { err: error },
        'Queue dashboard readiness check failed'
      );
      return reply.code(503).send({ ok: false });
    } finally {
      try {
        await readinessQueue.close();
      } catch {
        readinessQueue.disconnect();
      }
    }
  });

  return app;
}

async function getLambdaHandler() {
  if (!cachedHandlerPromise) {
    cachedHandlerPromise = (async () => {
      const redisSecret = await getRedisSecret();
      const app = await buildApp({ redisSecret });
      const proxy = awsLambdaFastify(app, {
        callbackWaitsForEmptyEventLoop: false,
        decorateRequest: false,
      });
      await app.ready();

      return proxy;
    })();
  }

  return cachedHandlerPromise;
}

export const handler = async (event: any, context: any) => {
  context.callbackWaitsForEmptyEventLoop = false;

  if (
    event?.rawPath === '/health' &&
    event?.requestContext?.http?.method === 'GET'
  ) {
    return {
      statusCode: 200,
      headers: {
        'content-type': 'application/json; charset=utf-8',
      },
      isBase64Encoded: false,
      body: JSON.stringify({ ok: true }),
    };
  }

  const proxy = await getLambdaHandler();
  return proxy(event, context);
};
