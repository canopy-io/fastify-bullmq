import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { FastifyAdapter } from '@bull-board/fastify';
import fastify, { FastifyInstance, FastifyRequest } from 'fastify';
import { Server, IncomingMessage, ServerResponse } from 'http';
import { env } from './env';

import { createQueue, setupQueueProcessor } from './queue';
import { TaskDetector } from './task-detector';
import { registerMonitoringRoutes } from './monitoring-routes';

interface AddJobQueryString {
  id: string;
  email: string;
}

const run = async () => {
  const advanceEvents = createQueue('advance-events');
  const referralEvents = createQueue('referral-events');
  const doznEvents = createQueue('dozn-events');

  // Week 1 PoC: 작업 감지 시스템 초기화
  const detector = new TaskDetector({
    host: env.REDISHOST,
    port: env.REDISPORT,
    username: env.REDISUSER,
    password: env.REDISPASSWORD,
  });

  // 모든 큐 감시 시작
  await Promise.all([
    detector.watchQueue(advanceEvents),
    detector.watchQueue(referralEvents),
    detector.watchQueue(doznEvents),
  ]);

  const server: FastifyInstance<Server, IncomingMessage, ServerResponse> =
    fastify();

  const serverAdapter = new FastifyAdapter();
  createBullBoard({
    queues: [new BullMQAdapter(advanceEvents), new BullMQAdapter(referralEvents), new BullMQAdapter(doznEvents)],
    serverAdapter,
  });
  serverAdapter.setBasePath('/');
  server.register(serverAdapter.registerPlugin(), {
    prefix: '/',
    basePath: '/',
  });

  // Week 1 PoC: 모니터링 라우트 등록
  registerMonitoringRoutes(server, detector);

  server.get(
    '/add-job',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            title: { type: 'string' },
            id: { type: 'string' },
          },
        },
      },
    },
    (req: FastifyRequest<{ Querystring: AddJobQueryString }>, reply) => {
      if (
        req.query == null ||
        req.query.email == null ||
        req.query.id == null
      ) {
        reply
          .status(400)
          .send({ error: 'Requests must contain both an id and a email' });

        return;
      }

      reply.send({
        ok: true,
      });
    }
  );

  await server.listen({ port: env.PORT, host: '0.0.0.0' });
  console.log(`
🚀 서버 시작: http://localhost:${env.PORT}

📊 Week 1 PoC - 작업 감지 시스템 활성화
   - GET  /detection/stats   : 감지 통계
   - GET  /detection/log     : 감지 로그
   - GET  /detection/active  : 활성 작업
   - GET  /detection/stream  : 실시간 스트림 (SSE)
   - GET  /detection/health  : 시스템 상태
   - POST /detection/test    : 테스트 이벤트

📝 테스트:
   curl https://${env.RAILWAY_STATIC_URL}/add-job?id=1&email=hello%40world.com
   curl https://${env.RAILWAY_STATIC_URL}/detection/stats
  `);

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\n🔄 종료 중...');
    await detector.close();
    await server.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
};

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
