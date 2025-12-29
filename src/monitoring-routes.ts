import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { TaskDetector, DetectionEvent, TaskFingerprint } from './task-detector';

/**
 * Week 1 Day 5-7: 실시간 모니터링 및 알림 엔드포인트
 *
 * 측정 지표:
 * - 하루에 몇 번 알림을 받았나?
 * - 알림이 실제로 유용했던 비율은?
 * - 수동 확인 횟수가 줄었나?
 */

export function registerMonitoringRoutes(
  server: FastifyInstance,
  detector: TaskDetector
): void {
  /**
   * GET /detection/stats
   * 감지 통계 조회
   */
  server.get('/detection/stats', async (_req, reply) => {
    const stats = detector.getStats();
    return reply.send({
      success: true,
      data: stats,
      timestamp: new Date().toISOString(),
    });
  });

  /**
   * GET /detection/log
   * 감지 로그 조회
   */
  server.get<{ Querystring: { limit?: string } }>(
    '/detection/log',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            limit: { type: 'string' },
          },
        },
      },
    },
    async (req, reply) => {
      const limit = parseInt(req.query.limit ?? '50', 10);
      const log = detector.getDetectionLog(limit);
      return reply.send({
        success: true,
        data: log,
        count: log.length,
        timestamp: new Date().toISOString(),
      });
    }
  );

  /**
   * GET /detection/active
   * 현재 활성 작업 조회
   */
  server.get('/detection/active', async (_req, reply) => {
    const activeJobs = detector.getActiveJobs();
    return reply.send({
      success: true,
      data: activeJobs,
      count: activeJobs.length,
      timestamp: new Date().toISOString(),
    });
  });

  /**
   * GET /detection/stream
   * Server-Sent Events로 실시간 감지 이벤트 스트리밍
   *
   * 사용 예:
   * curl -N http://localhost:3000/detection/stream
   */
  server.get('/detection/stream', async (req, reply) => {
    reply.raw.setHeader('Content-Type', 'text/event-stream');
    reply.raw.setHeader('Cache-Control', 'no-cache');
    reply.raw.setHeader('Connection', 'keep-alive');
    reply.raw.setHeader('Access-Control-Allow-Origin', '*');

    // 초기 연결 메시지
    reply.raw.write(`data: ${JSON.stringify({ type: 'connected', message: '실시간 감지 스트림 연결됨' })}\n\n`);

    // 감지 이벤트 리스너
    const onDetection = (event: DetectionEvent) => {
      reply.raw.write(`data: ${JSON.stringify(event)}\n\n`);
    };

    // 패턴 매치 이벤트 리스너
    const onPatternMatch = (data: { pattern: string; fingerprint: TaskFingerprint; message: string }) => {
      reply.raw.write(`data: ${JSON.stringify({ type: 'pattern-match', ...data })}\n\n`);
    };

    detector.on('detection', onDetection);
    detector.on('pattern-match', onPatternMatch);

    // 연결 종료 처리
    req.raw.on('close', () => {
      detector.off('detection', onDetection);
      detector.off('pattern-match', onPatternMatch);
    });

    // 응답을 열린 상태로 유지
    await new Promise(() => {});
  });

  /**
   * GET /detection/health
   * 감지 시스템 상태 확인
   */
  server.get('/detection/health', async (_req, reply) => {
    const stats = detector.getStats();
    return reply.send({
      success: true,
      status: 'healthy',
      uptime: process.uptime(),
      monitoring: {
        queues: Object.keys(stats.byQueue).length,
        activeJobs: stats.activeJobs,
        totalDetections: stats.totalDetections,
      },
      timestamp: new Date().toISOString(),
    });
  });

  /**
   * POST /detection/test
   * 테스트용 감지 이벤트 발생 (개발/디버깅용)
   */
  server.post<{ Body: { queueName?: string; status?: string } }>(
    '/detection/test',
    {
      schema: {
        body: {
          type: 'object',
          properties: {
            queueName: { type: 'string' },
            status: { type: 'string' },
          },
        },
      },
    },
    async (req, reply) => {
      const testEvent: DetectionEvent = {
        type: 'completed',
        fingerprint: {
          jobId: `test-${Date.now()}`,
          queueName: req.body?.queueName ?? 'test-queue',
          status: (req.body?.status as any) ?? 'completed',
          timestamp: Date.now(),
          duration: Math.floor(Math.random() * 5000),
        },
        message: '🧪 테스트 감지 이벤트',
      };

      detector.emit('detection', testEvent);

      return reply.send({
        success: true,
        message: '테스트 이벤트 발생',
        event: testEvent,
      });
    }
  );

  console.log('📡 모니터링 라우트 등록 완료');
  console.log('  - GET  /detection/stats   : 감지 통계');
  console.log('  - GET  /detection/log     : 감지 로그');
  console.log('  - GET  /detection/active  : 활성 작업');
  console.log('  - GET  /detection/stream  : 실시간 스트림 (SSE)');
  console.log('  - GET  /detection/health  : 시스템 상태');
  console.log('  - POST /detection/test    : 테스트 이벤트');
}
