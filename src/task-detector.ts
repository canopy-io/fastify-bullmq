import { Queue, QueueEvents, Job } from 'bullmq';
import { EventEmitter } from 'events';

/**
 * Week 1 PoC: 작업 감지 시스템
 *
 * 핵심 검증 가설:
 * 1. "작업 지문"이 실제로 작동하는가? (기술 타당성)
 * 2. 자동 감지가 수동 확인보다 빠른가? (사용자 가치)
 */

// 작업 상태 타입
export type TaskStatus = 'waiting' | 'active' | 'completed' | 'failed' | 'delayed' | 'paused';

// 작업 지문 (Task Fingerprint)
export interface TaskFingerprint {
  jobId: string;
  queueName: string;
  status: TaskStatus;
  timestamp: number;
  duration?: number;
  progress?: number;
  returnValue?: unknown;
  failedReason?: string;
}

// 감지 이벤트
export interface DetectionEvent {
  type: 'started' | 'progress' | 'completed' | 'failed';
  fingerprint: TaskFingerprint;
  message: string;
}

// 패턴 매칭 규칙
export interface DetectionPattern {
  name: string;
  match: (fingerprint: TaskFingerprint) => boolean;
  priority: 'low' | 'medium' | 'high' | 'critical';
  message: (fingerprint: TaskFingerprint) => string;
}

/**
 * TaskDetector: BullMQ 작업 상태 자동 감지기
 *
 * Day 1-2 구현: Accessibility API 대신 BullMQ 이벤트 시스템 활용
 */
export class TaskDetector extends EventEmitter {
  private queueEvents: Map<string, QueueEvents> = new Map();
  private activeJobs: Map<string, TaskFingerprint> = new Map();
  private detectionLog: DetectionEvent[] = [];
  private patterns: DetectionPattern[] = [];
  private connectionOptions: { host: string; port: number; username?: string; password?: string };

  constructor(connectionOptions: { host: string; port: number; username?: string; password?: string }) {
    super();
    this.connectionOptions = connectionOptions;
    this.initializeDefaultPatterns();
  }

  /**
   * Day 3-4: 기본 패턴 매칭 규칙 설정
   */
  private initializeDefaultPatterns(): void {
    this.patterns = [
      {
        name: 'quick-success',
        match: (fp) => fp.status === 'completed' && (fp.duration ?? 0) < 1000,
        priority: 'low',
        message: (fp) => `⚡ 빠른 완료: ${fp.queueName}/${fp.jobId} (${fp.duration}ms)`,
      },
      {
        name: 'long-running',
        match: (fp) => fp.status === 'active' && (fp.duration ?? 0) > 30000,
        priority: 'medium',
        message: (fp) => `⏳ 장시간 실행 중: ${fp.queueName}/${fp.jobId} (${Math.round((fp.duration ?? 0) / 1000)}s)`,
      },
      {
        name: 'completed',
        match: (fp) => fp.status === 'completed',
        priority: 'medium',
        message: (fp) => `✅ 작업 완료: ${fp.queueName}/${fp.jobId}`,
      },
      {
        name: 'failed',
        match: (fp) => fp.status === 'failed',
        priority: 'critical',
        message: (fp) => `❌ 작업 실패: ${fp.queueName}/${fp.jobId} - ${fp.failedReason}`,
      },
      {
        name: 'high-progress',
        match: (fp) => fp.status === 'active' && (fp.progress ?? 0) >= 90,
        priority: 'low',
        message: (fp) => `🔄 거의 완료: ${fp.queueName}/${fp.jobId} (${fp.progress}%)`,
      },
    ];
  }

  /**
   * 큐 감시 시작
   */
  async watchQueue(queue: Queue): Promise<void> {
    const queueName = queue.name;

    if (this.queueEvents.has(queueName)) {
      return; // 이미 감시 중
    }

    const queueEvents = new QueueEvents(queueName, {
      connection: this.connectionOptions,
    });

    await queueEvents.waitUntilReady();
    this.queueEvents.set(queueName, queueEvents);

    // 작업 시작 감지
    queueEvents.on('active', async ({ jobId, prev }) => {
      const fingerprint = this.createFingerprint(jobId, queueName, 'active');
      this.activeJobs.set(`${queueName}:${jobId}`, fingerprint);
      this.emitDetection('started', fingerprint);
    });

    // 진행률 감지
    queueEvents.on('progress', async ({ jobId, data }) => {
      const key = `${queueName}:${jobId}`;
      const existing = this.activeJobs.get(key);
      if (existing) {
        existing.progress = typeof data === 'number' ? data : (data as any)?.progress ?? 0;
        existing.duration = Date.now() - existing.timestamp;
        this.checkPatterns(existing);
      }
    });

    // 완료 감지
    queueEvents.on('completed', async ({ jobId, returnvalue }) => {
      const key = `${queueName}:${jobId}`;
      const existing = this.activeJobs.get(key);
      const fingerprint: TaskFingerprint = {
        ...(existing ?? this.createFingerprint(jobId, queueName, 'completed')),
        status: 'completed',
        duration: existing ? Date.now() - existing.timestamp : 0,
        returnValue: returnvalue,
      };
      this.activeJobs.delete(key);
      this.emitDetection('completed', fingerprint);
    });

    // 실패 감지
    queueEvents.on('failed', async ({ jobId, failedReason }) => {
      const key = `${queueName}:${jobId}`;
      const existing = this.activeJobs.get(key);
      const fingerprint: TaskFingerprint = {
        ...(existing ?? this.createFingerprint(jobId, queueName, 'failed')),
        status: 'failed',
        duration: existing ? Date.now() - existing.timestamp : 0,
        failedReason,
      };
      this.activeJobs.delete(key);
      this.emitDetection('failed', fingerprint);
    });

    console.log(`👁️ 큐 감시 시작: ${queueName}`);
  }

  /**
   * 작업 지문 생성
   */
  private createFingerprint(jobId: string, queueName: string, status: TaskStatus): TaskFingerprint {
    return {
      jobId,
      queueName,
      status,
      timestamp: Date.now(),
    };
  }

  /**
   * 감지 이벤트 발생
   */
  private emitDetection(type: DetectionEvent['type'], fingerprint: TaskFingerprint): void {
    const matchedPattern = this.findMatchingPattern(fingerprint);
    const message = matchedPattern?.message(fingerprint) ?? this.getDefaultMessage(type, fingerprint);

    const event: DetectionEvent = {
      type,
      fingerprint,
      message,
    };

    this.detectionLog.push(event);

    // 로그 크기 제한 (최근 1000개만 유지)
    if (this.detectionLog.length > 1000) {
      this.detectionLog = this.detectionLog.slice(-1000);
    }

    this.emit('detection', event);
    console.log(`[TaskDetector] ${message}`);
  }

  /**
   * 패턴 체크
   */
  private checkPatterns(fingerprint: TaskFingerprint): void {
    const matchedPattern = this.findMatchingPattern(fingerprint);
    if (matchedPattern && matchedPattern.priority !== 'low') {
      this.emit('pattern-match', {
        pattern: matchedPattern.name,
        fingerprint,
        message: matchedPattern.message(fingerprint),
      });
    }
  }

  /**
   * 매칭되는 패턴 찾기
   */
  private findMatchingPattern(fingerprint: TaskFingerprint): DetectionPattern | undefined {
    // 우선순위 순으로 정렬하여 가장 높은 우선순위 패턴 반환
    const priorityOrder = { critical: 0, high: 1, medium: 2, low: 3 };
    const sorted = [...this.patterns].sort(
      (a, b) => priorityOrder[a.priority] - priorityOrder[b.priority]
    );
    return sorted.find((p) => p.match(fingerprint));
  }

  /**
   * 기본 메시지 생성
   */
  private getDefaultMessage(type: DetectionEvent['type'], fingerprint: TaskFingerprint): string {
    const messages: Record<DetectionEvent['type'], string> = {
      started: `🚀 작업 시작: ${fingerprint.queueName}/${fingerprint.jobId}`,
      progress: `🔄 진행 중: ${fingerprint.queueName}/${fingerprint.jobId} (${fingerprint.progress}%)`,
      completed: `✅ 작업 완료: ${fingerprint.queueName}/${fingerprint.jobId}`,
      failed: `❌ 작업 실패: ${fingerprint.queueName}/${fingerprint.jobId}`,
    };
    return messages[type];
  }

  /**
   * 커스텀 패턴 추가
   */
  addPattern(pattern: DetectionPattern): void {
    this.patterns.push(pattern);
  }

  /**
   * 감지 로그 조회
   */
  getDetectionLog(limit: number = 100): DetectionEvent[] {
    return this.detectionLog.slice(-limit);
  }

  /**
   * 현재 활성 작업 조회
   */
  getActiveJobs(): TaskFingerprint[] {
    return Array.from(this.activeJobs.values());
  }

  /**
   * 통계 조회
   */
  getStats(): {
    totalDetections: number;
    activeJobs: number;
    byType: Record<DetectionEvent['type'], number>;
    byQueue: Record<string, number>;
  } {
    const byType: Record<DetectionEvent['type'], number> = {
      started: 0,
      progress: 0,
      completed: 0,
      failed: 0,
    };
    const byQueue: Record<string, number> = {};

    for (const event of this.detectionLog) {
      byType[event.type]++;
      const queueName = event.fingerprint.queueName;
      byQueue[queueName] = (byQueue[queueName] ?? 0) + 1;
    }

    return {
      totalDetections: this.detectionLog.length,
      activeJobs: this.activeJobs.size,
      byType,
      byQueue,
    };
  }

  /**
   * 리소스 정리
   */
  async close(): Promise<void> {
    for (const [name, queueEvents] of this.queueEvents) {
      await queueEvents.close();
      console.log(`👁️ 큐 감시 종료: ${name}`);
    }
    this.queueEvents.clear();
    this.activeJobs.clear();
  }
}
