<?php

declare(strict_types=1);

use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Lettermint\RabbitMQ\Attributes\ConsumesQueue;
use Lettermint\RabbitMQ\Connection\ChannelManager;
use Lettermint\RabbitMQ\Consumers\Consumer;
use Lettermint\RabbitMQ\Discovery\AttributeScanner;
use Lettermint\RabbitMQ\Queue\RabbitMQJob;
use Lettermint\RabbitMQ\Queue\RabbitMQQueue;
use Mockery\MockInterface;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Build a Consumer wired to mocked collaborators (used by the multi-queue tests).
 *
 * @return array{0: Consumer, 1: MockInterface}
 */
function makeConsumer(): array
{
    // heartbeat 0 disables the PCNTL heartbeat sender path
    $connection = mockAMQPConnection(heartbeat: 0);
    $channel = mockAMQPChannel($connection);

    // Empty queue: wait() times out immediately so the consume loop can exit
    // once stopWhenEmpty is set.
    $channel->shouldReceive('wait')->andThrow(new AMQPTimeoutException('timeout'));

    $channelManager = Mockery::mock(ChannelManager::class);
    $channelManager->shouldReceive('consumeChannel')->andReturn($channel);
    $channelManager->shouldReceive('getConnection')->andReturn($connection);

    $events = Mockery::mock(Dispatcher::class);
    $events->shouldReceive('dispatch')->andReturnNull();

    $consumer = new Consumer(
        $channelManager,
        Mockery::mock(AttributeScanner::class),
        Mockery::mock(RabbitMQQueue::class),
        Mockery::mock(ExceptionHandler::class),
        $events,
    );

    return [$consumer, $channel];
}

describe('multi-queue consumption', function () {
    it('registers one consumer per queue', function () {
        [$consumer, $channel] = makeConsumer();

        $channel->shouldReceive('basic_consume')
            ->withArgs(fn ($queue) => $queue === 'queue-a')
            ->once()
            ->andReturn('tag-a');

        $channel->shouldReceive('basic_consume')
            ->withArgs(fn ($queue) => $queue === 'queue-b')
            ->once()
            ->andReturn('tag-b');

        $consumer->setQueues(['queue-a', 'queue-b'])
            ->setStopWhenEmpty(true)
            ->consume();

        // Expectations verified on teardown by Mockery.
        expect(true)->toBeTrue();
    });

    it('cancels every registered consumer on cleanup', function () {
        [$consumer, $channel] = makeConsumer();

        $channel->shouldReceive('basic_consume')->andReturn('tag-a', 'tag-b');
        $channel->shouldReceive('basic_cancel')->with('tag-a')->once();
        $channel->shouldReceive('basic_cancel')->with('tag-b')->once();

        $consumer->setQueues(['queue-a', 'queue-b'])
            ->setStopWhenEmpty(true)
            ->consume();

        expect(true)->toBeTrue();
    });

    it('still supports a single queue via setQueue', function () {
        [$consumer, $channel] = makeConsumer();

        $channel->shouldReceive('basic_consume')
            ->withArgs(fn ($queue) => $queue === 'solo')
            ->once()
            ->andReturn('tag-solo');

        $consumer->setQueue('solo')
            ->setStopWhenEmpty(true)
            ->consume();

        expect(true)->toBeTrue();
    });

    it('tags each job with the queue it was received from', function () {
        $consumer = new class(Mockery::mock(ChannelManager::class), Mockery::mock(AttributeScanner::class), Mockery::mock(RabbitMQQueue::class), Mockery::mock(ExceptionHandler::class), Mockery::mock(Dispatcher::class)) extends Consumer
        {
            /** @var list<string> */
            public array $handledQueues = [];

            public function dispatchTo(AMQPMessage $message, string $queue): void
            {
                $this->handleMessage($message, $queue);
            }

            protected function processJob(RabbitMQJob $job): void
            {
                $this->handledQueues[] = $job->getQueue();
            }
        };

        $consumer->dispatchTo(mockAMQPMessage(), 'queue-b');
        $consumer->dispatchTo(mockAMQPMessage(), 'queue-a');

        expect($consumer->handledQueues)->toBe(['queue-b', 'queue-a']);
    });
});

/**
 * A Consumer subclass exposing the protected retry-decision seams, wired in
 * tests to a real RabbitMQQueue so reject/publish calls land on the mock channel.
 */
class RequeueProbeConsumer extends Consumer
{
    public function callHandleJobException(RabbitMQJob $job, Throwable $e): void
    {
        $this->handleJobException($job, $e);
    }

    public function callRequeuePreservesAttempts(RabbitMQJob $job): bool
    {
        return $this->requeuePreservesAttempts($job);
    }
}

/**
 * Build a RequeueProbeConsumer with a real RabbitMQQueue over a mock channel
 * and the given scanner.
 *
 * @return array{0: RequeueProbeConsumer, 1: MockInterface, 2: RabbitMQQueue}
 */
function makeRequeueConsumer(MockInterface $scanner): array
{
    $connection = mockAMQPConnection(heartbeat: 0);
    $channel = mockAMQPChannel($connection);

    $channelManager = Mockery::mock(ChannelManager::class);
    $channelManager->shouldReceive('consumeChannel')->andReturn($channel);
    $channelManager->shouldReceive('publishChannel')->andReturn($channel);
    $channelManager->shouldReceive('getConnection')->andReturn($connection);

    $rabbitmq = new RabbitMQQueue($channelManager, $scanner, []);
    $rabbitmq->setContainer(new Container);

    $events = Mockery::mock(Dispatcher::class);
    $events->shouldReceive('dispatch')->andReturnNull();

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldReceive('report')->andReturnNull();

    $consumer = new RequeueProbeConsumer($channelManager, $scanner, $rabbitmq, $exceptions, $events);

    return [$consumer, $channel, $rabbitmq];
}

/**
 * Wrap a mock message as a RabbitMQJob on the given queue and real transport.
 */
function requeueJob(RabbitMQQueue $rabbitmq, MockInterface $channel, MockInterface $message, string $queue): RabbitMQJob
{
    return new RabbitMQJob(new Container, $rabbitmq, $channel, $message, 'rabbitmq', $queue);
}

function quorumQueueAttribute(): ConsumesQueue
{
    return new ConsumesQueue(
        queue: 'events:ordered',
        bindings: ['app' => 'events.*'],
        quorum: true,
    );
}

function classicQueueAttribute(): ConsumesQueue
{
    return new ConsumesQueue(
        queue: 'events:classic',
        bindings: ['app' => 'events.*'],
        quorum: false,
    );
}

describe('failure handling', function () {
    it('requeues a failed job on a quorum queue so the delivery counter advances', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')
            ->with('events:ordered')
            ->andReturn(quorumQueueAttribute());

        [$consumer, $channel, $rabbitmq] = makeRequeueConsumer($scanner);

        // First delivery (attempts = 1 < tries), so the retry branch is taken.
        $message = mockAMQPMessage(['deliveryTag' => 7, 'headers' => []]);

        // Reject WITH requeue (same message, x-delivery-count preserved); no
        // fresh re-publish to the tail.
        $channel->shouldReceive('basic_reject')->once()->with(7, true)->andReturnNull();
        $channel->shouldNotReceive('basic_publish');
        $channel->shouldNotReceive('tx_select');

        $consumer->callHandleJobException(
            requeueJob($rabbitmq, $channel, $message, 'events:ordered'),
            new RuntimeException('transient failure'),
        );

        expect(true)->toBeTrue();
    });

    it('re-publishes a failed job on a classic queue (no delivery counter)', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')
            ->with('events:classic')
            ->andReturn(classicQueueAttribute());
        // releaseWithException re-publishes via the attribute lookup for routing.
        $scanner->shouldReceive('getQueueForJob')->andReturn(classicQueueAttribute());

        [$consumer, $channel, $rabbitmq] = makeRequeueConsumer($scanner);

        $message = mockAMQPMessage(['deliveryTag' => 9, 'headers' => []]);

        // ack + re-publish inside a transaction, no requeue.
        $channel->shouldReceive('tx_select')->once();
        $channel->shouldReceive('basic_publish')->once();
        $channel->shouldReceive('basic_ack')->once();
        $channel->shouldReceive('tx_commit')->once();
        $channel->shouldNotReceive('basic_reject');

        $consumer->callHandleJobException(
            requeueJob($rabbitmq, $channel, $message, 'events:classic'),
            new RuntimeException('transient failure'),
        );

        expect(true)->toBeTrue();
    });

    it('parks a job that reached the tries cap to the DLX (reject, no requeue)', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        // At the cap the failJob branch runs before any requeue decision, so
        // getAttributeForQueue is never consulted.
        $scanner->shouldReceive('getAttributeForQueue')->never();

        [$consumer, $channel, $rabbitmq] = makeRequeueConsumer($scanner);

        // x-delivery-count 3 => attempts 4 >= default tries 3 => terminal fail.
        $message = mockAMQPMessage(['deliveryTag' => 5, 'headers' => ['x-delivery-count' => 3]]);

        // Terminal park: reject WITHOUT requeue so the broker dead-letters it.
        $channel->shouldReceive('basic_reject')->once()->with(5, false)->andReturnNull();

        $consumer->callHandleJobException(
            requeueJob($rabbitmq, $channel, $message, 'events:ordered'),
            new RuntimeException('poison'),
        );

        expect(true)->toBeTrue();
    });
});

describe('requeuePreservesAttempts', function () {
    it('is true for a quorum queue (x-delivery-count is maintained)', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')->with('events:ordered')->andReturn(quorumQueueAttribute());

        [$consumer, $channel, $rabbitmq] = makeRequeueConsumer($scanner);
        $job = requeueJob($rabbitmq, $channel, mockAMQPMessage(), 'events:ordered');

        expect($consumer->callRequeuePreservesAttempts($job))->toBeTrue();
    });

    it('is false for a classic queue (no delivery counter, would churn)', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')->with('events:classic')->andReturn(classicQueueAttribute());

        [$consumer, $channel, $rabbitmq] = makeRequeueConsumer($scanner);
        $job = requeueJob($rabbitmq, $channel, mockAMQPMessage(), 'events:classic');

        expect($consumer->callRequeuePreservesAttempts($job))->toBeFalse();
    });

    it('is false when the queue has no discovered attribute', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')->with('mystery')->andReturnNull();

        [$consumer, $channel, $rabbitmq] = makeRequeueConsumer($scanner);
        $job = requeueJob($rabbitmq, $channel, mockAMQPMessage(), 'mystery');

        expect($consumer->callRequeuePreservesAttempts($job))->toBeFalse();
    });
});
