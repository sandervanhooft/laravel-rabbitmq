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
 * A Consumer subclass exposing the protected retry-decision seams, wired in
 * tests to a real RabbitMQQueue so reject/publish calls land on the mock channel.
 */
class OrderRetryProbeConsumer extends Consumer
{
    public function callHandleJobException(RabbitMQJob $job, Throwable $e): void
    {
        $this->handleJobException($job, $e);
    }

    public function callShouldRetryInOrder(RabbitMQJob $job): bool
    {
        return $this->shouldRetryInOrder($job);
    }
}

/**
 * Build an OrderRetryProbeConsumer with a real RabbitMQQueue over a mock
 * channel and the given scanner.
 *
 * @return array{0: OrderRetryProbeConsumer, 1: MockInterface, 2: RabbitMQQueue}
 */
function makeOrderRetryConsumer(MockInterface $scanner): array
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

    $consumer = new OrderRetryProbeConsumer($channelManager, $scanner, $rabbitmq, $exceptions, $events);

    return [$consumer, $channel, $rabbitmq];
}

/**
 * Wrap a mock message as a RabbitMQJob on the given queue and real transport.
 */
function orderRetryJob(RabbitMQQueue $rabbitmq, MockInterface $channel, MockInterface $message, string $queue): RabbitMQJob
{
    return new RabbitMQJob(new Container, $rabbitmq, $channel, $message, 'rabbitmq', $queue);
}

function orderedShardAttribute(): ConsumesQueue
{
    return new ConsumesQueue(
        queue: 'ordered.shard.0',
        bindings: ['vatly.ordered' => 'ordered.shard.0'],
        quorum: true,
        prefetch: 1,
        singleActiveConsumer: true,
    );
}

function commutativeQueueAttribute(): ConsumesQueue
{
    return new ConsumesQueue(
        queue: 'default',
        bindings: ['vatly' => '#'],
        quorum: true,
    );
}

/**
 * Build a Consumer wired to mocked collaborators.
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
        $connection = mockAMQPConnection(heartbeat: 0);

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

describe('order-preserving retry', function () {
    it('requeues a failed job in place on a single-active quorum shard', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')
            ->with('ordered.shard.0')
            ->andReturn(orderedShardAttribute());

        [$consumer, $channel, $rabbitmq] = makeOrderRetryConsumer($scanner);

        // First delivery (attempts = 1 < tries), so the retry branch is taken.
        $message = mockAMQPMessage(['deliveryTag' => 7, 'headers' => []]);

        // In-place requeue: reject with requeue = true, and NO tail re-publish.
        $channel->shouldReceive('basic_reject')->once()->with(7, true)->andReturnNull();
        $channel->shouldNotReceive('basic_publish');
        $channel->shouldNotReceive('tx_select');

        $consumer->callHandleJobException(
            orderRetryJob($rabbitmq, $channel, $message, 'ordered.shard.0'),
            new RuntimeException('transient failure'),
        );

        expect(true)->toBeTrue();
    });

    it('releases a failed job to the tail on a commutative queue', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')
            ->with('default')
            ->andReturn(commutativeQueueAttribute());
        // releaseWithException re-publishes via the attribute lookup for routing.
        $scanner->shouldReceive('getQueueForJob')->andReturn(commutativeQueueAttribute());

        [$consumer, $channel, $rabbitmq] = makeOrderRetryConsumer($scanner);

        $message = mockAMQPMessage(['deliveryTag' => 9, 'headers' => []]);

        // Tail-republish path: ack + re-publish inside a transaction, no requeue.
        $channel->shouldReceive('tx_select')->once();
        $channel->shouldReceive('basic_publish')->once();
        $channel->shouldReceive('basic_ack')->once();
        $channel->shouldReceive('tx_commit')->once();
        $channel->shouldNotReceive('basic_reject');

        $consumer->callHandleJobException(
            orderRetryJob($rabbitmq, $channel, $message, 'default'),
            new RuntimeException('transient failure'),
        );

        expect(true)->toBeTrue();
    });

    it('parks a job that reached the tries cap to the DLX (reject, no requeue)', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        // At the cap the failJob branch runs before any retry-in-order decision,
        // so getAttributeForQueue is never consulted.
        $scanner->shouldReceive('getAttributeForQueue')->never();

        [$consumer, $channel, $rabbitmq] = makeOrderRetryConsumer($scanner);

        // x-delivery-count 3 => attempts 4 >= default tries 3 => terminal fail.
        $message = mockAMQPMessage(['deliveryTag' => 5, 'headers' => ['x-delivery-count' => 3]]);

        // Terminal park: reject WITHOUT requeue so the broker dead-letters it.
        $channel->shouldReceive('basic_reject')->once()->with(5, false)->andReturnNull();

        $consumer->callHandleJobException(
            orderRetryJob($rabbitmq, $channel, $message, 'ordered.shard.0'),
            new RuntimeException('poison'),
        );

        expect(true)->toBeTrue();
    });
});

describe('shouldRetryInOrder', function () {
    it('is true for a single-active quorum queue', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')->with('ordered.shard.0')->andReturn(orderedShardAttribute());

        [$consumer, $channel, $rabbitmq] = makeOrderRetryConsumer($scanner);
        $job = orderRetryJob($rabbitmq, $channel, mockAMQPMessage(), 'ordered.shard.0');

        expect($consumer->callShouldRetryInOrder($job))->toBeTrue();
    });

    it('is false for a quorum queue without a single active consumer', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')->with('default')->andReturn(commutativeQueueAttribute());

        [$consumer, $channel, $rabbitmq] = makeOrderRetryConsumer($scanner);
        $job = orderRetryJob($rabbitmq, $channel, mockAMQPMessage(), 'default');

        expect($consumer->callShouldRetryInOrder($job))->toBeFalse();
    });

    it('is false when the queue has no discovered attribute', function () {
        $scanner = Mockery::mock(AttributeScanner::class);
        $scanner->shouldReceive('getAttributeForQueue')->with('mystery')->andReturnNull();

        [$consumer, $channel, $rabbitmq] = makeOrderRetryConsumer($scanner);
        $job = orderRetryJob($rabbitmq, $channel, mockAMQPMessage(), 'mystery');

        expect($consumer->callShouldRetryInOrder($job))->toBeFalse();
    });
});
