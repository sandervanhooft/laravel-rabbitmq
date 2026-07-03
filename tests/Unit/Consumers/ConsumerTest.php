<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Lettermint\RabbitMQ\Connection\ChannelManager;
use Lettermint\RabbitMQ\Consumers\Consumer;
use Lettermint\RabbitMQ\Discovery\AttributeScanner;
use Lettermint\RabbitMQ\Queue\RabbitMQJob;
use Lettermint\RabbitMQ\Queue\RabbitMQQueue;
use Mockery\MockInterface;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

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
