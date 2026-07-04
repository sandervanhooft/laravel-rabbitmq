<?php

declare(strict_types=1);

use Lettermint\RabbitMQ\Connection\ChannelManager;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

pest()->group('integration');

/**
 * Pins the two RabbitMQ behaviors the retry design depends on, against a real
 * broker:
 *
 * 1. A requeued message returns to the TAIL of a quorum queue — so reject-with-
 *    requeue does NOT preserve order, which is why order-sensitive consumers
 *    still need their own version guard.
 * 2. `x-delivery-limit` parks a repeatedly-failing (poison) message to the DLX
 *    instead of churning forever.
 *
 * @see https://github.com/rabbitmq/rabbitmq-server/discussions/10500
 */
describe('RabbitMQ requeue + parking behavior', function () {
    beforeEach(function () {
        if (! canConnectToRabbitMQ()) {
            $this->markTestSkipped('RabbitMQ is not available');
        }

        config([
            'rabbitmq.connections.default.hosts' => [
                [
                    'host' => env('RABBITMQ_HOST', 'localhost'),
                    'port' => (int) env('RABBITMQ_PORT', 5672),
                    'user' => env('RABBITMQ_USER', 'guest'),
                    'password' => env('RABBITMQ_PASSWORD', 'guest'),
                    'vhost' => env('RABBITMQ_VHOST', '/'),
                ],
            ],
        ]);
    });

    afterEach(function () {
        try {
            $channel = app(ChannelManager::class)->topologyChannel();
            $channel->queue_delete('int-requeue-tail');
            $channel->queue_delete('int-parking');
            $channel->queue_delete('int-dlq');
            $channel->exchange_delete('int.dlx');
        } catch (Throwable $e) {
            // best-effort cleanup
        }

        try {
            app(ChannelManager::class)->closeAll();
        } catch (Throwable $e) {
            // ignore
        }
    });

    it('requeues a quorum-queue message to the tail, not the head', function () {
        $channelManager = app(ChannelManager::class);
        $channel = $channelManager->topologyChannel();

        $channel->queue_delete('int-requeue-tail');
        $channel->queue_declare(
            'int-requeue-tail',
            false, true, false, false, false,
            new AMQPTable(['x-queue-type' => 'quorum'])
        );

        // Enqueue A then B.
        $publish = $channelManager->publishChannel();
        foreach (['A', 'B'] as $body) {
            $publish->basic_publish(
                new AMQPMessage($body, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
                '',
                'int-requeue-tail'
            );
        }

        $consume = $channelManager->consumeChannel();

        // Head is A. Reject it with requeue.
        $first = basicGetWithRetry($consume, 'int-requeue-tail');
        expect($first)->not->toBeNull();
        expect($first->getBody())->toBe('A');
        $consume->basic_reject($first->getDeliveryTag(), true);

        // If requeue returned A to the HEAD, the next delivery would be A again.
        // On a quorum queue it is placed at the TAIL, so B is delivered first.
        $second = basicGetWithRetry($consume, 'int-requeue-tail');
        expect($second)->not->toBeNull();
        expect($second->getBody())->toBe('B');
        $consume->basic_ack($second->getDeliveryTag());

        // A comes back only after its successor — proving the reorder.
        $third = basicGetWithRetry($consume, 'int-requeue-tail');
        expect($third)->not->toBeNull();
        expect($third->getBody())->toBe('A');
        expect($third->isRedelivered())->toBeTrue();
        $consume->basic_ack($third->getDeliveryTag());
    });

    it('parks a poison message to the DLX once the delivery limit is exceeded', function () {
        $channelManager = app(ChannelManager::class);
        $channel = $channelManager->topologyChannel();

        // Dead-letter exchange + queue.
        $channel->exchange_declare('int.dlx', 'direct', false, true, false);
        $channel->queue_delete('int-dlq');
        $channel->queue_declare(
            'int-dlq',
            false, true, false, false, false,
            new AMQPTable(['x-queue-type' => 'quorum'])
        );
        $channel->queue_bind('int-dlq', 'int.dlx', 'parked');

        // Main quorum queue with a delivery limit that dead-letters to the DLX.
        $channel->queue_delete('int-parking');
        $channel->queue_declare(
            'int-parking',
            false, true, false, false, false,
            new AMQPTable([
                'x-queue-type' => 'quorum',
                'x-delivery-limit' => 2,
                'x-dead-letter-exchange' => 'int.dlx',
                'x-dead-letter-routing-key' => 'parked',
            ])
        );

        $channelManager->publishChannel()->basic_publish(
            new AMQPMessage('poison', ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
            '',
            'int-parking'
        );

        // Reject-with-requeue repeatedly (as the failure path does). The broker
        // increments x-delivery-count each time and, once the limit is exceeded,
        // dead-letters the message instead of redelivering it.
        $consume = $channelManager->consumeChannel();
        $deliveries = 0;
        for ($i = 0; $i < 10; $i++) {
            $message = basicGetWithRetry($consume, 'int-parking');
            if ($message === null) {
                break; // parked (or drained)
            }
            $deliveries++;
            $consume->basic_reject($message->getDeliveryTag(), true);
        }

        // It was retried a bounded number of times (not forever) ...
        expect($deliveries)->toBeGreaterThanOrEqual(1);
        expect($deliveries)->toBeLessThanOrEqual(5);

        // ... the main queue is now empty ...
        $mainInfo = $channel->queue_declare('int-parking', true, false, false, false);
        expect($mainInfo[1])->toBe(0);

        // ... and the poison message landed in the DLQ.
        $parked = basicGetWithRetry($consume, 'int-dlq');
        expect($parked)->not->toBeNull();
        expect($parked->getBody())->toBe('poison');
        $consume->basic_ack($parked->getDeliveryTag());
    });
});

/**
 * basic_get with a short retry, to absorb the small window between publish/
 * requeue and the message becoming available on a quorum queue.
 */
function basicGetWithRetry(PhpAmqpLib\Channel\AMQPChannel $channel, string $queue, int $attempts = 20): ?AMQPMessage
{
    for ($i = 0; $i < $attempts; $i++) {
        $message = $channel->basic_get($queue, false);
        if ($message !== null) {
            return $message;
        }
        usleep(50_000); // 50ms
    }

    return null;
}
