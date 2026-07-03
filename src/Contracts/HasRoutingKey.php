<?php

declare(strict_types=1);

namespace Lettermint\RabbitMQ\Contracts;

/**
 * Interface for jobs that publish with a per-message routing key.
 *
 * By default a job publishes with the static routing key derived from its
 * `#[ConsumesQueue]` binding (or queue name). Implement this interface to
 * compute the routing key per dispatched instance instead — e.g. to shard a
 * topic exchange across many queues by a stable key (an aggregate id, a tenant
 * id) so ordering is preserved per key while different keys fan out in parallel.
 *
 * The returned key is injected into the job payload at publish time, so it is
 * also used verbatim when the job is re-published on retry — a retried message
 * lands on the same route it started on.
 *
 * @example
 * ```php
 * #[Exchange(name: 'events', type: ExchangeType::Topic)]
 * #[ConsumesQueue(queue: 'events.shard.0', bindings: ['events' => 'events.shard.0'], singleActiveConsumer: true)]
 * class ProjectEvent implements ShouldQueue, HasRoutingKey
 * {
 *     public function __construct(private string $aggregateId) {}
 *
 *     public function getRoutingKey(): string
 *     {
 *         return 'events.shard.'.(crc32($this->aggregateId) % 16);
 *     }
 * }
 * ```
 */
interface HasRoutingKey
{
    /**
     * Get the routing key this job should publish with.
     *
     * Overrides the static routing key from the `#[ConsumesQueue]` binding.
     * The exchange is still resolved from the job's attribute.
     */
    public function getRoutingKey(): string;
}
