<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Postgres\DefaultPostgresConnector;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresConnectionPool;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Revolt\EventLoop;

final class AsyncDriver extends AbstractPostgreSQLDriver
{
    private bool $init = false;

    /**
     * Extract connection from the pool
     * @var \Closure(): PostgresConnection
     */
    private \Closure $pop;

    /**
     * Return the extracted connection back to the pool
     * @var \Closure(PostgresConnection): void
     */
    private \Closure $push;

    /**
     * Connection - release connection callback
     * @var \WeakMap<PostgresConnection, \Closure(): void>
     */
    private static \WeakMap $releaseCallback;

    public function connect(#[\SensitiveParameter] array $params): ConnectionInterface
    {
        if (!$this->init) {
            $this->init($params);
            $this->init = true;
        }

        $push = $this->push;
        $postgresConnection = ($this->pop)();
        $releaseConnection = static function () use($push, $postgresConnection): void {
            $push($postgresConnection);
        };
        self::$releaseCallback->offsetSet($postgresConnection, $releaseConnection);

        return new Connection($postgresConnection);
    }

    private function init(#[\SensitiveParameter] array $params): void
    {
        $pool = new PostgresConnectionPool(
            config: new PostgresConfig(
                host: $params['host'] ?? '',
                port: $params['port'] ?? PostgresConfig::DEFAULT_PORT,
                user: $params['user'] ?? null,
                password: $params['password'] ?? null,
                database: $params['dbname'] ?? null,
            ),
            maxConnections: $params['driverOptions']['max_connections'] ?? PostgresConnectionPool::DEFAULT_MAX_CONNECTIONS,
            idleTimeout: $params['driverOptions']['idle_timeout'] ?? PostgresConnectionPool::DEFAULT_IDLE_TIMEOUT,
            connector: new DefaultPostgresConnector(),
        );

        $this->pop = (function (): PostgresConnection {
            return $this->pop();
        })->bindTo($pool, $pool);

        $this->push = (function (PostgresConnection $connection): void {
            $this->push($connection);
        })->bindTo($pool, $pool);

        self::$releaseCallback ??= new \WeakMap();
    }

    /**
     * @internal
     */
    public static function releaseConnection(PostgresConnection $postgresConnection): void
    {
        EventLoop::defer(self::$releaseCallback->offsetGet($postgresConnection));
        self::$releaseCallback->offsetUnset($postgresConnection);
    }
}
