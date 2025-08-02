<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Postgres\PostgresConnection;
use Doctrine\DBAL\Cache\CacheException;
use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\ConnectionException;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\Expression\ExpressionBuilder;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Statement;
use Doctrine\DBAL\TransactionIsolationLevel;
use Revolt\EventLoop\FiberLocal;

final class AsyncConection extends DbalConnection
{
    private FiberLocal $fiberLocal;
    private DbalConnection $baseConnection;
    private AbstractPlatform $databasePlatform;

    /**
     * @internal The connection can be only instantiated by the driver manager.
     * @psalm-suppress InternalMethod
     */
    public function __construct(#[\SensitiveParameter] array $params, Driver $driver, Configuration|null $config = null)
    {
        $this->fiberLocal = new FiberLocal(static fn() => null);
        $this->driver = $driver;
        $this->baseConnection = new DbalConnection($params, $driver, $config);
        // skip parent constructor
    }

    private function getOrCreateConnection(): DbalConnection
    {
        $wrapper = $this->fiberLocal->get();

        if ($wrapper === null) {
            $wrapper = new class (clone $this->baseConnection) {
                public function __construct(public readonly DbalConnection $connection)
                {
                }

                public function __destruct()
                {
                    $connection = $this->connection->getNativeConnection();
                    \assert($connection instanceof PostgresConnection);
                    AsyncDriver::releaseConnection($connection);
                }
            };

            $this->fiberLocal->set($wrapper);
        }

        return $wrapper->connection;
    }

    /**
     * @internal
     * @psalm-suppress InternalMethod
     */
    public function getParams(): array
    {
        return $this->baseConnection->getParams();
    }

    /**
     * @throws Exception
     */
    public function getDatabase(): string|null
    {
        return $this->getOrCreateConnection()->getDatabase();
    }

    public function getDriver(): Driver
    {
        return $this->driver;
    }

    public function getConfiguration(): Configuration
    {
        return $this->baseConnection->getConfiguration();
    }

    /**
     * @throws Exception
     */
    public function getDatabasePlatform(): AbstractPlatform
    {
        return $this->databasePlatform ??= $this->getOrCreateConnection()->getDatabasePlatform();
    }

    public function createExpressionBuilder(): ExpressionBuilder
    {
        return $this->getOrCreateConnection()->createExpressionBuilder();
    }

    /**
     * @throws Exception
     */
    protected function connect(): DriverConnection
    {
        return $this->getOrCreateConnection()->connect();
    }

    /**
     * @throws Exception
     */
    public function getServerVersion(): string
    {
        return $this->getOrCreateConnection()->connect()->getServerVersion();
    }

    public function isAutoCommit(): bool
    {
        return $this->getOrCreateConnection()->isAutoCommit();
    }

    /**
     * @throws Exception
     */
    public function setAutoCommit(bool $autoCommit): void
    {
        $this->getOrCreateConnection()->setAutoCommit($autoCommit);
    }

    /**
     * @throws Exception
     */
    public function fetchAssociative(string $query, array $params = [], array $types = []): array|false
    {
        return $this->getOrCreateConnection()->fetchAssociative($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function fetchNumeric(string $query, array $params = [], array $types = []): array|false
    {
        return $this->getOrCreateConnection()->fetchNumeric($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function fetchOne(string $query, array $params = [], array $types = []): mixed
    {
        return $this->getOrCreateConnection()->fetchOne($query, $params, $types);
    }

    public function isConnected(): bool
    {
        return $this->getOrCreateConnection()->isConnected();
    }

    public function isTransactionActive(): bool
    {
        return $this->getOrCreateConnection()->isTransactionActive();
    }

    /**
     * @throws Exception
     */
    public function delete(string $table, array $criteria = [], array $types = []): int|string
    {
        return $this->getOrCreateConnection()->delete($table, $criteria, $types);
    }

    public function close(): void
    {
        $this->getOrCreateConnection()->close();
    }

    /**
     * @throws Exception
     */
    public function setTransactionIsolation(TransactionIsolationLevel $level): void
    {
        $this->getOrCreateConnection()->setTransactionIsolation($level);
    }

    /**
     * @throws Exception
     */
    public function getTransactionIsolation(): TransactionIsolationLevel
    {
        return $this->getOrCreateConnection()->getTransactionIsolation();
    }

    /**
     * @throws Exception
     */
    public function update(string $table, array $data, array $criteria = [], array $types = []): int|string
    {
        return $this->getOrCreateConnection()->update($table, $data, $criteria, $types);
    }

    /**
     * @throws Exception
     */
    public function insert(string $table, array $data, array $types = []): int|string
    {
        return $this->getOrCreateConnection()->insert($table, $data, $types);
    }

    /**
     * @deprecated
     * @throws Exception
     */
    public function quoteIdentifier(string $identifier): string
    {
        return $this->getOrCreateConnection()->quoteIdentifier($identifier);
    }

    /**
     * @throws Exception
     */
    public function quoteSingleIdentifier(string $identifier): string
    {
        return $this->getOrCreateConnection()->getDatabasePlatform()->quoteSingleIdentifier($identifier);
    }

    /**
     * @throws Exception
     */
    public function quote(string $value): string
    {
        return $this->getOrCreateConnection()->quote($value);
    }

    /**
     * @throws Exception
     */
    public function fetchAllNumeric(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllNumeric($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function fetchAllAssociative(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllAssociative($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function fetchAllKeyValue(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllKeyValue($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function fetchAllAssociativeIndexed(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllAssociativeIndexed($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function fetchFirstColumn(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchFirstColumn($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function iterateNumeric(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateNumeric($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function iterateAssociative(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateAssociative($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function iterateKeyValue(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateKeyValue($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function iterateAssociativeIndexed(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateAssociativeIndexed($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function iterateColumn(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateColumn($query, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function prepare(string $sql): Statement
    {
        return $this->getOrCreateConnection()->prepare($sql);
    }

    /**
     * @throws Exception
     */
    public function executeQuery(string $sql, array $params = [], array $types = [], QueryCacheProfile|null $qcp = null): Result
    {
        return $this->getOrCreateConnection()->executeQuery($sql, $params, $types, $qcp);
    }

    /**
     * @throws CacheException
     * @throws Exception
     */
    public function executeCacheQuery(string $sql, array $params, array $types, QueryCacheProfile $qcp): Result
    {
        return $this->getOrCreateConnection()->executeCacheQuery($sql, $params, $types, $qcp);
    }

    /**
     * @throws Exception
     */
    public function executeStatement(string $sql, array $params = [], array $types = []): int|string
    {
        return $this->getOrCreateConnection()->executeStatement($sql, $params, $types);
    }

    public function getTransactionNestingLevel(): int
    {
        return $this->getOrCreateConnection()->getTransactionNestingLevel();
    }

    /**
     * @throws Exception
     */
    public function lastInsertId(): int|string
    {
        return $this->getOrCreateConnection()->lastInsertId();
    }

    /**
     * @throws \Throwable
     */
    public function transactional(\Closure $func): mixed
    {
        return $this->getOrCreateConnection()->transactional($func);
    }

    /**
     * @deprecated
     */
    public function setNestTransactionsWithSavepoints(bool $nestTransactionsWithSavepoints): void
    {
        $this->getOrCreateConnection()->setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints);
    }

    /**
     * @deprecated
     * @throws Exception
     */
    public function getNestTransactionsWithSavepoints(): bool
    {
        return $this->getOrCreateConnection()->getNestTransactionsWithSavepoints();
    }

    protected function _getNestedTransactionSavePointName(): string
    {
        return $this->getOrCreateConnection()->_getNestedTransactionSavePointName();
    }

    /**
     * @throws Exception
     */
    public function beginTransaction(): void
    {
        $this->getOrCreateConnection()->beginTransaction();
    }

    /**
     * @throws Exception
     */
    public function commit(): void
    {
        $this->getOrCreateConnection()->commit();
    }

    /**
     * @throws Exception
     */
    public function rollBack(): void
    {
        $this->getOrCreateConnection()->rollBack();
    }

    /**
     * @throws Exception
     */
    public function createSavepoint(string $savepoint): void
    {
        $this->getOrCreateConnection()->createSavepoint($savepoint);
    }

    /**
     * @throws Exception
     */
    public function releaseSavepoint(string $savepoint): void
    {
        $this->getOrCreateConnection()->releaseSavepoint($savepoint);
    }

    /**
     * @throws Exception
     */
    public function rollbackSavepoint(string $savepoint): void
    {
        $this->getOrCreateConnection()->rollbackSavepoint($savepoint);
    }

    /**
     * @return resource|object
     * @throws Exception
     */
    public function getNativeConnection(): mixed
    {
        return $this->getOrCreateConnection()->getNativeConnection();
    }

    /**
     * @throws Exception
     */
    public function createSchemaManager(): AbstractSchemaManager
    {
        return $this->getOrCreateConnection()->createSchemaManager();
    }

    /**
     * @throws ConnectionException
     */
    public function setRollbackOnly(): void
    {
        $this->getOrCreateConnection()->setRollbackOnly();
    }

    /**
     * @throws ConnectionException
     */
    public function isRollbackOnly(): bool
    {
        return $this->getOrCreateConnection()->isRollbackOnly();
    }

    /**
     * @throws Exception
     */
    public function convertToDatabaseValue(mixed $value, string $type): mixed
    {
        return $this->getOrCreateConnection()->convertToDatabaseValue($value, $type);
    }

    /**
     * @throws Exception
     */
    public function convertToPHPValue(mixed $value, string $type): mixed
    {
        return $this->getOrCreateConnection()->convertToPHPValue($value, $type);
    }

    public function createQueryBuilder(): QueryBuilder
    {
        return $this->getOrCreateConnection()->createQueryBuilder();
    }
}
