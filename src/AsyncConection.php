<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Postgres\PostgresConnection;
use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
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

    public function getDatabasePlatform(): AbstractPlatform
    {
        return $this->databasePlatform ??= $this->getOrCreateConnection()->getDatabasePlatform();
    }

    public function createExpressionBuilder(): ExpressionBuilder
    {
        return $this->getOrCreateConnection()->createExpressionBuilder();
    }

    protected function connect(): DriverConnection
    {
        return $this->getOrCreateConnection()->connect();
    }

    public function getServerVersion(): string
    {
        return $this->getOrCreateConnection()->connect()->getServerVersion();
    }

    public function isAutoCommit(): bool
    {
        return $this->getOrCreateConnection()->isAutoCommit();
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        $this->getOrCreateConnection()->setAutoCommit($autoCommit);
    }

    public function fetchAssociative(string $query, array $params = [], array $types = []): array|false
    {
        return $this->getOrCreateConnection()->fetchAssociative($query, $params, $types);
    }

    public function fetchNumeric(string $query, array $params = [], array $types = []): array|false
    {
        return $this->getOrCreateConnection()->fetchNumeric($query, $params, $types);
    }

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

    public function delete(string $table, array $criteria = [], array $types = []): int|string
    {
        return $this->getOrCreateConnection()->delete($table, $criteria, $types);
    }

    public function close(): void
    {
        $this->getOrCreateConnection()->close();
    }

    public function setTransactionIsolation(TransactionIsolationLevel $level): void
    {
        $this->getOrCreateConnection()->setTransactionIsolation($level);
    }

    public function getTransactionIsolation(): TransactionIsolationLevel
    {
        return $this->getOrCreateConnection()->getTransactionIsolation();
    }

    public function update(string $table, array $data, array $criteria = [], array $types = []): int|string
    {
        return $this->getOrCreateConnection()->update($table, $data, $criteria, $types);
    }

    public function insert(string $table, array $data, array $types = []): int|string
    {
        return $this->getOrCreateConnection()->insert($table, $data, $types);
    }

    public function quoteIdentifier(string $identifier): string
    {
        return $this->getOrCreateConnection()->quoteIdentifier($identifier);
    }

    public function quote(string $value): string
    {
        return $this->getOrCreateConnection()->quote($value);
    }

    public function fetchAllNumeric(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllNumeric($query, $params, $types);
    }

    public function fetchAllAssociative(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllAssociative($query, $params, $types);
    }

    public function fetchAllKeyValue(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllKeyValue($query, $params, $types);
    }

    public function fetchAllAssociativeIndexed(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchAllAssociativeIndexed($query, $params, $types);
    }

    public function fetchFirstColumn(string $query, array $params = [], array $types = []): array
    {
        return $this->getOrCreateConnection()->fetchFirstColumn($query, $params, $types);
    }

    public function iterateNumeric(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateNumeric($query, $params, $types);
    }

    public function iterateAssociative(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateAssociative($query, $params, $types);
    }

    public function iterateKeyValue(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateKeyValue($query, $params, $types);
    }

    public function iterateAssociativeIndexed(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateAssociativeIndexed($query, $params, $types);
    }

    public function iterateColumn(string $query, array $params = [], array $types = []): \Traversable
    {
        return $this->getOrCreateConnection()->iterateColumn($query, $params, $types);
    }

    public function prepare(string $sql): Statement
    {
        return $this->getOrCreateConnection()->prepare($sql);
    }

    public function executeQuery(string $sql, array $params = [], array $types = [], QueryCacheProfile|null $qcp = null): Result
    {
        return $this->getOrCreateConnection()->executeQuery($sql, $params, $types, $qcp);
    }

    public function executeCacheQuery(string $sql, array $params, array $types, QueryCacheProfile $qcp): Result
    {
        return $this->getOrCreateConnection()->executeCacheQuery($sql, $params, $types, $qcp);
    }

    public function executeStatement(string $sql, array $params = [], array $types = []): int|string
    {
        return $this->getOrCreateConnection()->executeStatement($sql, $params, $types);
    }

    public function getTransactionNestingLevel(): int
    {
        return $this->getOrCreateConnection()->getTransactionNestingLevel();
    }

    public function lastInsertId(): int|string
    {
        return $this->getOrCreateConnection()->lastInsertId();
    }

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
     */
    public function getNestTransactionsWithSavepoints(): bool
    {
        return $this->getOrCreateConnection()->getNestTransactionsWithSavepoints();
    }

    protected function _getNestedTransactionSavePointName(): string
    {
        return $this->getOrCreateConnection()->_getNestedTransactionSavePointName();
    }

    public function beginTransaction(): void
    {
        $this->getOrCreateConnection()->beginTransaction();
    }

    public function commit(): void
    {
        $this->getOrCreateConnection()->commit();
    }

    public function rollBack(): void
    {
        $this->getOrCreateConnection()->rollBack();
    }

    public function createSavepoint(string $savepoint): void
    {
        $this->getOrCreateConnection()->createSavepoint($savepoint);
    }

    public function releaseSavepoint(string $savepoint): void
    {
        $this->getOrCreateConnection()->releaseSavepoint($savepoint);
    }

    public function rollbackSavepoint(string $savepoint): void
    {
        $this->getOrCreateConnection()->rollbackSavepoint($savepoint);
    }

    /**
     * @return resource|object
     */
    public function getNativeConnection(): mixed
    {
        return $this->getOrCreateConnection()->getNativeConnection();
    }

    public function createSchemaManager(): AbstractSchemaManager
    {
        return $this->getOrCreateConnection()->createSchemaManager();
    }

    public function setRollbackOnly(): void
    {
        $this->getOrCreateConnection()->setRollbackOnly();
    }

    public function isRollbackOnly(): bool
    {
        return $this->getOrCreateConnection()->isRollbackOnly();
    }

    public function convertToDatabaseValue(mixed $value, string $type): mixed
    {
        return $this->getOrCreateConnection()->convertToDatabaseValue($value, $type);
    }

    public function convertToPHPValue(mixed $value, string $type): mixed
    {
        return $this->getOrCreateConnection()->convertToPHPValue($value, $type);
    }

    public function createQueryBuilder(): QueryBuilder
    {
        return $this->getOrCreateConnection()->createQueryBuilder();
    }
}
