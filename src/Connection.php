<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Postgres\PostgresConnection;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Exception\NoIdentityValue;

final readonly class Connection implements DriverConnection
{
    public function __construct(private PostgresConnection $connection)
    {
    }

    public function prepare(string $sql): Statement
    {
        return new Statement($this->connection->prepare($sql));
    }

    /**
     * @throws Exception
     */
    public function query(string $sql): Result
    {
        try {
            return new Result($this->connection->query($sql));
        } catch (\Throwable $e) {
            throw new Exception($e);
        }
    }

    /**
     * @throws Exception
     */
    public function quote(string $value): string
    {
        try {
            return $this->connection->quoteLiteral($value);
        } catch (\Throwable $e) {
            throw new Exception($e);
        }
    }

    /**
     * @throws Exception
     */
    public function exec(string $sql): int
    {
        try {
            return $this->connection->query($sql)->getRowCount();
        } catch (\Throwable $e) {
            throw new Exception($e);
        }
    }

    /**
     * @throws NoIdentityValue
     * @throws Exception
     */
    public function lastInsertId(): int|string
    {
        try {
            return $this->query('SELECT LASTVAL()')->fetchOne();
        } catch (Exception $exception) {
            if ($exception->getSQLState() === '55000') {
                throw NoIdentityValue::new($exception);
            }

            throw $exception;
        }
    }

    public function beginTransaction(): void
    {
        $this->exec('BEGIN');
    }

    public function commit(): void
    {
        $this->exec('COMMIT');
    }

    public function rollBack(): void
    {
        $this->exec('ROLLBACK');
    }

    public function getNativeConnection(): PostgresConnection
    {
        return $this->connection;
    }

    public function getServerVersion(): string
    {
        try {
            return $this->query('SHOW server_version')->fetchOne();
        } catch (Exception) {
            return '';
        }
    }
}
