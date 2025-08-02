<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Sql\SqlResult;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Exception\InvalidColumnIndex;

final class Result implements ResultInterface
{
    private array|null $firstRowCache = null;
    private array|null $columnNames = null;

    public function __construct(private readonly SqlResult $result)
    {
    }

    public function __destruct()
    {
        $this->free();
    }

    public function fetchRow(): array|null
    {
        if ($this->firstRowCache !== null) {
            $result = $this->firstRowCache;
            $this->firstRowCache = null;
        } else {
            $result = $this->result->fetchRow();
        }

        if ($this->columnNames === null && $result !== null) {
            $this->columnNames = \array_keys($result);
        }

        return $result;
    }

    public function fetchNumeric(): array|false
    {
        $result = $this->fetchRow();

        if ($result === null) {
            return false;
        }

        return \array_values($result);
    }

    public function fetchAssociative(): array|false
    {
        return $this->fetchRow() ?? false;
    }

    public function fetchOne(): mixed
    {
        $result = $this->fetchRow();

        if ($result === null || $result === []) {
            return false;
        }

        return $result[\array_key_first($result)];
    }

    public function fetchAllNumeric(): array
    {
        $rows = [];
        while (null !== $row = $this->fetchRow()) {
            $rows[] = \array_values($row);
        }

        return $rows;
    }

    public function fetchAllAssociative(): array
    {
        $rows = [];
        while (null !== $row = $this->fetchRow()) {
            $rows[] = $row;
        }

        return $rows;
    }

    public function fetchFirstColumn(): array
    {
        $rows = [];
        while (null !== $row = $this->fetchRow()) {
            $rows[] = $row[\array_key_first($row)];
        }

        return $rows;
    }

    public function rowCount(): int
    {
        return $this->result->getRowCount() ?? 0;
    }

    public function columnCount(): int
    {
        return $this->result->getColumnCount() ?? 0;
    }

    public function getColumnName(int $index): string
    {
        if ($this->columnNames === null) {
            $this->firstRowCache = $this->result->fetchRow();
            $this->columnNames = \array_keys($this->firstRowCache);
        }

        return $this->columnNames[$index] ?? throw InvalidColumnIndex::new($index);
    }

    public function free(): void
    {
        $this->firstRowCache = null;
        $this->columnNames = null;
    }
}
