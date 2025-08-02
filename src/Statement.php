<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Postgres\PostgresByteA;
use Amp\Postgres\PostgresStatement;
use Amp\Sql\SqlStatement;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;

final class Statement implements StatementInterface
{
    private array $parameters = [];

    public function __construct(private readonly SqlStatement $statement)
    {
    }

    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING): void
    {
        \assert(\is_int($param));

        $value = $value === null ? null : match ($type) {
            ParameterType::STRING => (string) $value,
            ParameterType::INTEGER => (int) $value,
            ParameterType::BOOLEAN => (bool) $value,
            ParameterType::NULL => null,
            ParameterType::BINARY, ParameterType::LARGE_OBJECT => match (true) {
                $this->statement instanceof PostgresStatement => new PostgresByteA($value),
                default => $value,
            },
            default => $value,
        };

        $this->parameters[$param - 1] = $value;
    }

    /**
     * @throws Exception
     */
    public function execute(): Result
    {
        try {
            return new Result($this->statement->execute($this->parameters));
        } catch (\Throwable $e) {
            throw new Exception($e);
        }
    }
}
