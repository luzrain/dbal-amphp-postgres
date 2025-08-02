<?php

declare(strict_types=1);

namespace Luzrain\DbalDriver\AmphpPostgres;

use Amp\Postgres\PostgresQueryError;
use Doctrine\DBAL\Driver\AbstractException;

/**
 * @internal
 */
final class Exception extends AbstractException
{
    public function __construct(\Throwable $previous)
    {
        if ($previous instanceof PostgresQueryError) {
            $sqlState = $previous->getDiagnostics()['sqlstate'] ?? null;
            $message = $sqlState === null ? $previous->getMessage() : \sprintf('SQLSTATE[%s]: %s', $sqlState, $previous->getMessage());
        } else {
            $sqlState = null;
            $message = $previous->getMessage();
        }

        parent::__construct($message, $sqlState, (int) $previous->getCode(), $previous);
    }
}
