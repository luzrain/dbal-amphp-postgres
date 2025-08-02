## Async Doctrine DBAL driver for AMPHP Postgres client
![PHP >=8.2](https://img.shields.io/badge/PHP->=8.2-777bb3.svg)

Async Doctrine DBAL driver for AMPHP Postgres client

## Installation
``` bash
$ composer require luzrain/dbal-amphp-postgres
```

#### Example of usage
```php
use Doctrine\DBAL\DriverManager;
use Luzrain\DbalDriver\AmphpPostgres\AsyncConection;
use Luzrain\DbalDriver\AmphpPostgres\AsyncDriver;

$connectionParams = [
    'dbname' => 'mydb',
    'user' => 'user',
    'password' => 'secret',
    'host' => 'localhost',
    'driverClass' => AsyncDriver::class,
    'wrapperClass' => AsyncConection::class,
    'driverOptions' => [
        'max_connections' => 100,
        'idle_timeout' => 60,
    ],
];
$conn = DriverManager::getConnection($connectionParams);
```
