<?php

namespace Ejz;

use Generator;
use Amp\Loop;
use Amp\Promise;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\PgSqlCommandResult;
use RuntimeException;

class DatabasePostgres implements DatabaseInterface
{
    /** @var string */
    private $name;

    /** @var ConnectionConfig */
    private $connectionConfig;

    /** @var array */
    private $config;

    /** @var ?Connection */
    private $connection;

    /**
     * @param string $name
     * @param string $dsn
     * @param array  $config (optional)
     */
    public function __construct(string $name, string $dsn, array $config = [])
    {
        $this->name = $name;
        $this->connectionConfig = ConnectionConfig::fromString($dsn);
        $this->config = $config + [
            'quote' => '"',
            'iterator_chunk_size' => 100,
            'rand_iterator_intervals' => 1000,
        ];
        $this->connection = null;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function execAsync(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            if (!$this->connection instanceof Connection) {
                yield $this->connect();
            }
            if ($args) {
                $statement = yield $this->connection->prepare($sql);
                $result = yield $statement->execute($args);
            } else {
                $result = yield $this->connection->query($sql);
            }
            if ($result instanceof PgSqlCommandResult) {
                return $result->getAffectedRowCount();
            }
            $return = [];
            while (yield $result->advance()) {
                $return[] = $result->getCurrent();
            }
            return $return;
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return mixed
     */
    public function exec(string $sql, ...$args)
    {
        return \Amp\Promise\wait($this->execAsync($sql, ...$args));
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function allAsync(string $sql, ...$args): Promise
    {
        return $this->execAsync($sql, ...$args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function all(string $sql, ...$args): array
    {
        return $this->exec($sql, ...$args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function oneAsync(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->allAsync($sql, ...$args);
            return $all ? $all[0] : [];
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function one(string $sql, ...$args): array
    {
        $all = $this->all($sql, ...$args);
        return $all ? $all[0] : [];
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function valAsync(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $one = yield $this->oneAsync($sql, ...$args);
            $vals = array_values($one);
            return $vals[0] ?? null;
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return mixed
     */
    public function val(string $sql, ...$args)
    {
        $one = $this->one($sql, ...$args);
        $vals = array_values($one);
        return $vals[0] ?? null;
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function colAsync(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->allAsync($sql, ...$args);
            return $this->all2col($all);
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function col(string $sql, ...$args): array
    {
        $all = $this->all($sql, ...$args);
        return $this->all2col($all);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function mapAsync(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->allAsync($sql, ...$args);
            return $this->all2map($all);
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function map(string $sql, ...$args): array
    {
        $all = $this->all($sql, ...$args);
        return $this->all2map($all);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function dictAsync(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->allAsync($sql, ...$args);
            return $this->all2dict($all);
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function dict(string $sql, ...$args): array
    {
        $all = $this->all($sql, ...$args);
        return $this->all2dict($all);
    }

    /**
     * @return Promise
     */
    public function tablesAsync(): Promise
    {
        return \Amp\call(function () {
            $sql = '
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = ?
            ';
            return yield $this->colAsync($sql, 'public');
        });
    }

    /**
     * @return array
     */
    public function tables(): array
    {
        return \Amp\Promise\wait($this->tablesAsync());
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function fieldsAsync(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!in_array($table, yield $this->tablesAsync())) {
                return null;
            }
            $sql = '
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
            ';
            return yield $this->colAsync($sql, 'public', $table);
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return ?array
     */
    public function fields(string $table): ?array
    {
        return \Amp\Promise\wait($this->fieldsAsync($table));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pkAsync(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!in_array($table, yield $this->tablesAsync())) {
                return null;
            }
            $sql = '
                SELECT pg_attribute.attname
                FROM pg_index, pg_class, pg_attribute, pg_namespace
                WHERE
                    pg_class.oid = ?::regclass AND
                    indrelid = pg_class.oid AND
                    nspname = ? AND
                    pg_class.relnamespace = pg_namespace.oid AND
                    pg_attribute.attrelid = pg_class.oid AND
                    pg_attribute.attnum = ANY(pg_index.indkey) AND
                    indisprimary
            ';
            return yield $this->colAsync($sql, $table, 'public');
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return ?array
     */
    public function pk(string $table): ?array
    {
        return \Amp\Promise\wait($this->pkAsync($table));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function minAsync(string $table): Promise
    {
        return $this->minMaxAsync($table, true);
    }

    /**
     * @param string $table
     *
     * @return ?array
     */
    public function min(string $table): ?array
    {
        return \Amp\Promise\wait($this->minAsync($table));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function maxAsync(string $table): Promise
    {
        return $this->minMaxAsync($table, false);
    }

    /**
     * @param string $table
     *
     * @return ?array
     */
    public function max(string $table): ?array
    {
        return \Amp\Promise\wait($this->maxAsync($table));
    }

    /**
     * @param string $table
     * @param bool   $asc
     *
     * @return Promise
     */
    private function minMaxAsync(string $table, bool $asc): Promise
    {
        return \Amp\call(function ($table, $asc) {
            $quote = $this->config['quote'];
            $pk = yield $this->pkAsync($table);
            if ($pk === null || $pk === []) {
                return $pk ?? null;
            }
            $asc = $asc ? 'ASC' : 'DESC';
            [$select, $order] = [[], []];
            foreach ($pk as $field) {
                $q = $quote . $field . $quote;
                $select[] = $q;
                $order[] = $q . ' ' . $asc;
            }
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
            $sql = sprintf(
                $sql,
                implode(', ', $select),
                $quote . $table . $quote,
                implode(', ', $order)
            );
            $one = yield $this->oneAsync($sql);
            return $one ? array_values($one) : array_fill(0, count($pk), null);
        }, $table, $asc);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function truncateAsync(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!in_array($table, yield $this->tablesAsync())) {
                return false;
            }
            $quote = $this->config['quote'];
            $quoted = $quote . $table . $quote;
            yield $this->execAsync('TRUNCATE ' . $quoted . ' CASCADE');
            return true;
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return bool
     */
    public function truncate(string $table): bool
    {
        return \Amp\Promise\wait($this->truncateAsync($table));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function dropAsync(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!in_array($table, yield $this->tablesAsync())) {
                return false;
            }
            $quote = $this->config['quote'];
            $quoted = $quote . $table . $quote;
            yield $this->execAsync('DROP TABLE ' . $quoted . ' CASCADE');
            return true;
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return bool
     */
    public function drop(string $table): bool
    {
        return \Amp\Promise\wait($this->dropAsync($table));
    }

    /**
     * @return Promise
     */
    private function connect(): Promise
    {
        return \Amp\call(function () {
            $this->connection = yield \Amp\Postgres\connect($this->connectionConfig);
        });
    }

    /**
     * @param array $all
     *
     * @return array
     */
    private function all2col(array $all): array
    {
        if (!$all) {
            return [];
        }
        $key = array_keys($all[0])[0];
        $return = [];
        foreach ($all as $row) {
            $return[] = $row[$key];
        }
        return $return;
    }

    /**
     * @param array $all
     *
     * @return array
     */
    private function all2map(array $all): array
    {
        if (!$all) {
            return [];
        }
        $keys = array_keys($all[0]);
        $return = [];
        foreach ($all as $row) {
            $return[$row[$keys[0]]] = isset($keys[1]) ? $row[$keys[1]] : null;
        }
        return $return;
    }

    /**
     * @param array $all
     *
     * @return array
     */
    private function all2dict(array $all): array
    {
        if (!$all) {
            return [];
        }
        $key = array_keys($all[0])[0];
        $return = [];
        foreach ($all as $row) {
            $val = $row[$key];
            unset($row[$key]);
            $return[$val] = $row;
        }
        return $return;
    }
}
