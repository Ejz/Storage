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

    /* -- */
    public const INVALID_PRIMARY_KEY = 'INVALID_PRIMARY_KEY: %s';
    public const INVALID_TABLE_FIELD = 'INVALID_TABLE_FIELD: %s -> %s';
    /* -- */

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
     * @return array
     */
    public function fields(string $table): array
    {
        return \Amp\Promise\wait($this->fieldsAsync($table));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pksAsync(string $table): Promise
    {
        return \Amp\call(function ($table) {
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
            if (!in_array($table, yield $this->tablesAsync())) {
                return null;
            }
            return yield $this->colAsync($sql, $table, 'public');
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return ?array
     */
    public function pks(string $table): ?array
    {
        return \Amp\Promise\wait($this->pksAsync($table));
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
     * @return mixed
     */
    public function min(string $table)
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
     * @return mixed
     */
    public function max(string $table)
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
            $pks = yield $this->pksAsync($table);
            if ($pks === null || count($pks) !== 1) {
                return null;
            }
            [$pk] = $pks;
            $pk = $quote . $pk . $quote;
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
            $sql = sprintf(
                $sql,
                $pk,
                $quote . $table . $quote,
                $pk . ' ' . ($asc ? 'ASC' : 'DESC')
            );
            return yield $this->valAsync($sql);
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
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate(string $table, array $params = []): Generator
    {
        $params += [
            'fields' => null,
            '_fields' => null,
            'asc' => true,
            'rand' => false,
            'min' => null,
            'max' => null,
            'limit' => 1E9,
            'pk' => null,
        ];
        $params['config'] = ($params['config'] ?? []) + $this->config;
        [
            'fields' => $fields,
            '_fields' => $_fields,
            'asc' => $asc,
            'rand' => $rand,
            'min' => $min,
            'max' => $max,
            'limit' => $limit,
            'pk' => $pk,
            'quote' => $quote,
            'iterator_chunk_size' => $iterator_chunk_size,
            'rand_iterator_intervals' => $rand_iterator_intervals,
        ] = $params;
        if (!isset($pk)) {
            $pks = $this->pks($table);
            if ($pks === null || count($pks) !== 1) {
                return;
            }
            [$pk] = $pks;
        }
        $qpk = $quote . $pk . $quote;
        $_fields = $_fields ?? $this->fields($table);
        $fields = $fields ?? $_fields;
        if (count($_ = array_diff($fields, $_fields))) {
            throw new RuntimeException(sprintf(self::INVALID_TABLE_FIELD, $table, $_[0]));
        }
        if ($rand) {
            $min = $min ?? $this->min($table);
            $max = $max ?? $this->max($table);
            if (!isset($min, $max)) {
                return;
            }
            $rand = false;
            $params = compact('fields', '_fields', 'pk', 'rand') + $params;
            if (is_int($min) && is_int($max)) {
                $rand_iterator_intervals = min($rand_iterator_intervals, $max - $min + 1);
                $intervals = $this->getIntervalsForRandIterator($min, $max, $rand_iterator_intervals);
            } else {
                $intervals = [[$min, $max]];
            }
            $c = count($intervals);
            while ($c) {
                $key = array_rand($intervals);
                if (!$intervals[$key] instanceof Generator) {
                    [$min, $max] = $intervals[$key];
                    $asc = (bool) mt_rand(0, 1);
                    $params = compact('asc', 'min', 'max') + $params;
                    $intervals[$key] = $this->iterate($table, $params);
                }
                if ($intervals[$key]->valid()) {
                    $k = $intervals[$key]->key();
                    $v = $intervals[$key]->current();
                    yield $k => $v;
                    $intervals[$key]->next();
                } else {
                    unset($intervals[$key]);
                    $c--;
                }
            }
            return;
        }
        $fields = array_map(function ($field) use ($quote) {
            return $quote . $field . $quote;
        }, $fields);
        $_pk = 'pk_' . md5($pk);
        $fields[] = $qpk . ' AS ' . $quote . $_pk . $quote;
        $order_by = $qpk . ' ' . ($asc ? 'ASC' : 'DESC');
        $template = sprintf(
            'SELECT %s FROM %s %%s ORDER BY %s LIMIT %%s',
            implode(', ', $fields),
            $quote . $table . $quote,
            $order_by
        );
        [$op1, $op2] = $asc ? ['>', '<='] : ['<', '>='];
        // $min = $min ?? ($asc ? 1 : null);
        // $max = $max ?? ($asc ? null : 1E9);
        while ($limit > 0) {
            $first = $first ?? true;
            $where = [];
            if (($asc && $min) || (!$asc && $max)) {
                $where[] = '(' . $qpk . ' ' . $op1 . ($first ? '=' : '') . ' ?)';
                $args[] = $asc ? $min : $max;
            }
            if (($asc && $max) || (!$asc && $min)) {
                $where[] = '(' . $qpk . ' ' . $op2 . ' ?)';
                $args[] = $asc ? $max : $min;
            }
            $where = ($where ? 'WHERE ' : '') . implode(' AND ', $where);
            $sql = sprintf($template, $where, min($limit, $iterator_chunk_size));
            $all = $this->all($sql, ...$args);
            if (!$all) {
                break;
            }
            foreach ($all as $row) {
                $id = $row[$_pk];
                unset($row[$_pk]);
                yield $id => $row;
            }
            $limit -= count($all);
            ${$asc ? 'min' : 'max'} = $id;
            $first = false;
        }
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

    /**
     * @param int $min
     * @param int $max
     * @param int $n
     *
     * @return array
     */
    private function getIntervalsForRandIterator(int $min, int $max, int $n): array
    {
        $inc = ($max - $min + 1) / $n;
        $intervals = [];
        for ($i = 0; $i < $n; $i++) {
            $one = (int) floor($min);
            $two = (int) floor($min + $inc - 1E-6);
            $one = $i > 0 && $one === $extwo ? $one + 1 : $one;
            $one = $one > $two ? $one - 1 : $one;
            $intervals[] = [$one, $two];
            $min += $inc;
            $extwo = $two;
        }
        return $intervals;
    }
}
