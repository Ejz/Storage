<?php

namespace Ejz;

use Generator;
use Amp\Loop;
use Amp\Promise;
use Amp\Deferred;
use Amp\Postgres;
use Amp\Producer;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;

class DatabasePostgres implements DatabaseInterface
{
    /** @var ConnectionConfig */
    private $connectionConfig;

    /** @var array */
    private $config;

    /** @var ?Connection */
    private $connection;

    /**
     * @param string $dsn
     * @param array  $config (optional)
     */
    public function __construct(string $dsn, array $config = [])
    {
        $this->connectionConfig = ConnectionConfig::fromString($dsn);
        $this->config = $config + [
            'quote' => '"',
            'iterator_chunk_size' => 100,
            'rand_iterator_intervals' => 1000,
        ];
        $this->connection = null;
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function execAsync(string $sql, ...$args): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $call = function () use ($deferred, $sql, $args) {
            if (!$this->connection instanceof Connection) {
                yield $this->connect();
            }
            if ($args) {
                $statement = yield $this->connection->prepare($sql);
                $result = yield $statement->execute($args);
            } else {
                $result = yield $this->connection->query($sql);
            }
            if ($result instanceof Postgres\PgSqlCommandResult) {
                $return = $result->getAffectedRowCount();
            } else {
                $return = [];
                while (yield $result->advance()) {
                    $return[] = $result->getCurrent();
                }
            }
            $deferred->resolve($return);
        };
        Loop::defer($call);
        return $promise;
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return mixed
     */
    public function exec(string $sql, ...$args)
    {
        Loop::run(function () use (&$ret, $sql, $args) {
            $ret = yield $this->execAsync($sql, ...$args);
        });
        return $ret;
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
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $this->allAsync($sql, ...$args)->onResolve(function ($_, $all) use ($deferred) {
            $deferred->resolve($all ? $all[0] : []);
        });
        return $promise;
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
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $this->oneAsync($sql, ...$args)->onResolve(function ($_, $one) use ($deferred) {
            $vals = array_values($one);
            $deferred->resolve($vals[0] ?? null);
        });
        return $promise;
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
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $this->allAsync($sql, ...$args)->onResolve(function ($_, $all) use ($deferred) {
            $deferred->resolve($this->all2col($all));
        });
        return $promise;
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
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $this->allAsync($sql, ...$args)->onResolve(function ($_, $all) use ($deferred) {
            $deferred->resolve($this->all2map($all));
        });
        return $promise;
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
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $this->allAsync($sql, ...$args)->onResolve(function ($_, $all) use ($deferred) {
            $deferred->resolve($this->all2dict($all));
        });
        return $promise;
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
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $sql = '
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = ?
        ';
        $this->colAsync($sql, 'public')->onResolve(function ($_, $col) use ($deferred) {
            $deferred->resolve($col);
        });
        return $promise;
    }

    /**
     * @return array
     */
    public function tables(): array
    {
        Loop::run(function () use (&$ret) {
            $ret = yield $this->tablesAsync();
        });
        return $ret;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function fieldsAsync(string $table): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $table) {
            $sql = '
                SELECT * FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
            ';
            $pk = yield $this->pkAsync($table);
            $all = yield $this->allAsync($sql, 'public', $table);
            $collect = [];
            $quote = $this->config['quote'];
            foreach ($all as $row) {
                $collect[$row['column_name']] = [
                    'quoted' => $quote . $row['column_name'] . $quote,
                    'type' => $row['data_type'],
                    'is_null' => strcasecmp($row['is_nullable'], 'yes') === 0,
                    'is_primary' => $row['column_name'] === $pk,
                ];
            }
            $deferred->resolve($collect);
        });
        return $promise;
    }

    /**
     * @param string $table
     *
     * @return array
     */
    public function fields(string $table): array
    {
        Loop::run(function () use (&$ret, $table) {
            $ret = yield $this->fieldsAsync($table);
        });
        return $ret;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pkAsync(string $table): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $table) {
            $sql = '
                SELECT pg_attribute.attname
                FROM pg_index, pg_class, pg_attribute, pg_namespace
                WHERE
                    pg_class.oid = ?::regclass AND
                    indrelid = pg_class.oid AND
                    nspname = ? AND
                    pg_class.relnamespace = pg_namespace.oid AND
                    pg_attribute.attrelid = pg_class.oid AND
                    pg_attribute.attnum = ANY(pg_index.indkey)
                    AND indisprimary
            ';
            $exists = in_array($table, yield $this->tablesAsync(), true);
            $pk = $exists ? yield $this->valAsync($sql, $table, 'public') : '';
            $deferred->resolve($pk);
        });
        return $promise;
    }

    /**
     * @param string $table
     *
     * @return string
     */
    public function pk(string $table): string
    {
        Loop::run(function () use (&$ret, $table) {
            $ret = yield $this->pkAsync($table);
        });
        return $ret;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function minAsync(string $table): Promise
    {
        return $this->minMaxAsync($table, 'min');
    }

    /**
     * @param string $table
     *
     * @return int
     */
    public function min(string $table): int
    {
        Loop::run(function () use (&$ret, $table) {
            $ret = yield $this->minAsync($table);
        });
        return $ret;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function maxAsync(string $table): Promise
    {
        return $this->minMaxAsync($table, 'max');
    }

    /**
     * @param string $table
     *
     * @return int
     */
    public function max(string $table): int
    {
        Loop::run(function () use (&$ret, $table) {
            $ret = yield $this->maxAsync($table);
        });
        return $ret;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function truncateAsync(string $table): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $table) {
            $tables = yield $this->tablesAsync();
            if (!in_array($table, $tables, true)) {
                return $deferred->resolve(false);
            }
            $quote = $this->config['quote'];
            yield $this->execAsync("TRUNCATE {$quote}{$table}{$quote} CASCADE");
            $deferred->resolve(true);
        });
        return $promise;
    }

    /**
     * @param string $table
     *
     * @return bool
     */
    public function truncate(string $table): bool
    {
        Loop::run(function () use (&$ret, $table) {
            $ret = yield $this->truncateAsync($table);
        });
        return $ret;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function dropAsync(string $table): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $table) {
            $quote = $this->config['quote'];
            yield $this->execAsync("DROP TABLE IF EXISTS {$quote}{$table}{$quote} CASCADE");
            $deferred->resolve();
        });
        return $promise;
    }

    /**
     * @param string $table
     */
    public function drop(string $table)
    {
        Loop::run(function () use ($table) {
            yield $this->dropAsync($table);
        });
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Producer
     */
    public function iterateAsync(string $table, array $params = []): Producer
    {
        $params += [
            'fields' => null,
            'where' => null,
            'asc' => true,
            'rand' => false,
            'min' => null,
            'max' => null,
        ];
        $params['config'] = ($params['config'] ?? []) + $this->config;
        $emit = function (callable $emit) use ($table, $params) {
            [
                'fields' => $fields,
                'where' => $where,
                'asc' => $asc,
                'rand' => $rand,
                'min' => $min,
                'max' => $max,
            ] = $params;
            $quote = $params['config']['quote'];
            $iterator_chunk_size = $params['config']['iterator_chunk_size'];
            $pk = yield $this->pkAsync($table);
            if (!$pk) {
                return;
            }
            if ($fields === null) {
                $fields = yield $this->fieldsAsync($table);
                $fields = array_map(function ($_) {
                    return $_['quoted'];
                }, $fields);
            }
            if ($rand) {
                $min = $min ?? yield $this->minAsync($table);
                $max = $max ?? yield $this->maxAsync($table);
                $params['fields'] = $fields;
                $n = $params['config']['rand_iterator_intervals'];
                $n = min($n, $max - $min + 1);
                $intervals = $this->getIntervalsForRandIterator($min, $max, $n);
                $params['rand'] = false;
                while (count($intervals)) {
                    $key = array_rand($intervals);
                    if (!isset($intervals[$key]['iterator'])) {
                        [$min, $max] = $intervals[$key];
                        $iterator = $this->iterateAsync(
                            $table,
                            [
                                'asc' => mt_rand() % 2,
                                'min' => $min,
                                'max' => $max,
                            ] + $params
                        );
                        $intervals[$key] = compact('min', 'max', 'iterator');
                    }
                    if (yield $intervals[$key]['iterator']->advance()) {
                        yield $emit($_ = $intervals[$key]['iterator']->getCurrent());
                    } else {
                        unset($intervals[$key]);
                    }
                }
                return;
            }
            $qk = $quote . $pk . $quote;
            $fields[$_pk = 'pk_' . md5($pk)] = $qk;
            $order_by = $qk . ' ' . ($asc ? 'ASC' : 'DESC');
            $fields = array_map(function ($field) use ($quote) {
                [$as, $exp] = $field;
                return $exp . ' AS ' . $quote . $as . $quote;
            }, array_map(null, array_keys($fields), array_values($fields)));
            $template = sprintf(
                'SELECT %s FROM %s WHERE (%s) AND (%%s) ORDER BY %s LIMIT %s',
                implode(', ', $fields),
                $quote . $table . $quote,
                $where === null ? 'TRUE' : $where,
                $order_by,
                $iterator_chunk_size
            );
            if ($asc) {
                $min = $min ?? yield $this->minAsync($table);
                if (!$min) {
                    return;
                }
            } else {
                $max = $max ?? yield $this->maxAsync($table);
                if (!$max) {
                    return;
                }
            }
            $first_iteration = true;
            do {
                $f = $quote . $pk . $quote;
                $op = ($asc ? '>' : '<') . ($first_iteration ? '=' : '');
                $where = "({$f} {$op} ?)";
                $args = [$asc ? $min : $max];
                if ($asc && $max || !$asc && $min) {
                    $op = $asc ? '<=' : '>=';
                    $where = "{$where} AND ({$f} {$op} ?)";
                    $args = [$args[0], $asc ? $max : $min];
                }
                $sql = sprintf($template, $where);
                $all = yield $this->allAsync($sql, ...$args);
                if (!$all) {
                    break;
                }
                foreach ($all as $row) {
                    $id = $row[$_pk];
                    unset($row[$_pk]);
                    yield $emit([$id, $row]);
                }
                if ($asc) {
                    $min = $id;
                } else {
                    $max = $id;
                }
                $first_iteration = false;
            } while (true);
        };
        return new Producer($emit);
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate(string $table, array $params = []): Generator
    {
        $ret = null;
        $iterator = function () use ($table, $params, &$ret) {
            static $iterator;
            if ($iterator === null) {
                $iterator = $this->iterateAsync($table, $params);
            }
            $ret = null;
            if (yield $iterator->advance()) {
                $ret = $iterator->getCurrent();
            }
        };
        do {
            \Amp\Loop::run($iterator);
            if ($ret === null) {
                break;
            }
            yield $ret;
        } while (true);
    }

    /**
     * @return Promise
     */
    private function connect(): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $call = function () use ($deferred) {
            $this->connection = yield Postgres\connect($this->connectionConfig);
            $deferred->resolve();
        };
        Loop::defer($call);
        return $promise;
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
     * @param string $table
     * @param string $type
     *
     * @return Promise
     */
    private function minMaxAsync(string $table, string $type): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $table, $type) {
            $quote = $this->config['quote'];
            $pk = yield $this->pkAsync($table);
            if (!$pk) {
                $deferred->resolve(0);
                return;
            }
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
            $pk = $quote . $pk . $quote;
            $sql = sprintf(
                $sql,
                $pk,
                $quote . $table . $quote,
                $pk . ' ' . ($type === 'min' ? 'ASC' : 'DESC')
            );
            $val = yield $this->valAsync($sql);
            $deferred->resolve($val);
        });
        return $promise;
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

    // /**
    //  * @param TableDefinition $definition
    //  *
    //  * @return Promise
    //  */
    // public function createAsync(TableDefinition $definition): Promise
    // {
    //     $deferred = new Deferred();
    //     $promise = $deferred->promise();
    //     Loop::defer(function () use ($deferred, $definition) {
    //         yield $this->dropAsync($definition->getTable());
    //         $commands = $this->createCommands($definition);
    //         foreach ($commands as $command) {
    //             yield $this->execAsync($command);
    //         }
    //         $deferred->resolve();
    //     });
    //     return $promise;
    // }

    // /**
    //  * @param TableDefinition $definition
    //  *
    //  * @return array
    //  */
    // private function createCommands(TableDefinition $definition): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $fields = $definition->getFields();
    //     $config = $definition->getConfig();
    //     $pk = $definition->getPk();
    //     $commands = [];
    //     $alters = [];
    //     $_fields = [];
    //     $type_append = [
    //         'timestamp' => '(0) without time zone',
    //     ];
    //     $sql_defaults = [
    //         'smallint' => '0',
    //         'integer' => '0',
    //         'bigint' => '0',
    //         'text' => '\'\'::text',
    //         'date' => 'CURRENT_DATE',
    //         'timestamp' => 'CURRENT_TIMESTAMP',
    //     ];
    //     $seq = "{$table}_seq";
    //     $pk_start_with = $config['pk_start_with'] ?? 1;
    //     $pk_increment_by = $config['pk_increment_by'] ?? 1;
    //     $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$seq}{$q} CASCADE";
    //     $commands[] = "
    //         CREATE SEQUENCE {$q}{$seq}{$q}
    //         AS bigint
    //         START WITH {$pk_start_with}
    //         INCREMENT BY {$pk_increment_by}
    //         MINVALUE {$pk_start_with}
    //     ";
    //     $alters[] = "
    //         ALTER TABLE {$q}{$table}{$q}
    //         ADD CONSTRAINT {$q}{$table}_{$pk}_pk{$q}
    //         PRIMARY KEY ({$q}{$pk}{$q})
    //     ";
    //     $_fields[] = sprintf(
    //         '%s bigint DEFAULT %s NOT NULL',
    //         $q . $pk . $q,
    //         "nextval('{$seq}'::regclass)"
    //     );
    //     $uniques = [];
    //     $indexes = [];
    //     foreach ($fields as $field => $meta) {
    //         if (!empty($meta['unique'])) {
    //             $u = (array) $meta['unique'];
    //             foreach ($u as $_) {
    //                 @ $uniques[$_][] = $field;
    //             }
    //         }
    //         $is_null = !empty($meta['is_null']);
    //         $_fields[] = sprintf(
    //             '%s %s%s DEFAULT %s %s',
    //             $q . $field . $q,
    //             $meta['type'],
    //             $type_append[$meta['type']] ?? '',
    //             $meta['sql_default'] ?? ($is_null ? 'NULL' : $sql_defaults[$meta['type']]),
    //             !$is_null ? 'NOT NULL' : 'NULL'
    //         );
    //     }
    //     foreach ($uniques as $name => $fs) {
    //         $fs = implode(', ', array_map(function ($f) use ($q) {
    //             return $q . $f . $q;
    //         }, $fs));
    //         $alters[] = "ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$name} UNIQUE ({$fs})";
    //     }
    //     foreach ($indexes as $name => [$type, $fs]) {
    //         $fs = implode(', ', array_map(function ($f) use ($q) {
    //             return $q . $f . $q;
    //         }, $fs));
    //         $alters[] = "CREATE INDEX {$q}{$name}{$q} ON {$q}{$table}{$q} USING {$type} ({$fs})";
    //     }
    //     $_fields = implode(', ', $_fields);
    //     $commands[] = "CREATE TABLE {$q}{$table}{$q} ({$_fields})";
    //     print_r($commands);
    //     print_r($alters);
    //     return array_merge($commands, $alters);
    // }

    

    // /**
    //  * @param TableDefinition $definition
    //  * @param array           $values
    //  *
    //  * @return Promise
    //  */
    // public function insertAsync(TableDefinition $definition, array $values): Promise
    // {
    //     $deferred = new Deferred();
    //     $promise = $deferred->promise();
    //     Loop::defer(function () use ($deferred, $definition, $values) {
    //         [$cmd, $args] = $this->insertCommand($definition, $values);
    //         $id = yield $this->valAsync($cmd, $args);
    //         $deferred->resolve($id);
    //     });
    //     return $promise;
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param array           $values
    //  *
    //  * @return array
    //  */
    // private function insertCommand(TableDefinition $definition, array $values): array
    // {
    //     $table = $definition->getTable();
    //     $fields = $definition->getFields();
    //     $pk = $definition->getPk();
    //     $q = $this->config['quote'];
    //     $_columns = [];
    //     $_values = [];
    //     $args = [];
    //     foreach ($fields as $field => $meta) {
    //         $_columns[] = $q . $field . $q;
    //         if (array_key_exists($field, $values)) {
    //             $args[] = $values[$field];
    //             $_values[] = '?';
    //         } else {
    //             $_values[] = 'DEFAULT';
    //         }
    //     }
    //     $_columns = implode(', ', $_columns);
    //     $_values = implode(', ', $_values);
    //     $insert = ($_columns && $_values) ? "({$_columns}) VALUES ({$_values})" : 'DEFAULT VALUES';
    //     $command = "INSERT INTO {$q}{$table}{$q} {$insert} RETURNING {$q}{$pk}{$q}";
    //     print_r($command);
    //     return [$command, $args];
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id
    //  *
    //  * @return Promise
    //  */
    // public function getAsync(TableDefinition $definition, int $id): Promise
    // {
    //     $deferred = new Deferred();
    //     $promise = $deferred->promise();
    //     Loop::defer(function () use ($deferred, $definition, $id) {
    //         [$cmd, $args] = $this->getCommand($definition, $id);
    //         $one = yield $this->oneAsync($cmd, $args);
    //         $deferred->resolve($one);
    //     });
    //     return $promise;
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id
    //  *
    //  * @return array
    //  */
    // private function getCommand(TableDefinition $definition, int $id): array
    // {
    //     $quote = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $pk = $definition->getPk();
    //     $args = [$id];
    //     $where = "{$q}{$pk}{$q} = ?";
    //     $command = "SELECT * FROM {$q}{$table}{$q} WHERE {$where} LIMIT 1";
    //     return [$command, $args];
    //     // $fields = $definition->getFields();
    //     // $select = is_numeric($select) ? 
    //     // $_columns = [];
    //     // $_values = [];
    //     // $args = [];
    //     // $pk = '';
    //     // foreach ($fields as $field => $meta) {
    //     //     if ($meta['type'] === 'primary') {
    //     //         $pk = $field;
    //     //         continue;
    //     //     }
    //     //     $_columns[] = $q . $field . $q;
    //     //     if (array_key_exists($field, $values)) {
    //     //         $args[] = $values[$field];
    //     //         $_values[] = '?';
    //     //     } else {
    //     //         $_values[] = 'DEFAULT';
    //     //     }
    //     // }
    //     // // $_columns = implode(', ', $_columns);
    //     // // $_values = implode(', ', $_values);
    //     // // $insert = $_columns && $_values ? "({$_columns}) VALUES ({$_values})" : 'DEFAULT VALUES';
    //     // print_r($command);
    // }
}
