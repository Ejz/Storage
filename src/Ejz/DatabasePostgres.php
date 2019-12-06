<?php

namespace Ejz;

use Generator;
use Amp\Loop;
use Amp\Promise;
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
            'shard' => '',
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
            if ($result instanceof Postgres\PgSqlCommandResult) {
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
     * @param bool   $pk    (optional)
     *
     * @return Promise
     */
    public function fieldsAsync(string $table, bool $pk = true): Promise
    {
        return \Amp\call(function ($table) {
            $quote = $this->config['quote'];
            $sql = '
                SELECT * FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
            ';
            $pk = yield $this->pkAsync($table);
            $all = yield $this->allAsync($sql, 'public', $table);
            $collect = [];
            foreach ($all as $row) {
                $collect[$row['column_name']] = [
                    'quoted' => $quote . $row['column_name'] . $quote,
                    'type' => $row['data_type'],
                    'is_nullable' => strcasecmp($row['is_nullable'], 'yes') === 0,
                    'is_primary' => $row['column_name'] === $pk,
                ];
            }
            return $collect;
        }, $table);
    }

    /**
     * @param string $table
     * @param bool   $pk    (optional)
     *
     * @return array
     */
    public function fields(string $table, bool $pk = true): array
    {
        return \Amp\Promise\wait($this->fieldsAsync($table, $pk));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pkAsync(string $table): Promise
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
                    pg_attribute.attnum = ANY(pg_index.indkey)
                    AND indisprimary
            ';
            $exists = in_array($table, yield $this->tablesAsync(), true);
            return $exists ? yield $this->valAsync($sql, $table, 'public') : '';
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return string
     */
    public function pk(string $table): string
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
        return $this->minMaxAsync($table, 'min');
    }

    /**
     * @param string $table
     *
     * @return int
     */
    public function min(string $table): int
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
        return $this->minMaxAsync($table, 'max');
    }

    /**
     * @param string $table
     *
     * @return int
     */
    public function max(string $table): int
    {
        return \Amp\Promise\wait($this->maxAsync($table));
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function truncateAsync(string $table): Promise
    {
        return \Amp\call(function ($table) {
            $quote = $this->config['quote'];
            $tables = yield $this->tablesAsync();
            if (!in_array($table, $tables, true)) {
                return false;
            }
            yield $this->execAsync("TRUNCATE {$quote}{$table}{$quote} CASCADE");
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
            $quote = $this->config['quote'];
            $sql = "DROP TABLE IF EXISTS {$quote}{$table}{$quote} CASCADE";
            yield $this->execAsync($sql);
        }, $table);
    }

    /**
     * @param string $table
     */
    public function drop(string $table)
    {
        \Amp\Promise\wait($this->dropAsync($table));
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Producer
     */
    public function iterate(string $table, array $params = []): Producer
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
            $pk = $params['_pk'] ?? yield $this->pkAsync($table);
            if (!$pk) {
                return;
            }
            $where = is_array($where) ? $this->flattenWhere($where) : $where;
            $quote = $params['config']['quote'];
            $iterator_chunk_size = $params['config']['iterator_chunk_size'];
            $fields = $fields ?? array_fill_keys(array_keys(yield $this->fieldsAsync($table)), []);
            $fields = (array) $fields;
            if (!array_filter(array_keys($fields), 'is_string')) {
                $fields = array_fill_keys($fields, []);
            }
            if ($rand) {
                $min = $min ?? yield $this->minAsync($table);
                $max = $max ?? yield $this->maxAsync($table);
                $params['fields'] = $fields;
                $params['_pk'] = $pk;
                $n = $params['config']['rand_iterator_intervals'];
                $n = min($n, $max - $min + 1);
                $intervals = $this->getIntervalsForRandIterator($min, $max, $n);
                $params['rand'] = false;
                while (count($intervals)) {
                    $key = array_rand($intervals);
                    if (!isset($intervals[$key]['iterator'])) {
                        [$min, $max] = $intervals[$key];
                        $iterator = $this->iterate(
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
            $qpk = $quote . $pk . $quote;
            $order_by = $qpk . ' ' . ($asc ? 'ASC' : 'DESC');
            $fields[$_pk = 'pk_' . md5($pk)] = ['field' => $pk];
            $fields = array_map(function ($_) use ($quote) {
                [$alias, $field] = $_;
                $fa = $quote . $alias . $quote;
                $f = $quote . ($field['field'] ?? $alias) . $quote;
                $pattern = $field['get_pattern'] ?? '%s';
                $head = str_replace('%s', $f, $pattern);
                return $head . ' AS ' . $fa;
            }, array_map(null, array_keys($fields), array_values($fields)));
            $template = sprintf(
                'SELECT %s FROM %s WHERE (%s) AND (%%s) ORDER BY %s LIMIT %s',
                implode(', ', $fields),
                $quote . $table . $quote,
                $where === null ? 'TRUE' : (is_array($where) ? $where[0] : $where),
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
                $_where = "({$f} {$op} ?)";
                $args = is_array($where) ? $where[1] : [];
                $args[] = $asc ? $min : $max;
                if ($asc && $max || !$asc && $min) {
                    $op = $asc ? '<=' : '>=';
                    $_where = "{$_where} AND ({$f} {$op} ?)";
                    $args = [$args[0], $asc ? $max : $min];
                }
                $sql = sprintf($template, $_where);
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
     * @param TableDefinition $definition
     *
     * @return Promise
     */
    public function createAsync(TableDefinition $definition): Promise
    {
        return \Amp\call(function ($definition) {
            yield $this->dropAsync($definition->getTable());
            $commands = $this->createCommands($definition);
            foreach ($commands as $command) {
                yield $this->execAsync($command);
            }
        }, $definition);
    }

    /**
     * @param TableDefinition $definition
     * @param array           $values
     *
     * @return Promise
     */
    public function insertAsync(TableDefinition $definition, array $values): Promise
    {
        return \Amp\call(function ($definition, $values) {
            [$cmd, $args] = $this->insertCommand($definition, $values);
            return yield $this->valAsync($cmd, ...$args);
        }, $definition, $values);
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id
     * @param array           $values
     *
     * @return Promise
     */
    public function updateAsync(TableDefinition $definition, int $id, array $values): Promise
    {
        return \Amp\call(function ($definition, $id, $values) {
            if (!$values) {
                return false;
            }
            [$cmd, $args] = $this->updateCommand($definition, $id, $values);
            return yield $this->execAsync($cmd, ...$args);
        }, $definition, $id, $values);
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id
     * @param mixed           $fields
     *
     * @return Promise
     */
    public function getAsync(TableDefinition $definition, int $id, $fields): Promise
    {
        return \Amp\call(function ($definition, $id, $fields) {
            [$cmd, $args] = $this->getCommand($definition, $id, $fields);
            $one = yield $this->oneAsync($cmd, ...$args);
            foreach ($one as $key => &$value) {
                if ($fields[$key]['get'] ?? null) {
                    $value = $fields[$key]['get']($value);
                }
            }
            unset($value);
            return $one;
        }, $definition, $id, $fields);
    }

    /**
     * @return Promise
     */
    private function connect(): Promise
    {
        return \Amp\call(function () {
            $this->connection = yield Postgres\connect($this->connectionConfig);
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
     * @param string $table
     * @param string $type
     *
     * @return Promise
     */
    private function minMaxAsync(string $table, string $type): Promise
    {
        return \Amp\call(function ($table, $type) {
            $quote = $this->config['quote'];
            $pk = yield $this->pkAsync($table);
            if (!$pk) {
                return 0;
            }
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
            $pk = $quote . $pk . $quote;
            $sql = sprintf(
                $sql,
                $pk,
                $quote . $table . $quote,
                $pk . ' ' . ($type === 'min' ? 'ASC' : 'DESC')
            );
            return yield $this->valAsync($sql);
        }, $table, $type);
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

    /**
     * @param TableDefinition $definition
     *
     * @return array
     */
    private function createCommands(TableDefinition $definition): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $fields = $definition->getFields();
        $pk = $definition->getPrimaryKey();
        $commands = [];
        $alters = [];
        $_fields = [];
        $map = [
            TableDefinition::TYPE_INT => 'INTEGER',
            TableDefinition::TYPE_BLOB => 'BYTEA',
            TableDefinition::TYPE_TEXT => 'TEXT',
            TableDefinition::TYPE_JSON => 'JSONB',
        ];
        $defaults = [
            TableDefinition::TYPE_INT => '0::INTEGER',
            TableDefinition::TYPE_BLOB => '\'\'::BYTEA',
            TableDefinition::TYPE_TEXT => '\'\'::TEXT',
            TableDefinition::TYPE_JSON => '\'{}\'::JSONB',
        ];
        // $type_append = [
        //     'timestamp' => '(0) without time zone',
        // ];
        // $sql_defaults = [
        //     'smallint' => '0',
        //     'integer' => '0',
        //     'int' => '0',
        //     'bigint' => '0',
        //     'text' => '\'\'::text',
        //     'date' => 'CURRENT_DATE',
        //     'timestamp' => 'CURRENT_TIMESTAMP',
        //     TableDefinition::TYPE_BLOB => 'CURRENT_TIMESTAMP',
        // ];
        $seq = "{$table}_seq";
        $pk_start_with = $definition->getPrimaryKeyStartWith($this->config['shard']);
        $pk_increment_by = $definition->getPrimaryKeyIncrementBy($this->config['shard']);
        $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$seq}{$q} CASCADE";
        $commands[] = "
            CREATE SEQUENCE {$q}{$seq}{$q}
            AS BIGINT
            START WITH {$pk_start_with}
            INCREMENT BY {$pk_increment_by}
            MINVALUE {$pk_start_with}
        ";
        $alters[] = "
            ALTER TABLE {$q}{$table}{$q}
            ADD CONSTRAINT {$q}{$table}_{$pk}_pk{$q}
            PRIMARY KEY ({$q}{$pk}{$q})
        ";
        $_fields[] = "
            {$q}{$pk}{$q} BIGINT DEFAULT
            nextval('{$seq}'::regclass) NOT NULL
        ";
        $uniques = [];
        // $indexes = [];
        foreach ($fields as $field => $meta) {
            [
                'is_nullable' => $is_nullable,
                'type' => $type,
                'default' => $default,
                'unique' => $unique,
            ] = $meta;
            foreach ($unique as $_) {
                @ $uniques[$_][] = $field;
            }
            $default = $default ?? ($is_nullable ? 'NULL' : $defaults[$type]);
            $default = (string) $default;
            // $default = array_key_exists('database_default', $meta) ? $meta['database_default'] : ();

            // $ ?? 
            $_field = $q . $field . $q . ' ' . $map[$type];
            if ($default !== '') {
                $_field .= ' DEFAULT ' . $default;
            }
            $_field .= ' ' . ($is_nullable ? 'NULL' : 'NOT NULL');
            $_fields[] = $_field;
        }
        foreach ($uniques as $unique => $fs) {
            $fs = implode(', ', array_map(function ($f) use ($q) {
                return $q . $f . $q;
            }, $fs));
            $alters[] = "ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$unique} UNIQUE ({$fs})";
        }
        // foreach ($indexes as $name => [$type, $fs]) {
        //     $fs = implode(', ', array_map(function ($f) use ($q) {
        //         return $q . $f . $q;
        //     }, $fs));
        //     $alters[] = "CREATE INDEX {$q}{$name}{$q} ON {$q}{$table}{$q} USING {$type} ({$fs})";
        // }
        $_fields = implode(', ', $_fields);
        $commands[] = "CREATE TABLE {$q}{$table}{$q} ({$_fields})";
        // print_r($commands);
        // print_r($alters);
        return array_merge($commands, $alters);
    }

    /**
     * @param TableDefinition $definition
     * @param array           $values
     *
     * @return array
     */
    private function insertCommand(TableDefinition $definition, array $values): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPrimaryKey();
        $_columns = [];
        $_values = [];
        $args = [];
        foreach ($values as $value) {
            $f = $q . $value['field'] . $q;
            $_columns[] = $f;
            $_values[] = str_replace('%s', $f, $value['set_pattern']);
            $args[] = $value['set'] ? $value['set']($value['value']) : $value['value'];
        }
        $_columns = implode(', ', $_columns);
        $_values = implode(', ', $_values);
        $insert = ($_columns && $_values) ? "({$_columns}) VALUES ({$_values})" : 'DEFAULT VALUES';
        $command = "INSERT INTO {$q}{$table}{$q} {$insert} RETURNING {$q}{$pk}{$q}";
        return [$command, $args];
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id
     * @param array           $values
     *
     * @return array
     */
    private function updateCommand(TableDefinition $definition, int $id, array $values): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPrimaryKey();
        $args = [];
        $update = [];
        foreach ($values as $value) {
            $f = $q . $value['field'] . $q;
            $update[] = $f . ' = ' . str_replace('%s', $f, $value['set_pattern']);
            $args[] = $value['set'] ? $value['set']($value['value']) : $value['value'];
        }
        $update = implode(', ', $update);
        $args[] = $id;
        $command = "UPDATE {$q}{$table}{$q} SET {$update} WHERE {$q}{$pk}{$q} = ?";
        return [$command, $args];
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id
     * @param mixed           $fields
     *
     * @return array
     */
    private function getCommand(TableDefinition $definition, int $id, $fields): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPrimaryKey();
        $args = [$id];
        $where = "{$q}{$pk}{$q} = ?";
        $_fields = [];
        foreach ($fields as $alias => $field) {
            $f = str_replace('%s', $q . $field['field'] . $q, $field['get_pattern']);
            $_fields[] = $f . ' AS ' . $q . $alias . $q;
        }
        $_fields = implode(', ', $_fields);
        $command = "SELECT {$_fields} FROM {$q}{$table}{$q} WHERE {$where} LIMIT 1";
        return [$command, $args];
    }

    /**
     * @param array $where
     *
     * @return array
     */
    private function flattenWhere(array $where): array
    {
        if (!$where) {
            return ['(TRUE)', []];
        }
        $q = $this->config['quote'];
        foreach ($where as $key => &$value) {
            $args[] = $value;
            $value = "({$q}{$key}{$q} = ?)";
        }
        unset($value);
        return ['(' . implode(' AND ', $where) . ')', $args];
    }
}


/*
$_columns = [];
        $_values = [];
        $args = [];
        foreach ($values as $key => $value) {
            $_columns[] = $q . $key . $q;
            $_values[] = '?';
            $args[] = $value;
        }
        // ("field") AS "field", 
        $_columns = implode(', ', $_columns);
        $_values = implode(', ', $_values);
        $insert = ($_columns && $_values) ? "({$_columns}) VALUES ({$_values})" : 'DEFAULT VALUES';
        $command = "INSERT INTO {$q}{$table}{$q} {$insert} RETURNING {$q}{$pk}{$q}";
        // print_r($command);
        // print_r($args);
        return [$command, $args];

        $quote = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPk();
        
        // $fields = $definition->getFields();
        // $select = is_numeric($select) ? 
        // $_columns = [];
        // $_values = [];
        // $args = [];
        // $pk = '';
        // foreach ($fields as $field => $meta) {
        //     if ($meta['type'] === 'primary') {
        //         $pk = $field;
        //         continue;
        //     }
        //     $_columns[] = $q . $field . $q;
        //     if (array_key_exists($field, $values)) {
        //         $args[] = $values[$field];
        //         $_values[] = '?';
        //     } else {
        //         $_values[] = 'DEFAULT';
        //     }
        // }
        // // $_columns = implode(', ', $_columns);
        // // $_values = implode(', ', $_values);
        // // $insert = $_columns && $_values ? "({$_columns}) VALUES ({$_values})" : 'DEFAULT VALUES';
        // print_r($command);
 */
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
    
    // }