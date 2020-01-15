<?php

namespace Ejz;

use Amp\Loop;
use Amp\Promise;
use Amp\Iterator;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\PgSqlCommandResult;

class DatabasePostgres implements DatabaseInterface
{
    /** @var string */
    protected $name;

    /** @var ConnectionConfig */
    protected $connectionConfig;

    /** @var array */
    protected $config;

    /** @var ?Connection */
    protected $connection;

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
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function exec(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            if ($this->connection === null) {
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
     * @return Promise
     */
    public function all(string $sql, ...$args): Promise
    {
        return $this->exec($sql, ...$args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function one(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->all($sql, ...$args);
            return $all ? $all[0] : [];
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function val(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $one = yield $this->one($sql, ...$args);
            $vals = array_values($one);
            return $vals[0] ?? null;
        }, $sql, $args);
    }

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function col(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->all($sql, ...$args);
            return $this->all2col($all);
        }, $sql, $args);
    }

    /**
     * @return Promise
     */
    public function tables(): Promise
    {
        return \Amp\call(function () {
            $sql = '
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = ?
            ';
            return yield $this->col($sql, 'public');
        });
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function fields(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!yield $this->tableExists($table)) {
                return null;
            }
            $sql = '
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
            ';
            return yield $this->col($sql, 'public', $table);
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pk(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!yield $this->tableExists($table)) {
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
            return yield $this->col($sql, $table, 'public');
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function min(string $table): Promise
    {
        return $this->minMax($table, true);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function max(string $table): Promise
    {
        return $this->minMax($table, false);
    }

    /**
     * @param string $table
     * @param bool   $asc
     *
     * @return Promise
     */
    private function minMax(string $table, bool $asc): Promise
    {
        return \Amp\call(function ($table, $asc) {
            $q = $this->config['quote'];
            $pk = yield $this->pk($table);
            if ($pk === null || $pk === []) {
                return $pk ?? null;
            }
            $asc = $asc ? 'ASC' : 'DESC';
            [$select, $order] = [[], []];
            foreach ($pk as $field) {
                $_ = $q . $field . $q;
                $select[] = $_;
                $order[] = $_ . ' ' . $asc;
            }
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
            $sql = sprintf(
                $sql,
                implode(', ', $select),
                $q . $table . $q,
                implode(', ', $order)
            );
            $one = yield $this->one($sql);
            return $one ? array_values($one) : array_fill(0, count($pk), null);
        }, $table, $asc);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function truncate(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!yield $this->tableExists($table)) {
                return false;
            }
            $q = $this->config['quote'];
            yield $this->exec("TRUNCATE {$q}{$table}{$q} CASCADE");
            return true;
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function drop(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!yield $this->tableExists($table)) {
                return false;
            }
            $q = $this->config['quote'];
            yield $this->exec("DROP TABLE {$q}{$table}{$q} CASCADE");
            return true;
        }, $table);
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
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->getName();
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function tableExists(string $table): Promise
    {
        return \Amp\call(function ($table) {
            return in_array($table, yield $this->tables());
        }, $table);
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function iterate(string $table, array $params = []): Iterator
    {
        $emit = function ($emit) use ($table, $params) {
            $params += [
                'fields' => null,
                'asc' => true,
                'rand' => false,
                'min' => null,
                'max' => null,
                'limit' => 1E9,
                'pk' => null,
                'order' => true,
                'where' => [],
                'config' => [],
            ];
            [
                'fields' => $fields,
                'asc' => $asc,
                'rand' => $rand,
                'min' => $min,
                'max' => $max,
                'limit' => $limit,
                'pk' => $pk,
                'order' => $order,
                'where' => $where,
                'config' => $config,
            ] = $params;
            $config += $this->config;
            [
                'quote' => $q,
                'iterator_chunk_size' => $iterator_chunk_size,
                'rand_iterator_intervals' => $rand_iterator_intervals,
            ] = $config;
            $pk = $pk ?? yield $this->pk($table);
            if ($pk === null || count($pk) !== 1) {
                return;
            }
            $fields = $fields ?? yield $this->fields($table);
            if ($rand) {
                $min = $min ?? (yield $this->min($table))[0];
                $max = $max ?? (yield $this->max($table))[0];
                if (!isset($min, $max)) {
                    return;
                }
                $rand = false;
                $params = compact('fields', 'pk', 'rand') + $params;
                if (is_int($min) && is_int($max)) {
                    $rand_iterator_intervals = min($rand_iterator_intervals, $max - $min + 1);
                    $intervals = $this->getIntervalsForRandIterator($min, $max, $rand_iterator_intervals);
                } else {
                    $intervals = [[$min, $max]];
                }
                $c = count($intervals);
                while ($c) {
                    $key = array_rand($intervals);
                    if (!$intervals[$key] instanceof Iterator) {
                        [$min, $max] = $intervals[$key];
                        $asc = (bool) mt_rand(0, 1);
                        $params = compact('asc', 'min', 'max') + $params;
                        $intervals[$key] = $this->iterate($table, $params);
                    }
                    if (yield $intervals[$key]->advance()) {
                        yield $emit($intervals[$key]->getCurrent());
                    } else {
                        unset($intervals[$key]);
                        $c--;
                    }
                }
                return;
            }
            [$pk] = $pk;
            $qpk = $q . $pk . $q;
            $collect = [];
            foreach ($fields as $field) {
                if (!$field instanceof Field) {
                    $field = new Field($field);
                }
                $collect[$field->getAlias()] = $field;
            }
            $fields = $collect;
            $select = array_map(function ($field) use ($q) {
                return $field->getSelectString($q);
            }, $fields);
            $_pk = 'pk_' . md5($pk);
            $select[] = $qpk . ' AS ' . $q . $_pk . $q;
            $order = $order ? $qpk . ' ' . ($asc ? 'ASC' : 'DESC') : '';
            $order = $order ? 'ORDER BY ' . $order : '';
            $template = sprintf(
                'SELECT %s FROM %s %%s %s LIMIT %%s',
                implode(', ', $select),
                $q . $table . $q,
                $order
            );
            [$op1, $op2] = $asc ? ['>', '<='] : ['<', '>='];
            $where = $where instanceof Condition ? $where : new Condition($where);
            while ($limit > 0) {
                $where->reset();
                if (($asc && $min !== null) || (!$asc && $max !== null)) {
                    $first = $first ?? true;
                    $where->push($pk, $op1 . ($first ? '=' : ''), $asc ? $min : $max);
                }
                if (($asc && $max !== null) || (!$asc && $min !== null)) {
                    $where->push($pk, $op2, $asc ? $max : $min);
                }
                [$_where, $_args] = $where->stringify($q);
                $_where = $_where ? 'WHERE ' . $_where : '';
                $sql = sprintf($template, $_where, min($limit, $iterator_chunk_size));
                $all = yield $this->all($sql, ...$_args);
                if (!$all) {
                    break;
                }
                foreach ($all as $row) {
                    $id = $row[$_pk];
                    unset($row[$_pk]);
                    foreach ($row as $k => &$v) {
                        $f = $fields[$k];
                        $f->importValue($v);
                        $v = $f->getValue();
                    }
                    unset($v);
                    yield $emit([$id, $row]);
                }
                $limit -= count($all);
                ${$asc ? 'min' : 'max'} = $id;
                $first = false;
            }
        };
        return new class($emit) extends Producer {
            use GeneratorTrait;
        };
    }

    /**
     * @param string $table
     * @param array  $ids
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function get(string $table, array $ids, array $params = []): Iterator
    {
        $emit = function ($emit) use ($table, $ids, $params) {
            $params += [
                'pk' => null,
                'fields' => null,
                'config' => [],
            ];
            [
                'pk' => $pk,
                'fields' => $fields,
                'config' => $config,
            ] = $params;
            $config += $this->config;
            [
                'iterator_chunk_size' => $iterator_chunk_size,
            ] = $config;
            $fields = $fields ?? yield $this->fields($table);
            $pk = $pk ?? yield $this->pk($table);
            if ($pk === null || count($pk) !== 1) {
                return;
            }
            [$pk] = $pk;
            foreach (array_chunk($ids, $iterator_chunk_size) as $chunk) {
                $iterator = $this->iterate($table, [
                    'where' => new Condition([$pk => $chunk]),
                    'fields' => $fields,
                    'limit' => $iterator_chunk_size,
                    'config' => compact('iterator_chunk_size'),
                    'order' => false,
                ]);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
            }
        };
        return new class($emit) extends Producer {
            use GeneratorTrait;
        };
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
    //  * @param Repository $repository
    //  *
    //  * @return Promise
    //  */
    // public function createAsync(Repository $repository): Promise
    // {
    //     return \Amp\call(function ($repository) {
    //         $commands = $this->getCreateCommands($repository);
    //         foreach ($commands as $command) {
    //             yield $this->execAsync($command);
    //         }
    //     }, $repository);
    // }

    // /**
    //  * @param Repository $repository
    //  *
    //  * @return array
    //  */
    // private function getCreateCommands(Repository $repository): array
    // {
    //     $q = $this->config['quote'];
    //     $rand = function () {
    //         return mt_rand();
    //     };
    //     $enq = function ($fields) use ($q) {
    //         return implode(', ', array_map(function ($field) use ($q) {
    //             return $q . $field . $q;
    //         }, $fields));
    //     };
    //     $table = $repository->getTable();
    //     [$fields, $indexes] = [[], []];
    //     if (!$repository->isForeignKeyTable($this->getName())) {
    //         $fields = $repository->getFields();
    //         $indexes = $repository->getIndexes();
    //         $foreignKeys = $repository->getForeignKeys();
    //     }
    //     $pk = $repository->getPk();
    //     $pkStartWith = $repository->getPkStartWith($this->getName());
    //     $pkIncrementBy = $repository->getPkIncrementBy($this->getName());
    //     $seq = $table . '_seq';
    //     $commands = [];
    //     // CREATE TABLE
    //     $commands[] = "CREATE TABLE {$q}{$table}{$q}()";
    //     // CREATE SEQUENCE
    //     $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$seq}{$q} CASCADE";
    //     $commands[] = "
    //         CREATE SEQUENCE {$q}{$seq}{$q}
    //         AS BIGINT
    //         START WITH {$pkStartWith}
    //         INCREMENT BY {$pkIncrementBy}
    //         MINVALUE {$pkStartWith}
    //     ";
    //     // ADD PRIMARY KEY
    //     $commands[] = "
    //         ALTER TABLE {$q}{$table}{$q}
    //         ADD COLUMN {$q}{$pk}{$q} BIGINT DEFAULT
    //         nextval('{$seq}'::regclass) NOT NULL
    //     ";
    //     $commands[] = "
    //         ALTER TABLE {$q}{$table}{$q}
    //         ADD CONSTRAINT {$q}{$table}_{$rand()}{$q}
    //         PRIMARY KEY ({$q}{$pk}{$q})
    //     ";
    //     // FIELDS
    //     foreach ($fields as $field) {
    //         $type = $field->getType();
    //         $null = $type->isNullable() ? 'NULL' : 'NOT NULL';
    //         $default = !$type->isNullable() ? $this->getDefault($type) : '';
    //         $default = $default ? 'DEFAULT ' . $default : '';
    //         $type = $this->getType($type);
    //         $commands[] = "
    //             ALTER TABLE {$q}{$table}{$q}
    //             ADD COLUMN {$q}{$field}{$q}
    //             {$type} {$null} {$default}
    //         ";
    //     }
    //     // INDEXES
    //     foreach ($indexes as $index) {
    //         $f = $index->getFields();
    //         $t = $index->getType();
    //         $commands[] = "
    //             CREATE INDEX {$q}{$table}_{$rand()}{$q} ON {$q}{$table}{$q}
    //             USING {$t} ($enq($f))
    //         ";
    //     }
    //     // FOREIGN KEYS
    //     foreach ($foreignKeys as $foreignKey) {
    //         $pt = $foreignKey->getParentTable();
    //         $cf = $foreignKey->getChildFields();
    //         $pf = $foreignKey->getParentFields();
    //         $commands[] = "
    //             ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$table}_{$rand()}{$q}
    //             FOREIGN KEY ({$enq($cf)}) REFERENCES {$q}{$pt}{$q} ({$enq($pf)})
    //             ON DELETE CASCADE ON UPDATE CASCADE
    //         ";
    //     }
    //     $commands = array_map('trim', $commands);
    //     return $commands;
    // }

    // /**
    //  * @param Repository $repository
    //  * @param array      $values
    //  *
    //  * @return Promise
    //  */
    // public function insertAsync(Repository $repository, array $values): Promise
    // {
    //     return \Amp\call(function ($repository, $values) {
    //         [$cmd, $args] = $this->getInsertCommand($repository, $values);
    //         return yield $this->valAsync($cmd, ...$args);
    //     }, $repository, $values);
    // }

    // /**
    //  * @param Repository $repository
    //  * @param array      $fields
    //  *
    //  * @return array
    //  */
    // private function getInsertCommand(Repository $repository, array $fields): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $repository->getTable();
    //     $pk = $repository->getPk();
    //     if ($repository->isForeignKeyTable($this->getName())) {
    //         $fields = [];
    //     }
    //     [$columns, $values, $args] = [[], [], []];
    //     foreach ($fields as $field) {
    //         $columns[] = $q . ((string) $field) . $q;
    //         $values[] = $field->getInsertString($q);
    //         $args[] = $field->getValue();
    //         // $f = $q . $value['field'] . $q;
    //         // $_values[] = str_replace('%s', $f, $value['set_pattern']);
    //         // $args[] = $value['set'] ? $value['set']($value['value']) : $value['value'];
    //     }
    //     $columns = implode(', ', $columns);
    //     $values = implode(', ', $values);
    //     $insert = ($columns && $values) ? "({$columns}) VALUES ({$values})" : 'DEFAULT VALUES';
    //     $command = "INSERT INTO {$q}{$table}{$q} {$insert} RETURNING {$q}{$pk}{$q}";
    //     return [$command, $args];
    //     // $_columns = [];
    //     // $_values = [];
    //     // $args = [];
    // }

    

    // /**
    //  * @param AbstractType $type
    //  *
    //  * @return string
    //  */
    // private function getType(AbstractType $type): string
    // {
    //     static $map;
    //     if ($map === null) {
    //         $map = [
    //             (string) Type::string() => 'TEXT',
    //             (string) Type::int() => 'INTEGER',
    //             (string) Type::float() => 'REAL',
    //             (string) Type::bool() => 'BOOLEAN',
    //             (string) Type::date() => 'DATE',
    //             (string) Type::dateTime() => 'TIMESTAMP(0) WITHOUT TIME ZONE',
    //             (string) Type::json() => 'JSONB',
    //             (string) Type::foreignKey() => 'BIGINT',
    //             (string) Type::intArray() => 'INTEGER[]',
    //             (string) Type::stringArray() => 'TEXT[]',
    //             (string) Type::binary() => 'BYTEA',
    //         ];
    //     }
    //     return $map[(string) $type];
    // }

    // /**
    //  * @param AbstractType $type
    //  *
    //  * @return string
    //  */
    // private function getDefault(AbstractType $type): string
    // {
    //     static $map;
    //     if ($map === null) {
    //         $map = [
    //             (string) Type::string() => "''::TEXT",
    //             (string) Type::int() => '0',
    //             (string) Type::float() => '0',
    //             (string) Type::bool() => "'f'",
    //             (string) Type::date() => 'CURRENT_DATE',
    //             (string) Type::dateTime() => 'CURRENT_TIMESTAMP',
    //             (string) Type::json() => "'{}'",
    //             (string) Type::foreignKey() => '0',
    //             (string) Type::intArray() => "'{}'",
    //             (string) Type::stringArray() => "'{}'",
    //             (string) Type::binary() => "''::BYTEA",
    //         ];
    //     }
    //     return $map[(string) $type];
    // }

    //     // $alters = [];
    //     // $_fields = [];
    //     // $map = [
    //     //     TableDefinition::TYPE_INT => 'INTEGER',
    //     //     TableDefinition::TYPE_BLOB => 'BYTEA',
    //     //     TableDefinition::TYPE_TEXT => 'TEXT',
    //     //     TableDefinition::TYPE_JSON => 'JSONB',
    //     //     TableDefinition::TYPE_FOREIGN_KEY => 'BIGINT',
    //     //     TableDefinition::TYPE_BOOL => 'BOOLEAN',
    //     //     TableDefinition::TYPE_FLOAT => 'REAL',
    //     //     TableDefinition::TYPE_DATE => 'DATE',
    //     //     TableDefinition::TYPE_DATETIME => 'TIMESTAMP(0) WITHOUT TIME ZONE',
    //     //     TableDefinition::TYPE_INT_ARRAY => 'INTEGER[]',
    //     //     TableDefinition::TYPE_TEXT_ARRAY => 'TEXT[]',
    //     // ];
    //     // $defaults = [
    //     //     TableDefinition::TYPE_INT => '0',
    //     //     TableDefinition::TYPE_FOREIGN_KEY => '0',
    //     //     TableDefinition::TYPE_BLOB => '\'\'::BYTEA',
    //     //     TableDefinition::TYPE_TEXT => ,
    //     //     TableDefinition::TYPE_JSON => '\'{}\'::JSONB',
    //     //     TableDefinition::TYPE_BOOL => '\'f\'::BOOLEAN',
    //     //     TableDefinition::TYPE_FLOAT => '0',
    //     //     TableDefinition::TYPE_DATE => 'CURRENT_DATE',
    //     //     TableDefinition::TYPE_DATETIME => 'CURRENT_TIMESTAMP',
    //     //     TableDefinition::TYPE_INT_ARRAY => '\'{}\'::INTEGER[]',
    //     //     TableDefinition::TYPE_TEXT_ARRAY => '\'{}\'::TEXT[]',
    //     // ];
    // // /**
    // //  * @param TableDefinition $definition
    // //  *
    // //  * @return Promise
    // //  */
    // // public function createAsync(TableDefinition $definition): Promise
    // // {
    // //     return \Amp\call(function ($definition) {
    // //         yield $this->dropAsync($definition->getTable());
    // //         $commands = $this->createCommands($definition);
    // //         foreach ($commands as $command) {
    // //             yield $this->execAsync($command);
    // //         }
    // //     }, $definition);
    // // }

    

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param int             $id
    // //  * @param array           $values
    // //  *
    // //  * @return Promise
    // //  */
    // // public function updateAsync(TableDefinition $definition, int $id, array $values): Promise
    // // {
    // //     return \Amp\call(function ($definition, $id, $values) {
    // //         if (!$values) {
    // //             return false;
    // //         }
    // //         [$cmd, $args] = $this->updateCommand($definition, $id, $values);
    // //         return yield $this->execAsync($cmd, ...$args);
    // //     }, $definition, $id, $values);
    // // }

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param array           $ids
    // //  * @param mixed           $fields
    // //  *
    // //  * @return Promise
    // //  */
    // // public function getAsync(TableDefinition $definition, array $ids, $fields): Promise
    // // {
    // //     return \Amp\call(function ($definition, $ids, $fields) {
    // //         [$cmd, $args, $pk] = $this->getCommand($definition, $ids, $fields);
    // //         $all = yield $this->allAsync($cmd, ...$args);
    // //         $collect = [];
    // //         foreach ($all as $row) {
    // //             $id = $row[$pk];
    // //             unset($row[$pk]);
    // //             foreach ($row as $k => &$v) {
    // //                 $get = $fields[$k]['get'] ?? null;
    // //                 $v = $get === null ? $v : $get($v);
    // //             }
    // //             unset($v);
    // //             $collect[$id] = $row;
    // //         }
    // //         return $collect;
    // //     }, $definition, $ids, $fields);
    // // }

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param int             $id1
    // //  * @param int             $id2
    // //  *
    // //  * @return Promise
    // //  */
    // // public function reidAsync(TableDefinition $definition, int $id1, int $id2): Promise
    // // {
    // //     return \Amp\call(function ($definition, $id1, $id2) {
    // //         [$cmd, $args] = $this->reidCommand($definition, $id1, $id2);
    // //         return yield $this->execAsync($cmd, ...$args);
    // //     }, $definition, $id1, $id2);
    // // }

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param int             $id1
    // //  *
    // //  * @return Promise
    // //  */
    // // public function deleteAsync(TableDefinition $definition, int $id): Promise
    // // {
    // //     return \Amp\call(function ($definition, $id) {
    // //         [$cmd, $args] = $this->deleteCommand($definition, $id);
    // //         return yield $this->execAsync($cmd, ...$args);
    // //     }, $definition, $id);
    // // }

    

    

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param int             $id
    // //  * @param array           $values
    // //  *
    // //  * @return array
    // //  */
    // // private function updateCommand(TableDefinition $definition, int $id, array $values): array
    // // {
    // //     $q = $this->config['quote'];
    // //     $table = $definition->getTable();
    // //     $pk = $definition->getPrimaryKey();
    // //     $args = [];
    // //     $update = [];
    // //     foreach ($values as $value) {
    // //         $f = $q . $value['field'] . $q;
    // //         $update[] = $f . ' = ' . str_replace('%s', $f, $value['set_pattern']);
    // //         $args[] = $value['set'] ? $value['set']($value['value']) : $value['value'];
    // //     }
    // //     $update = implode(', ', $update);
    // //     $args[] = $id;
    // //     $command = "UPDATE {$q}{$table}{$q} SET {$update} WHERE {$q}{$pk}{$q} = ?";
    // //     return [$command, $args];
    // // }

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param int             $id1
    // //  * @param int             $id2
    // //  *
    // //  * @return array
    // //  */
    // // private function reidCommand(TableDefinition $definition, int $id1, int $id2): array
    // // {
    // //     $q = $this->config['quote'];
    // //     $table = $definition->getTable();
    // //     $pk = $definition->getPrimaryKey();
    // //     $args = [$id2, $id1];
    // //     $command = "UPDATE {$q}{$table}{$q} SET {$q}{$pk}{$q} = ? WHERE {$q}{$pk}{$q} = ?";
    // //     return [$command, $args];
    // // }

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param int             $id
    // //  *
    // //  * @return array
    // //  */
    // // private function deleteCommand(TableDefinition $definition, int $id): array
    // // {
    // //     $q = $this->config['quote'];
    // //     $table = $definition->getTable();
    // //     $pk = $definition->getPrimaryKey();
    // //     $args = [$id];
    // //     $command = "DELETE FROM {$q}{$table}{$q} WHERE {$q}{$pk}{$q} = ?";
    // //     return [$command, $args];
    // // }

    // // /**
    // //  * @param TableDefinition $definition
    // //  * @param array           $ids
    // //  * @param mixed           $fields
    // //  *
    // //  * @return array
    // //  */
    // // private function getCommand(TableDefinition $definition, array $ids, $fields): array
    // // {
    // //     $q = $this->config['quote'];
    // //     $table = $definition->getTable();
    // //     $pk = $definition->getPrimaryKey();
    // //     $ids = array_map('intval', $ids);
    // //     $_ = implode(', ', array_fill(0, count($ids), '?'));
    // //     $where = "{$q}{$pk}{$q} IN ({$_})";
    // //     $_pk = '_pk_' . mt_rand();
    // //     $_fields = ["{$q}{$pk}{$q} AS {$q}{$_pk}{$q}"];
    // //     foreach ($fields as $alias => $field) {
    // //         $f = str_replace('%s', $q . $field['field'] . $q, $field['get_pattern']);
    // //         $_fields[] = $f . ' AS ' . $q . $alias . $q;
    // //     }
    // //     $_fields = implode(', ', $_fields);
    // //     $command = "SELECT {$_fields} FROM {$q}{$table}{$q} WHERE {$where}";
    // //     return [$command, $ids, $_pk];
    // // }

    // // /**
    // //  * @param array $where
    // //  *
    // //  * @return array
    // //  */
    // // private function flattenWhere(array $where): array
    // // {
    // //     if (!$where) {
    // //         return ['(TRUE)', []];
    // //     }
    // //     $q = $this->config['quote'];
    // //     $collect = [];
    // //     foreach ($where as $key => $value) {
    // //         if (is_array($value) && count($value)) {
    // //             $args = array_merge($args, $value);
    // //             $_ = implode(', ', array_fill(0, count($value), '?'));
    // //             $collect[] = "({$q}{$key}{$q} IN ({$_}))";
    // //         } elseif (!is_array($value)) {
    // //             $args[] = $value;
    // //             $collect[] = "({$q}{$key}{$q} = ?)";
    // //         }
    // //     }
    // //     return ['(' . implode(' AND ', $collect) . ')', $args];
    // // }
}
