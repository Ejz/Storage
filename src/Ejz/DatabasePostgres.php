<?php

namespace Ejz;

use Amp\Loop;
use Amp\Promise;
use Amp\Iterator;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\PgSqlCommandResult;
use Ejz\Type\AbstractType;

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
        $sql = '
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = ?
        ';
        return $this->col($sql, 'public');
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
    public function count(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!yield $this->tableExists($table)) {
                return null;
            }
            $q = $this->config['quote'];
            $sql = "SELECT COUNT(1) FROM {$q}{$table}{$q}";
            return (int) yield $this->val($sql);
        }, $table);
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function indexes(string $table): Promise
    {
        return \Amp\call(function ($table) {
            if (!yield $this->tableExists($table)) {
                return null;
            }
            $sql = '
                SELECT i.relname AS i, a.attname AS c
                FROM
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a,
                    pg_namespace
                WHERE
                    t.oid = ix.indrelid AND
                    i.oid = ix.indexrelid AND
                    a.attrelid = t.oid AND
                    a.attnum = ANY(ix.indkey) AND
                    t.relkind = ? AND
                    t.relname = ? AND
                    nspname = ? AND
                    NOT indisprimary
            ';
            $all = yield $this->all($sql, 'r', $table, 'public');
            $indexes = [];
            foreach ($all as ['i' => $i, 'c' => $c]) {
                @ $indexes[$i][] = $c;
            }
            return $indexes;
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
     */
    public function close()
    {
        if ($this->connection !== null) {
            $this->connection->close();
        }
    }

    /**
     */
    public function __destruct()
    {
        $this->close();
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
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function iterate(string $table, array $params = []): Iterator
    {
        $emit = function ($emit) use ($table, $params) {
            $params += [
                'fields' => null,
                'returnFields' => false,
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
                'returnFields' => $returnFields,
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
                $as = $q . $field->getAlias() . $q;
                return $field->getSelectString($q) . ' AS ' . $as;
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
                $lim = min($limit, $iterator_chunk_size);
                $sql = sprintf($template, $_where, $lim);
                $all = yield $this->all($sql, ...$_args);
                $c = count($all);
                $limit -= $c;
                foreach ($all as $row) {
                    $id = $row[$_pk];
                    unset($row[$_pk]);
                    foreach ($row as $k => &$v) {
                        $f = $fields[$k];
                        $f->importValue($v);
                        $v = $returnFields ? clone $f : $f->getValue();
                    }
                    unset($v);
                    yield $emit([$id, $row]);
                }
                if (!$c || $c < $lim) {
                    break;
                }
                ${$asc ? 'min' : 'max'} = $id;
                $first = false;
            }
        };
        return new Producer($emit);
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
                'returnFields' => false,
                'order' => false,
                'config' => [],
            ];
            [
                'pk' => $pk,
                'fields' => $fields,
                'returnFields' => $returnFields,
                'order' => $order,
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
                    'pk' => [$pk],
                    'fields' => $fields,
                    'returnFields' => $returnFields,
                    'limit' => $iterator_chunk_size,
                    'config' => compact('iterator_chunk_size'),
                    'order' => $order,
                ]);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
            }
        };
        return new Producer($emit);
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
     * @param Repository $repository
     *
     * @return Promise
     */
    public function create(Repository $repository): Promise
    {
        return \Amp\call(function ($repository) {
            $commands = $this->getCreateCommands($repository);
            foreach ($commands as $command) {
                yield $this->exec($command);
            }
        }, $repository);
    }
    
    /**
     * @param Repository $repository
     *
     * @return array
     */
    private function getCreateCommands(Repository $repository): array
    {
        $q = $this->config['quote'];
        $enq = function ($fields) use ($q) {
            return implode(', ', array_map(function ($field) use ($q) {
                return $q . $field . $q;
            }, $fields));
        };
        $table = $repository->getDatabaseTable();
        [$fields, $indexes, $foreignKeys] = [[], [], []];
        $primary = $repository->getPrimaryDatabasePool()->names();
        if (in_array($this->getName(), $primary)) {
            $fields = $repository->getDatabaseFields();
            $indexes = $repository->getDatabaseIndexes();
            $foreignKeys = $repository->getDatabaseForeignKeys();
        }
        $pk = $repository->getDatabasePk();
        $pkStartWith = $repository->getDatabasePkStartWith($this->getName());
        $pkIncrementBy = $repository->getDatabasePkIncrementBy($this->getName());
        $seq = $table . '_seq';
        $commands = [];
        // CREATE TABLE
        $commands[] = "CREATE TABLE {$q}{$table}{$q}()";
        // CREATE SEQUENCE
        $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$seq}{$q} CASCADE";
        $commands[] = "
            CREATE SEQUENCE {$q}{$seq}{$q}
            AS BIGINT
            START WITH {$pkStartWith}
            INCREMENT BY {$pkIncrementBy}
            MINVALUE {$pkStartWith}
        ";
        // ADD PRIMARY KEY
        $commands[] = "
            ALTER TABLE {$q}{$table}{$q}
            ADD COLUMN {$q}{$pk}{$q} BIGINT DEFAULT
            nextval('{$seq}'::regclass) NOT NULL
        ";
        $commands[] = "
            ALTER TABLE {$q}{$table}{$q}
            ADD CONSTRAINT {$q}{$table}_pk{$q}
            PRIMARY KEY ({$q}{$pk}{$q})
        ";
        // FIELDS
        foreach ($fields as $field) {
            $type = $field->getType();
            $null = $type->isNullable() ? 'NULL' : 'NOT NULL';
            $default = !$type->isNullable() ? $this->getFieldTypeDefault($type) : '';
            $default = $default !== '' ? 'DEFAULT ' . $default : '';
            $type = $this->getFieldTypeString($type);
            $commands[] = "
                ALTER TABLE {$q}{$table}{$q}
                ADD COLUMN {$q}{$field}{$q}
                {$type} {$null} {$default}
            ";
        }
        // INDEXES
        foreach ($indexes as $index) {
            $f = $index->getFields();
            $t = $this->getIndexTypeString($index->getType());
            $u = $index->isUnique() ? 'UNIQUE' : '';
            $commands[] = "
                CREATE {$u} INDEX {$q}{$table}_{$index}{$q} ON {$q}{$table}{$q}
                USING {$t} ({$enq($f)})
            ";
        }
        // FOREIGN KEYS
        foreach ($foreignKeys as $foreignKey) {
            $parentTable = $foreignKey->getParentTable();
            $parentFields = $foreignKey->getParentFields();
            $childFields = $foreignKey->getChildFields();
            $commands[] = "
                ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$table}_{$foreignKey}{$q}
                FOREIGN KEY ({$enq($childFields)})
                REFERENCES {$q}{$parentTable}{$q} ({$enq($parentFields)})
                ON DELETE CASCADE ON UPDATE CASCADE
            ";
        }
        $commands = array_map('trim', $commands);
        return $commands;
    }

    /**
     * @param Repository $repository
     * @param array      $fields
     *
     * @return Promise
     */
    public function insert(Repository $repository, array $fields): Promise
    {
        return \Amp\call(function ($repository, $fields) {
            [$cmd, $args] = $this->getInsertCommand($repository, $fields);
            return yield $this->val($cmd, ...$args);
        }, $repository, $fields);
    }

    /**
     * @param Repository $repository
     * @param array      $fields
     *
     * @return array
     */
    private function getInsertCommand(Repository $repository, array $fields): array
    {
        $q = $this->config['quote'];
        $table = $repository->getDatabaseTable();
        $pk = $repository->getDatabasePk();
        $primary = $repository->getPrimaryDatabasePool()->names();
        if (!in_array($this->getName(), $primary)) {
            $fields = [];
        }
        [$columns, $values, $args] = [[], [], []];
        foreach ($fields as $field) {
            $columns[] = $q . $field->getName() . $q;
            $values[] = $field->getInsertString();
            $args[] = $field->exportValue();
        }
        $columns = implode(', ', $columns);
        $values = implode(', ', $values);
        $insert = ($columns && $values) ? "({$columns}) VALUES ({$values})" : 'DEFAULT VALUES';
        $command = "INSERT INTO {$q}{$table}{$q} {$insert} RETURNING {$q}{$pk}{$q}";
        return [$command, $args];
    }

    /**
     * @param Repository $repository
     * @param array      $ids
     * @param array      $fields
     *
     * @return Promise
     */
    public function update(Repository $repository, array $ids, array $fields): Promise
    {
        return \Amp\call(function ($repository, $ids, $fields) {
            if (!$fields || !$ids) {
                return 0;
            }
            [$cmd, $args] = $this->getUpdateCommand($repository, $ids, $fields);
            return (int) yield $this->exec($cmd, ...$args);
        }, $repository, $ids, $fields);
    }

    /**
     * @param Repository $repository
     * @param array      $ids
     * @param array      $fields
     *
     * @return array
     */
    private function getUpdateCommand(Repository $repository, array $ids, array $fields): array
    {
        $q = $this->config['quote'];
        $table = $repository->getDatabaseTable();
        $pk = $repository->getDatabasePk();
        [$args, $update] = [[], []];
        foreach ($fields as $field) {
            $f = $q . $field->getName() . $q;
            $update[] = $f . ' = ' . $field->getUpdateString($q);
            $args[] = $field->exportValue();
        }
        $update = implode(', ', $update);
        $args = array_merge($args, $ids);
        $_ = implode(', ', array_fill(0, count($ids), '?'));
        $command = "UPDATE {$q}{$table}{$q} SET {$update} WHERE {$q}{$pk}{$q} IN ({$_})";
        return [$command, $args];
    }

    /**
     * @param Repository $repository
     * @param array      $ids
     *
     * @return Promise
     */
    public function delete(Repository $repository, array $ids): Promise
    {
        return \Amp\call(function ($repository, $ids) {
            if (!$ids) {
                return 0;
            }
            [$cmd, $args] = $this->getDeleteCommand($repository, $ids);
            return (int) yield $this->exec($cmd, ...$args);
        }, $repository, $ids);
    }

    /**
     * @param Repository $repository
     * @param array      $ids
     *
     * @return array
     */
    private function getDeleteCommand(Repository $repository, array $ids): array
    {
        $q = $this->config['quote'];
        $table = $repository->getDatabaseTable();
        $pk = $repository->getDatabasePk();
        $_ = implode(', ', array_fill(0, count($ids), '?'));
        $command = "DELETE FROM {$q}{$table}{$q} WHERE {$q}{$pk}{$q} IN ({$_})";
        return [$command, $ids];
    }

    /**
     * @param Repository $repository
     * @param int        $id1
     * @param int        $id2
     *
     * @return Promise
     */
    public function reid(Repository $repository, int $id1, int $id2): Promise
    {
        return \Amp\call(function ($repository, $id1, $id2) {
            [$cmd, $args] = $this->getReidCommand($repository, $id1, $id2);
            return (bool) yield $this->exec($cmd, ...$args);
        }, $repository, $id1, $id2);
    }

    /**
     * @param Repository $repository
     * @param int        $id1
     * @param int        $id2
     *
     * @return array
     */
    private function getReidCommand(Repository $repository, int $id1, int $id2): array
    {
        $q = $this->config['quote'];
        $table = $repository->getTable();
        $pk = $repository->getPk();
        $command = "UPDATE {$q}{$table}{$q} SET {$q}{$pk}{$q} = ? WHERE {$q}{$pk}{$q} = ?";
        return [$command, [$id2, $id1]];
    }

    /**
     * @param AbstractType $type
     *
     * @return string
     */
    private function getFieldTypeString(AbstractType $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) Type::string() => 'TEXT',
                (string) Type::int() => 'INTEGER',
                (string) Type::float() => 'REAL',
                (string) Type::bool() => 'BOOLEAN',
                (string) Type::date() => 'DATE',
                (string) Type::dateTime() => 'TIMESTAMP(0) WITHOUT TIME ZONE',
                (string) Type::json() => 'JSONB',
                (string) Type::bigInt() => 'BIGINT',
                (string) Type::intArray() => 'INTEGER[]',
                (string) Type::stringArray() => 'TEXT[]',
                (string) Type::binary() => 'BYTEA',
                (string) Type::enum() => 'TEXT',
                (string) Type::compressedBinary() => 'BYTEA',
            ];
        }
        return $map[(string) $type];
    }

    /**
     * @param AbstractType $type
     *
     * @return string
     */
    private function getFieldTypeDefault(AbstractType $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) Type::string() => "''::TEXT",
                (string) Type::int() => '0',
                (string) Type::float() => '0',
                (string) Type::bool() => "'f'",
                (string) Type::date() => 'CURRENT_DATE',
                (string) Type::dateTime() => 'CURRENT_TIMESTAMP',
                (string) Type::json() => "'{}'",
                (string) Type::bigInt() => '0',
                (string) Type::intArray() => "'{}'",
                (string) Type::stringArray() => "'{}'",
                (string) Type::binary() => "''::BYTEA",
                (string) Type::compressedBinary() => "''::BYTEA",
            ];
        }
        if ($type->is(Type::enum())) {
            return "'" . $type->getDefault() . "'::TEXT";
        }
        return $map[(string) $type];
    }

    /**
     * @param string $type
     *
     * @return string
     */
    private function getIndexTypeString(string $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                Index::INDEX_TYPE_BTREE => 'BTREE',
                Index::INDEX_TYPE_HASH => 'HASH',
                Index::INDEX_TYPE_GIST => 'GIST',
                Index::INDEX_TYPE_GIN => 'GIN',
                Index::INDEX_TYPE_UNIQUE => 'BTREE',
            ];
        }
        return $map[$type];
    }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        if (substr($name, -4) === 'Sync') {
            $name = substr($name, 0, -4);
            return Promise\wait($this->$name(...$arguments));
        }
    }
}
