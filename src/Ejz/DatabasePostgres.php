<?php

namespace Ejz;

use Amp\Loop;
use Amp\Promise;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\PgSqlCommandResult;
use Ejz\Type\AbstractType;
use RuntimeException;

class DatabasePostgres implements NameInterface, DatabaseInterface
{
    use NameTrait;
    use SyncTrait;

    /** @var ConnectionConfig */
    protected $connectionConfig;

    /** @var array */
    protected $config;

    /** @var ?Connection */
    protected $connection;

    /** @var string */
    private const SEQUENCE_NAME = '%s_seq';

    /** @var string */
    private const PK_CONSTRAINT_NAME = '%s_pk';

    /** @var string */
    private const INDEX_NAME = '%s_%s';

    /** @var string */
    private const FOREIGN_KEY_NAME = '%s_%s';

    /**
     * @param string $name
     * @param string $dsn
     * @param array  $config (optional)
     */
    public function __construct(string $name, string $dsn, array $config = [])
    {
        $this->setName($name);
        $this->connectionConfig = ConnectionConfig::fromString($dsn);
        $this->config = $config + [
            'quote' => '"',
            'iterator_chunk_size' => 100,
            'rand_iterator_intervals' => 100,
        ];
        $this->connection = null;
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
    public function col(string $sql, ...$args): Promise
    {
        return \Amp\call(function ($sql, $args) {
            $all = yield $this->all($sql, ...$args);
            return $this->all2col($all);
        }, $sql, $args);
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
            $sequence = sprintf(self::SEQUENCE_NAME, $table);
            yield $this->exec("DROP SEQUENCE IF EXISTS {$q}{$sequence}{$q} CASCADE");
            return true;
        }, $table);
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
     * @param string          $table
     * @param ?WhereCondition $where (optional)
     *
     * @return Promise
     */
    public function count(string $table, ?WhereCondition $where = null): Promise
    {
        return \Amp\call(function ($table, $where) {
            if (!yield $this->tableExists($table)) {
                return null;
            }
            $q = $this->config['quote'];
            [$where, $args] = $where === null ? ['', []] : $where->stringify($q);
            $sql = "SELECT COUNT(1) FROM {$q}{$table}{$q} {$where}";
            return (int) yield $this->val($sql, ...$args);
        }, $table, $where);
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
     * @param string $field
     *
     * @return Promise
     */
    public function fieldExists(string $table, string $field): Promise
    {
        return \Amp\call(function ($table, $field) {
            return in_array($field, yield $this->fields($table));
        }, $table, $field);
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
     * @param string $index
     *
     * @return Promise
     */
    public function indexExists(string $table, string $index): Promise
    {
        return \Amp\call(function ($table, $index) {
            return array_key_exists($index, yield $this->indexes($table));
        }, $table, $index);
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
     * @param string $pk
     * @param int    $pkStart     (optional)
     * @param int    $pkIncrement (optional)
     * @param array  $fields      (optional)
     * @param array  $indexes     (optional)
     * @param array  $foreignKeys (optional)
     *
     * @return Promise
     */
    public function create(
        string $table,
        string $pk,
        int $pkStart = 1,
        int $pkIncrement = 1,
        array $fields = [],
        array $indexes = [],
        array $foreignKeys = []
    ): Promise {
        return \Amp\call(function (...$args) {
            $commands = $this->getCreateCommands(...$args);
            foreach ($commands as $command) {
                yield $this->exec($command);
            }
        },
            $table, $pk, $pkStart, $pkIncrement,
            $fields, $indexes, $foreignKeys
        );
    }

    /**
     * @param string $table
     * @param string $pk
     * @param int    $pkStart
     * @param int    $pkIncrement
     * @param array  $fields
     * @param array  $indexes
     * @param array  $foreignKeys
     *
     * @return array
     */
    private function getCreateCommands(
        string $table,
        string $pk,
        int $pkStart,
        int $pkIncrement,
        array $fields,
        array $indexes,
        array $foreignKeys
    ): array {
        $q = $this->config['quote'];
        $enq = function ($fields) use ($q) {
            return implode(', ', array_map(function ($field) use ($q) {
                return $q . $field . $q;
            }, $fields));
        };
        $sequence = sprintf(self::SEQUENCE_NAME, $table);
        $commands = [];
        // CREATE TABLE
        $commands[] = "CREATE TABLE {$q}{$table}{$q}()";
        // CREATE SEQUENCE
        $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$sequence}{$q} CASCADE";
        $commands[] = "
            CREATE SEQUENCE {$q}{$sequence}{$q}
            AS BIGINT
            START {$pkStart}
            INCREMENT {$pkIncrement}
            MINVALUE {$pkStart}
            NO CYCLE
        ";
        // ADD PRIMARY KEY
        $commands[] = "
            ALTER TABLE {$q}{$table}{$q}
            ADD COLUMN {$q}{$pk}{$q} BIGINT DEFAULT
            nextval('{$sequence}'::regclass) NOT NULL
        ";
        $_pk = sprintf(self::PK_CONSTRAINT_NAME, $table);
        $commands[] = "
            ALTER TABLE {$q}{$table}{$q}
            ADD CONSTRAINT {$q}{$_pk}{$q}
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
            $_index = sprintf(self::INDEX_NAME, $table, $index);
            $commands[] = "
                CREATE {$u} INDEX {$q}{$_index}{$q} ON {$q}{$table}{$q}
                USING {$t} ({$enq($f)})
            ";
        }
        // FOREIGN KEYS
        foreach ($foreignKeys as $foreignKey) {
            $parentTable = $foreignKey->getParentTable();
            $parentFields = $foreignKey->getParentFields();
            $childFields = $foreignKey->getChildFields();
            $_fk = sprintf(self::FOREIGN_KEY_NAME, $table, (string) $foreignKey);
            $commands[] = "
                ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$_fk}{$q}
                FOREIGN KEY ({$enq($childFields)})
                REFERENCES {$q}{$parentTable}{$q} ({$enq($parentFields)})
                ON DELETE CASCADE ON UPDATE CASCADE
            ";
        }
        $commands = array_map('trim', $commands);
        return $commands;
    }

    /**
     * @param string $table
     * @param string $pk
     * @param array  $fields     (optional)
     *
     * @return Promise
     */
    public function insert(string $table, string $pk, array $fields = []): Promise
    {
        return \Amp\call(function (...$args) {
            [$cmd, $args] = $this->getInsertCommand(...$args);
            return yield $this->val($cmd, ...$args);
        }, $table, $pk, $fields);
    }

    /**
     * @param string $table
     * @param string $pk
     * @param array  $fields
     *
     * @return array
     */
    private function getInsertCommand(string $table, string $pk, array $fields): array
    {
        $q = $this->config['quote'];
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
     * @param string $table
     * @param string $pk
     * @param array  $ids
     * @param array  $fields
     *
     * @return Promise
     */
    public function update(string $table, string $pk, array $ids, array $fields): Promise
    {
        return \Amp\call(function ($table, $pk, $ids, $fields) {
            if (!$fields || !$ids) {
                return 0;
            }
            [$cmd, $args] = $this->getUpdateCommand($table, $pk, $ids, $fields);
            return (int) yield $this->exec($cmd, ...$args);
        }, $table, $pk, $ids, $fields);
    }

    /**
     * @param string $table
     * @param string $pk
     * @param array  $ids
     * @param array  $fields
     *
     * @return array
     */
    private function getUpdateCommand(string $table, string $pk, array $ids, array $fields): array
    {
        $q = $this->config['quote'];
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
     * @param string $table
     * @param string $pk
     * @param array  $ids
     *
     * @return Promise
     */
    public function delete(string $table, string $pk, array $ids): Promise
    {
        return \Amp\call(function ($table, $pk, $ids) {
            if (!$ids) {
                return 0;
            }
            [$cmd, $args] = $this->getDeleteCommand($table, $pk, $ids);
            return (int) yield $this->exec($cmd, ...$args);
        }, $table, $pk, $ids);
    }

    /**
     * @param string $table
     * @param string $pk
     * @param array  $ids
     *
     * @return array
     */
    private function getDeleteCommand(string $table, string $pk, array $ids): array
    {
        $q = $this->config['quote'];
        $_ = implode(', ', array_fill(0, count($ids), '?'));
        $command = "DELETE FROM {$q}{$table}{$q} WHERE {$q}{$pk}{$q} IN ({$_})";
        return [$command, $ids];
    }

    /**
     * @param string $table
     * @param string $pk
     * @param int    $id1
     * @param int    $id2
     *
     * @return Promise
     */
    public function reid(string $table, string $pk, int $id1, int $id2): Promise
    {
        return \Amp\call(function ($table, $pk, $id1, $id2) {
            [$cmd, $args] = $this->getReidCommand($table, $pk, $id1, $id2);
            return (bool) yield $this->exec($cmd, ...$args);
        }, $table, $pk, $id1, $id2);
    }

    /**
     * @param string $table
     * @param string $pk
     * @param int    $id1
     * @param int    $id2
     *
     * @return array
     */
    private function getReidCommand(string $table, string $pk, int $id1, int $id2): array
    {
        $q = $this->config['quote'];
        $command = "UPDATE {$q}{$table}{$q} SET {$q}{$pk}{$q} = ? WHERE {$q}{$pk}{$q} = ?";
        return [$command, [$id2, $id1]];
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
                'min' => null,
                'max' => null,
                'limit' => 1E9,
                'pk' => null,
                'where' => null,
                'config' => [],
            ];
            [
                'fields' => $fields,
                'returnFields' => $returnFields,
                'asc' => $asc,
                'min' => $min,
                'max' => $max,
                'limit' => $limit,
                'pk' => $pk,
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
            if ($asc === null) {
                $min = $min ?? (yield $this->min($table))[0];
                $max = $max ?? (yield $this->max($table))[0];
                if (!isset($min, $max)) {
                    return;
                }
                $params = compact('fields', 'pk') + $params;
                if (is_int($min) && is_int($max)) {
                    $intervals = $this->getIntervalsForRandIterator(
                        $min,
                        $max,
                        min($rand_iterator_intervals, $max - $min + 1)
                    );
                } else {
                    $intervals = [[$min, $max]];
                }
                $iterators = array_map(function ($interval) use ($table, $params) {
                    [$min, $max] = $interval;
                    $asc = (bool) mt_rand(0, 1);
                    return $this->iterate($table, compact('asc', 'min', 'max') + $params);
                }, $intervals);
                $iterator = Iterator::merge($iterators);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
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
            if (is_bool($asc)) {
                $order = 'ORDER BY ' . $qpk . ' ' . ($asc ? 'ASC' : 'DESC');
            } elseif (is_array($asc)) {
                $order = sprintf(
                    'ORDER BY array_position(ARRAY[%s]::BIGINT[], %s::BIGINT)',
                    implode(', ', array_map('intval', $asc)),
                    $qpk
                );
            }
            $template = sprintf(
                'SELECT %s FROM %s %%s %s LIMIT %%s',
                implode(', ', $select),
                $q . $table . $q,
                $order
            );
            if ($where !== null && !$where instanceof WhereCondition) {
                $where = new WhereCondition($where);
            }
            $min = isset($min, $max) ? $min : 1;
            [$op1, $op2] = $asc ? ['>', '<='] : ['<', '>='];
            while ($limit > 0) {
                $_where = $where !== null ? clone $where : new WhereCondition();
                if (($asc === true && $min !== null) || ($asc === false && $max !== null)) {
                    $first = $first ?? true;
                    $_where->append($pk, $asc ? $min : $max, $op1 . ($first ? '=' : ''));
                }
                if (($asc === true && $max !== null) || ($asc === false && $min !== null)) {
                    $_where->append($pk, $asc ? $max : $min, $op2);
                }
                [$_where, $_args] = $_where->stringify($q);
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
        return new Iterator($emit);
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
                'config' => [],
            ];
            [
                'pk' => $pk,
                'fields' => $fields,
                'returnFields' => $returnFields,
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
                    'where' => new WhereCondition([$pk => $chunk]),
                    'pk' => [$pk],
                    'fields' => $fields,
                    'returnFields' => $returnFields,
                    'limit' => $iterator_chunk_size,
                    'config' => compact('iterator_chunk_size'),
                    'asc' => $chunk,
                ]);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
            }
        };
        return new Iterator($emit);
        // $coroutine->onResolve(function ($exception) use (&$emitter) {
        //     if ($exception) {
        //         $emitter->fail($exception);
        //         $emitter = null;
        //     } else {
        //         $emitter->complete();
        //     }
        // });
        // $emit = function ($emit) use ($table, $ids, $params) {
            
        // };
        // return new Producer($emit);
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
     * @param AbstractType $type
     *
     * @return string
     */
    private function getFieldTypeString(AbstractType $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) Type::default() => 'TEXT',
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
                (string) Type::default() => "''::TEXT",
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
}
