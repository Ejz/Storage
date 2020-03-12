<?php

namespace Ejz;

use Amp\Loop;
use Amp\Promise;
use Amp\Postgres\Connection;
use Amp\Postgres\ConnectionConfig;
use Amp\Postgres\PgSqlCommandResult;
use Ejz\Type\AbstractType;
use RuntimeException;

class DatabasePostgres implements DatabaseInterface
{
    use NameTrait;
    use SyncTrait;

    /** @var string */
    protected $dsn;

    /** @var array */
    protected $config;

    /** @var ?DatabasePostgresConnection */
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
        $this->dsn = $dsn;
        $this->config = $config + [
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
            $connection = yield DatabasePostgresConnection::connect($this->dsn);
            $this->connection = $connection;
        });
    }

    /**
     */
    public function close()
    {
        if ($this->connection !== null) {
            $this->connection->close();
            $this->connection = null;
        }
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
            return yield $this->connection->query($sql, $args);
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
            return $all[0] ?? [];
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
            return array_map(function ($row) {
                return current($row);
            }, $all);
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
            yield $this->exec('DROP TABLE IF EXISTS # CASCADE', $table);
            $sequence = sprintf(self::SEQUENCE_NAME, $table);
            yield $this->exec('DROP SEQUENCE IF EXISTS # CASCADE', $sequence);
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
            yield $this->exec('TRUNCATE # CASCADE', $table);
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
            [$where, $args] = $where === null ? [null, []] : $where->stringify();
            $sql = 'SELECT COUNT(1) FROM #' . ($where !== null ? ' ' . $where : '');
            return (int) yield $this->val($sql, $table, ...$args);
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
            return $this->col($sql, 'public', $table);
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
            $fields = yield $this->fields($table);
            return in_array($field, $fields ?? []);
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
            $iterator = yield $this->all($sql, 'r', $table, 'public');
            $indexes = [];
            foreach ($iterator as ['i' => $i, 'c' => $c]) {
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
            $indexes = yield $this->indexes($table);
            $indexes = $indexes ?? [];
            return isset($indexes[$index]);
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
            return $this->col($sql, $table, 'public');
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
            $pk = yield $this->pk($table);
            if ($pk === null || $pk === []) {
                return $pk ?? null;
            }
            $c = count($pk);
            $asc = $asc ? 'ASC' : 'DESC';
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
            $sql = sprintf(
                $sql,
                implode(', ', array_fill(0, $c, '#')),
                '#',
                implode(', ', array_fill(0, $c, '# ' . $asc))
            );
            $args = [];
            array_push($args, ...$pk);
            array_push($args, $table);
            array_push($args, ...$pk);
            $one = yield $this->one($sql, ...$args);
            return $one ? array_values($one) : array_fill(0, $c, null);
        }, $table, $asc);
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param int    $primaryKeyStart     (optional)
     * @param int    $primaryKeyIncrement (optional)
     * @param array  $fields              (optional)
     * @param array  $indexes             (optional)
     * @param array  $foreignKeys         (optional)
     *
     * @return Promise
     */
    public function create(
        string $table,
        string $primaryKey,
        int $primaryKeyStart = 1,
        int $primaryKeyIncrement = 1,
        array $fields = [],
        array $indexes = [],
        array $foreignKeys = []
    ): Promise {
        return \Amp\call(function (...$args) {
            $commands = $this->getCreateCommands(...$args);
            foreach ($commands as $args) {
                yield $this->exec(...$args);
            }
        },
            $table, $primaryKey, $primaryKeyStart, $primaryKeyIncrement,
            $fields, $indexes, $foreignKeys
        );
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param int    $primaryKeyStart
     * @param int    $primaryKeyIncrement
     * @param array  $fields
     * @param array  $indexes
     * @param array  $foreignKeys
     *
     * @return array
     */
    private function getCreateCommands(
        string $table,
        string $primaryKey,
        int $primaryKeyStart,
        int $primaryKeyIncrement,
        array $fields,
        array $indexes,
        array $foreignKeys
    ): array {
        $comma = function ($array) {
            return implode(', ', array_fill(0, count($array), '#'));
        };
        $commands = [];
        // CREATE TABLE
        $commands[] = ['CREATE TABLE #()', $table];
        // CREATE SEQUENCE
        $sequence = sprintf(self::SEQUENCE_NAME, $table);
        $commands[] = ['DROP SEQUENCE IF EXISTS # CASCADE', $sequence];
        $commands[] = [
            'CREATE SEQUENCE # AS BIGINT START % INCREMENT % MINVALUE % NO CYCLE',
            $sequence, $primaryKeyStart, $primaryKeyIncrement, $primaryKeyStart
        ];
        // ADD PRIMARY KEY
        $commands[] = [
            'ALTER TABLE # ADD COLUMN # BIGINT DEFAULT 0 NOT NULL',
            $table, $primaryKey
        ];
        $constraint = sprintf(self::PK_CONSTRAINT_NAME, $table);
        $commands[] = [
            'ALTER TABLE # ADD CONSTRAINT # PRIMARY KEY (#)',
            $table, $constraint, $primaryKey
        ];
        // FIELDS
        foreach ($fields as $field) {
            $commands[] = [
                'ALTER TABLE # ADD COLUMN # % NULL',
                $table,
                $field->getName(),
                $this->getFieldTypeString($field),
            ];
        }
        // INDEXES
        foreach ($indexes as $index) {
            $fields = $index->getFields();
            $command = [
                'CREATE % INDEX # ON # USING % (' . $comma($fields) . ')',
                $index->isUnique() ? 'UNIQUE' : '',
                sprintf(self::INDEX_NAME, $table, $index->getName()),
                $table,
                $this->getIndexTypeString($index),
            ];
            array_push($command, ...$fields);
            $commands[] = $command;
        }
        // FOREIGN KEYS
        foreach ($foreignKeys as $foreignKey) {
            $parentTable = $foreignKey->getParentTable();
            $parentFields = $foreignKey->getParentFields();
            $childFields = $foreignKey->getChildFields();
            $command = [
                '
                    ALTER TABLE # ADD CONSTRAINT #
                    FOREIGN KEY (' . $comma($childFields) . ')
                    REFERENCES # (' . $comma($parentFields) . ')
                    ON DELETE CASCADE ON UPDATE CASCADE
                ',
                $table,
                sprintf(self::FOREIGN_KEY_NAME, $table, $foreignKey->getName()),
            ];
            array_push($command, ...$childFields);
            array_push($command, $parentTable);
            array_push($command, ...$parentFields);
            $commands[] = $command;
        }
        return $commands;
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param ?int   $int        (optional)
     * @param array  $fields     (optional)
     *
     * @return Promise
     */
    public function insert(
        string $table,
        string $primaryKey,
        ?int $id = null,
        array $fields = []
    ): Promise {
        return \Amp\call(function ($table, $primaryKey, $id, $fields) {
            $id = $id ?? yield $this->getNextId($table);
            $args = $this->getInsertCommand($table, $primaryKey, $id, $fields);
            return yield $this->val(...$args);
        }, $table, $primaryKey, $id, $fields);
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param int    $id
     * @param array  $fields
     *
     * @return array
     */
    private function getInsertCommand(
        string $table,
        string $primaryKey,
        int $id,
        array $fields
    ): array {
        $_0s = array_fill(0, count($fields) + 1, '#');
        $_0a = [$primaryKey];
        $_1s = ['?'];
        $_1a = [$id];
        foreach ($fields as $field) {
            $_0a[] = $field->getName();
            $_1s[] = $field->getType()->isBinary() ? '$' : '?';
            $_1a[] = $field->exportValue();
        }
        $_0s = implode(', ', $_0s);
        $_1s = implode(', ', $_1s);
        $command = ['INSERT INTO # (' . $_0s . ') VALUES (' . $_1s . ') RETURNING #'];
        array_push($command, $table, ...$_0a, ...$_1a);
        $command[] = $primaryKey;
        return $command;
    }

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function getNextId(string $table): Promise
    {
        $sequence = sprintf(self::SEQUENCE_NAME, $table);
        return $this->val('SELECT nextval(?::regclass)', $sequence);
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     * @param array  $fields
     *
     * @return Promise
     */
    public function update(
        string $table,
        string $primaryKey,
        array $ids,
        array $fields
    ): Promise {
        return \Amp\call(function ($table, $primaryKey, $ids, $fields) {
            if (!count($fields) || !$ids) {
                return 0;
            }
            $args = $this->getUpdateCommand($table, $primaryKey, $ids, $fields);
            return (int) yield $this->exec(...$args);
        }, $table, $primaryKey, $ids, $fields);
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     * @param array  $fields
     *
     * @return Promise
     */
    private function getUpdateCommand(
        string $table,
        string $primaryKey,
        array $ids,
        array $fields
    ): array {
        $update = [];
        $args = [];
        foreach ($fields as $field) {
            $_ = $field->getType()->isBinary() ? '$' : '?';
            $update[] = '# = ' . $_;
            $args[] = $field->getName();
            $args[] = $field->exportValue();
        }
        $update = implode(', ', $update);
        $_ = implode(', ', array_fill(0, count($ids), '?'));
        $command = ['UPDATE # SET ' . $update . ' WHERE # IN (' . $_ . ')'];
        $command[] = $table;
        array_push($command, ...$args);
        $command[] = $primaryKey;
        array_push($command, ...$ids);
        return $command;
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     *
     * @return Promise
     */
    public function delete(string $table, string $primaryKey, array $ids): Promise
    {
        return \Amp\call(function ($table, $primaryKey, $ids) {
            if (!$ids) {
                return 0;
            }
            $args = $this->getDeleteCommand($table, $primaryKey, $ids);
            return (int) yield $this->exec(...$args);
        }, $table, $primaryKey, $ids);
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     *
     * @return array
     */
    private function getDeleteCommand(string $table, string $primaryKey, array $ids): array
    {
        $_ = implode(', ', array_fill(0, count($ids), '?'));
        $command = ['DELETE FROM # WHERE # IN (' . $_ . ')'];
        array_push($command, $table, $primaryKey, ...$ids);
        return $command;
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param int    $id1
     * @param int    $id2
     *
     * @return Promise
     */
    public function reid(string $table, string $primaryKey, int $id1, int $id2): Promise
    {
        return \Amp\call(function ($table, $primaryKey, $id1, $id2) {
            if ($id1 === $id2) {
                return false;
            }
            $args = $this->getReidCommand($table, $primaryKey, $id1, $id2);
            return (bool) yield $this->exec(...$args);
        }, $table, $primaryKey, $id1, $id2);
    }

    /**
     * @param string $table
     * @param string $primaryKey
     * @param int    $id1
     * @param int    $id2
     *
     * @return array
     */
    private function getReidCommand(string $table, string $primaryKey, int $id1, int $id2): array
    {
        return ['UPDATE # SET # = ? WHERE # = ?', $table, $primaryKey, $id2, $primaryKey, $id1];
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
                'min' => null,
                'max' => null,
                'limit' => 1E9,
                'pk' => null,
                'where' => [],
                'config' => [],
            ];
            [
                'fields' => $fields,
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
                $iterator = Iterator::merge($iterators, function () {
                    return mt_rand(-1, 1);
                });
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
                return;
            }
            [$pk] = $pk;
            $_pk = 'pk_' . md5($pk);
            $types = [];
            foreach ($fields as &$field) {
                if (!$field instanceof Field) {
                    $field = new Field($field);
                }
                $types[$field->getName()] = $field->getType();
            }
            unset($field);
            $args = [];
            $select = array_fill(0, count($fields), '#');
            array_push($args, ...array_keys($types));
            if (!isset($types[$pk])) {
                $select[] = '# AS #';
                array_push($args, $pk, $_pk);
            }
            $args[] = $table;
            if (is_bool($asc)) {
                $order = 'ORDER BY # ' . ($asc ? 'ASC' : 'DESC');
            } else {
                $ids = array_unique(array_map('intval', $asc));
                $order = 'ORDER BY array_position(ARRAY[' . implode(',', $ids) . ']::BIGINT[], #::BIGINT)';
            }
            $select = implode(', ', $select);
            $template = 'SELECT ' . $select . ' FROM # %s ' . $order . ' LIMIT %s';
            $where = is_array($where) ? new WhereCondition($where) : $where;
            $min = isset($min, $max) ? $min : 1;
            [$op1, $op2] = $asc ? ['>', '<='] : ['<', '>='];
            while ($limit > 0) {
                $_where = clone $where;
                if (($asc === true && $min !== null) || ($asc === false && $max !== null)) {
                    $first = $first ?? true;
                    $_where->append($pk, $asc ? $min : $max, $op1 . ($first ? '=' : ''));
                }
                if (($asc === true && $max !== null) || ($asc === false && $min !== null)) {
                    $_where->append($pk, $asc ? $max : $min, $op2);
                }
                [$_where, $_args] = $_where->stringify();
                $_args[] = $pk;
                $lim = min($limit, $iterator_chunk_size);
                $sql = sprintf($template, $_where, $lim);
                $all = yield $this->all($sql, ...$args, ...$_args);
                $c = count($all);
                $limit -= $c;
                foreach ($all as $row) {
                    $id = $row[$_pk] ?? $row[$pk];
                    unset($row[$_pk]);
                    $_ = [];
                    foreach ($row as $k => $v) {
                        $_[$k] = $types[$k]->importValue($v);
                    }
                    yield $emit([$id, $_]);
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
                    'where' => new WhereCondition([$pk => $chunk]),
                    'pk' => [$pk],
                    'fields' => $fields,
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
     * @param Field $field
     *
     * @return string
     */
    private function getFieldTypeString(Field $field): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) DatabaseType::default()          => 'TEXT',
                (string) DatabaseType::string()           => 'TEXT',
                (string) DatabaseType::int()              => 'INTEGER',
                (string) DatabaseType::float()            => 'REAL',
                (string) DatabaseType::bool()             => 'BOOLEAN',
                (string) DatabaseType::date()             => 'DATE',
                (string) DatabaseType::dateTime()         => 'TIMESTAMP(0) WITHOUT TIME ZONE',
                (string) DatabaseType::json()             => 'JSONB',
                (string) DatabaseType::bigInt()           => 'BIGINT',
                (string) DatabaseType::intArray()         => 'INTEGER[]',
                (string) DatabaseType::stringArray()      => 'TEXT[]',
                (string) DatabaseType::enum()             => 'TEXT',
                (string) DatabaseType::binary()           => 'BYTEA',
                (string) DatabaseType::compressedBinary() => 'BYTEA',
            ];
        }
        return $map[$field->getType()->getName()];
    }

    /**
     * @param Index $index
     *
     * @return string
     */
    private function getIndexTypeString(Index $index): string
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
        return $map[$index->getType()->getName()];
    }
}
