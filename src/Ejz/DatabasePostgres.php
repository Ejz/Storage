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
        return $this->minMaxAsync($table, 'min');
    }

    /**
     * @param string $table
     *
     * @return ?int
     */
    public function min(string $table): ?int
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
     * @return ?int
     */
    public function max(string $table): ?int
    {
        return \Amp\Promise\wait($this->maxAsync($table));
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
            $pks = yield $this->pksAsync($table);
            if ($pks === null || count($pks) > 1) {
                throw new RuntimeException(sprintf(self::INVALID_PRIMARY_KEY, $table));
            }
            [$pk] = $pks;
            $pk = $quote . $pk . $quote;
            $sql = 'SELECT %s FROM %s ORDER BY %s LIMIT 1';
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
            yield $this->execAsync('TRUNCATE ? CASCADE', $table);
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
            yield $this->execAsync('DROP TABLE ? CASCADE', $table);
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
            $pks = yield $this->pksAsync($table);
            if ($pks === null || count($pks) > 1) {
                throw new RuntimeException(sprintf(self::INVALID_PRIMARY_KEY, $table));
            }
            [$pk] = $pks;
        }
        $qpk = $quote . $pk . $quote;
        $_fields = $_fields ?? yield $this->fieldsAsync($table);
        $fields = $fields ?? $_fields;
        if (count($_ = array_diff($fields, $_fields))) {
            throw new RuntimeException(sprintf(self::INVALID_TABLE_FIELD, $table, $_[0]));
        }
        $fields = array_map(function ($field) use ($quote) {
            return $quote . $field . $quote;
        }, $fields);
        $_pk = 'pk_' . md5($pk);
        $fields[] = $qpk . ' AS ' . $quote . $_pk . $quote;
        $order_by = $qpk . ' ' . ($asc ? 'ASC' : 'DESC');
        $template = sprintf(
            'SELECT %s FROM %s WHERE %%s ORDER BY %s LIMIT %%s',
            implode(', ', $fields),
            $quote . $table . $quote,
            $order_by
        );
        $op1 = $asc ? '>=' : '<=';
        $op2 = $asc ? '<=' : '>=';
        $min = $min ?? ($asc ? 1 : null);
        $max = $max ?? ($asc ? null : 1E9);
        while ($limit > 0) {
            $where = [];
            $where[] = '(' . $qpk . ' ' . $op1 . ' ?)';
            $args[] = $asc ? $min : $max;
            if (($asc && $max) || (!$asc && $min)) {
                $where[] = '(' . $qpk . ' ' . $op2 . ' ?)';
                $args[] = $asc ? $max : $min;
            }
            $where = implode(' AND ', $where);
            $sql = sprintf($template, $where, min($limit, $iterator_chunk_size));
            $all = yield $this->allAsync($sql, ...$args);
            if (!$all) {
                break;
            }
            foreach ($all as $row) {
                $id = $row[$_pk];
                unset($row[$_pk]);
                yield $id => $row;
            }
            $limit -= count($all);
            ${$asc ? 'min' : 'max'} = $id + ($asc ? 1 : -1);
        }
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
     * @param array           $ids
     * @param mixed           $fields
     *
     * @return Promise
     */
    public function getAsync(TableDefinition $definition, array $ids, $fields): Promise
    {
        return \Amp\call(function ($definition, $ids, $fields) {
            [$cmd, $args, $pk] = $this->getCommand($definition, $ids, $fields);
            $all = yield $this->allAsync($cmd, ...$args);
            $collect = [];
            foreach ($all as $row) {
                $id = $row[$pk];
                unset($row[$pk]);
                foreach ($row as $k => &$v) {
                    $get = $fields[$k]['get'] ?? null;
                    $v = $get === null ? $v : $get($v);
                }
                unset($v);
                $collect[$id] = $row;
            }
            return $collect;
        }, $definition, $ids, $fields);
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id1
     * @param int             $id2
     *
     * @return Promise
     */
    public function reidAsync(TableDefinition $definition, int $id1, int $id2): Promise
    {
        return \Amp\call(function ($definition, $id1, $id2) {
            [$cmd, $args] = $this->reidCommand($definition, $id1, $id2);
            return yield $this->execAsync($cmd, ...$args);
        }, $definition, $id1, $id2);
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id1
     *
     * @return Promise
     */
    public function deleteAsync(TableDefinition $definition, int $id): Promise
    {
        return \Amp\call(function ($definition, $id) {
            [$cmd, $args] = $this->deleteCommand($definition, $id);
            return yield $this->execAsync($cmd, ...$args);
        }, $definition, $id);
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
        if ($definition->isForeignKeyTable($this->getName())) {
            $fields = [];
        }
        $pk = $definition->getPrimaryKey();
        $commands = [];
        $alters = [];
        $_fields = [];
        $map = [
            TableDefinition::TYPE_INT => 'INTEGER',
            TableDefinition::TYPE_BLOB => 'BYTEA',
            TableDefinition::TYPE_TEXT => 'TEXT',
            TableDefinition::TYPE_JSON => 'JSONB',
            TableDefinition::TYPE_FOREIGN_KEY => 'BIGINT',
            TableDefinition::TYPE_BOOL => 'BOOLEAN',
            TableDefinition::TYPE_FLOAT => 'REAL',
            TableDefinition::TYPE_DATE => 'DATE',
            TableDefinition::TYPE_DATETIME => 'TIMESTAMP(0) WITHOUT TIME ZONE',
            TableDefinition::TYPE_INT_ARRAY => 'INTEGER[]',
            TableDefinition::TYPE_TEXT_ARRAY => 'TEXT[]',
        ];
        $defaults = [
            TableDefinition::TYPE_INT => '0',
            TableDefinition::TYPE_FOREIGN_KEY => '0',
            TableDefinition::TYPE_BLOB => '\'\'::BYTEA',
            TableDefinition::TYPE_TEXT => '\'\'::TEXT',
            TableDefinition::TYPE_JSON => '\'{}\'::JSONB',
            TableDefinition::TYPE_BOOL => '\'f\'::BOOLEAN',
            TableDefinition::TYPE_FLOAT => '0',
            TableDefinition::TYPE_DATE => 'CURRENT_DATE',
            TableDefinition::TYPE_DATETIME => 'CURRENT_TIMESTAMP',
            TableDefinition::TYPE_INT_ARRAY => '\'{}\'::INTEGER[]',
            TableDefinition::TYPE_TEXT_ARRAY => '\'{}\'::TEXT[]',
        ];
        $seq = $table . '_seq';
        $pk_start_with = $definition->getPrimaryKeyStartWith($this->getName());
        $pk_increment_by = $definition->getPrimaryKeyIncrementBy($this->getName());
        $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$seq}{$q} CASCADE";
        $commands[] = "
            CREATE SEQUENCE {$q}{$seq}{$q}
            AS BIGINT
            START WITH {$pk_start_with}
            INCREMENT BY {$pk_increment_by}
            MINVALUE {$pk_start_with}
        ";
        $rand = mt_rand();
        $alters[] = "
            ALTER TABLE {$q}{$table}{$q}
            ADD CONSTRAINT {$q}{$table}_{$rand}{$q}
            PRIMARY KEY ({$q}{$pk}{$q})
        ";
        $_fields[] = "
            {$q}{$pk}{$q} BIGINT DEFAULT
            nextval('{$seq}'::regclass) NOT NULL
        ";
        $uniques = [];
        $indexes = [];
        foreach ($fields as $field => $meta) {
            [
                'is_nullable' => $is_nullable,
                'type' => $type,
                'default' => $default,
                'unique' => $unique,
                'index' => $index,
            ] = $meta;
            foreach ($unique as $_) {
                @ $uniques[$_][] = $field;
            }
            foreach ($index as $_) {
                @ $indexes[$_][] = $field;
            }
            $default = $default ?? ($is_nullable ? 'NULL' : ($defaults[$type] ?? null));
            $default = (string) $default;
            $_field = $q . $field . $q . ' ' . $map[$type];
            if ($default !== '') {
                $_field .= ' DEFAULT ' . $default;
            }
            $_field .= ' ' . ($is_nullable ? 'NULL' : 'NOT NULL');
            $_fields[] = $_field;
            if ($type === TableDefinition::TYPE_FOREIGN_KEY) {
                $c = $table . '_' . mt_rand();
                $alters[] = "
                    ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$c}{$q} FOREIGN KEY ({$field})
                    REFERENCES {$meta['references']} ON DELETE CASCADE ON UPDATE CASCADE
                ";
                @ $indexes['HASH:' . mt_rand()][] = $field;
            }
        }
        foreach ($uniques as $unique => $fs) {
            $fs = implode(', ', array_map(function ($f) use ($q) {
                return $q . $f . $q;
            }, $fs));
            $c = $table . '_' . mt_rand();
            $alters[] = "ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$c}{$q} UNIQUE ({$fs})";
        }
        foreach ($indexes as $index => $fs) {
            $index = explode(':', $index);
            $type = count($index) > 1 ? $index[0] : 'BTREE';
            $fs = implode(', ', array_map(function ($f) use ($q) {
                return $q . $f . $q;
            }, $fs));
            $c = $table . '_' . mt_rand();
            $alters[] = "CREATE INDEX {$q}{$c}{$q} ON {$q}{$table}{$q} USING {$type} ({$fs})";
        }
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
        if ($definition->isForeignKeyTable($this->getName())) {
            $values = [];
        }
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
     * @param int             $id1
     * @param int             $id2
     *
     * @return array
     */
    private function reidCommand(TableDefinition $definition, int $id1, int $id2): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPrimaryKey();
        $args = [$id2, $id1];
        $command = "UPDATE {$q}{$table}{$q} SET {$q}{$pk}{$q} = ? WHERE {$q}{$pk}{$q} = ?";
        return [$command, $args];
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id
     *
     * @return array
     */
    private function deleteCommand(TableDefinition $definition, int $id): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPrimaryKey();
        $args = [$id];
        $command = "DELETE FROM {$q}{$table}{$q} WHERE {$q}{$pk}{$q} = ?";
        return [$command, $args];
    }

    /**
     * @param TableDefinition $definition
     * @param array           $ids
     * @param mixed           $fields
     *
     * @return array
     */
    private function getCommand(TableDefinition $definition, array $ids, $fields): array
    {
        $q = $this->config['quote'];
        $table = $definition->getTable();
        $pk = $definition->getPrimaryKey();
        $ids = array_map('intval', $ids);
        $_ = implode(', ', array_fill(0, count($ids), '?'));
        $where = "{$q}{$pk}{$q} IN ({$_})";
        $_pk = '_pk_' . mt_rand();
        $_fields = ["{$q}{$pk}{$q} AS {$q}{$_pk}{$q}"];
        foreach ($fields as $alias => $field) {
            $f = str_replace('%s', $q . $field['field'] . $q, $field['get_pattern']);
            $_fields[] = $f . ' AS ' . $q . $alias . $q;
        }
        $_fields = implode(', ', $_fields);
        $command = "SELECT {$_fields} FROM {$q}{$table}{$q} WHERE {$where}";
        return [$command, $ids, $_pk];
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
        $collect = [];
        foreach ($where as $key => $value) {
            if (is_array($value) && count($value)) {
                $args = array_merge($args, $value);
                $_ = implode(', ', array_fill(0, count($value), '?'));
                $collect[] = "({$q}{$key}{$q} IN ({$_}))";
            } elseif (!is_array($value)) {
                $args[] = $value;
                $collect[] = "({$q}{$key}{$q} = ?)";
            }
        }
        return ['(' . implode(' AND ', $collect) . ')', $args];
    }
}
