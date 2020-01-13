<?php

namespace Ejz;

use Generator;
use Amp\Promise;

class DatabaseExtendedPostgres extends DatabasePostgres implements DatabaseExtendedInterface
{
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
            'quote' => $quote,
            'iterator_chunk_size' => $iterator_chunk_size,
            'rand_iterator_intervals' => $rand_iterator_intervals,
        ] = $config;
        $pk = $pk ?? $this->pk($table);
        if ($pk === null || count($pk) !== 1) {
            return;
        }
        $fields = $fields ?? $this->fields($table);
        if ($rand) {
            $min = $min ?? $this->min($table)[0];
            $max = $max ?? $this->max($table)[0];
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
        [$pk] = $pk;
        $qpk = $quote . $pk . $quote;
        $collect = [];
        foreach ($fields as $field) {
            if (!$field instanceof Field) {
                $field = new Field($field);
            }
            $collect[$field->getAlias()] = $field;
        }
        $fields = $collect;
        $select = array_map(function ($field) use ($quote) {
            return $field->getSelectString($quote);
        }, $fields);
        $_pk = 'pk_' . md5($pk);
        $select[] = $qpk . ' AS ' . $quote . $_pk . $quote;
        $order = $order ? $qpk . ' ' . ($asc ? 'ASC' : 'DESC') : '';
        $order = $order ? 'ORDER BY ' . $order : '';
        $template = sprintf(
            'SELECT %s FROM %s %%s %s LIMIT %%s',
            implode(', ', $select),
            $quote . $table . $quote,
            $order
        );
        [$op1, $op2] = $asc ? ['>', '<='] : ['<', '>='];
        $where = $where instanceof Condition ? $where : new Condition($where);
        while ($limit > 0) {
            $where->resetPushed();
            if (($asc && $min !== null) || (!$asc && $max !== null)) {
                $first = $first ?? true;
                $where->push($pk, $op1 . ($first ? '=' : ''), $asc ? $min : $max);
            }
            if (($asc && $max !== null) || (!$asc && $min !== null)) {
                $where->push($pk, $op2, $asc ? $max : $min);
            }
            [$_where, $_args] = $where->stringify($quote);
            $_where = $_where ? 'WHERE ' . $_where : '';
            $sql = sprintf($template, $_where, min($limit, $iterator_chunk_size));
            $all = $this->all($sql, ...$_args);
            if (!$all) {
                break;
            }
            foreach ($all as $row) {
                $id = $row[$_pk];
                unset($row[$_pk]);
                foreach ($row as $k => &$v) {
                    $v = $fields[$k]->getSelectValue($v);
                }
                unset($v);
                yield $id => $row;
            }
            $limit -= count($all);
            ${$asc ? 'min' : 'max'} = $id;
            $first = false;
        }
    }

    /**
     * @param string $table
     * @param array  $ids
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function get(string $table, array $ids, array $params = []): Generator
    {
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
        $pk = $pk ?? $this->pk($table);
        if ($pk === null || count($pk) !== 1) {
            return;
        }
        [$pk] = $pk;
        foreach (array_chunk($ids, $iterator_chunk_size) as $chunk) {
            yield from $this->iterate($table, [
                'where' => new Condition([$pk => $chunk]),
                'fields' => $fields,
                'limit' => $iterator_chunk_size,
                'config' => compact('iterator_chunk_size'),
                'order' => false,
            ]);
        }
    }

    /**
     * @param Repository $repository
     *
     * @return Promise
     */
    public function createAsync(Repository $repository): Promise
    {
        return \Amp\call(function ($repository) {
            $commands = $this->getCreateCommands($repository);
            foreach ($commands as $command) {
                yield $this->execAsync($command);
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
        $rand = function () {
            return mt_rand();
        };
        $enq = function ($fields) use ($q) {
            return implode(', ', array_map(function ($field) use ($q) {
                return $q . $field . $q;
            }, $fields));
        };
        $table = $repository->getTable();
        [$fields, $indexes] = [[], []];
        if (!$repository->isForeignKeyTable($this->getName())) {
            $fields = $repository->getFields();
            $indexes = $repository->getIndexes();
            $foreignKeys = $repository->getForeignKeys();
        }
        $pk = $repository->getPk();
        $pkStartWith = $repository->getPkStartWith($this->getName());
        $pkIncrementBy = $repository->getPkIncrementBy($this->getName());
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
            ADD CONSTRAINT {$q}{$table}_{$rand()}{$q}
            PRIMARY KEY ({$q}{$pk}{$q})
        ";
        // FIELDS
        foreach ($fields as $field) {
            $type = $field->getType();
            $null = $type->isNullable() ? 'NULL' : 'NOT NULL';
            $default = !$type->isNullable() ? $this->getDefault($type) : '';
            $default = $default ? 'DEFAULT ' . $default : '';
            $type = $this->getType($type);
            $commands[] = "
                ALTER TABLE {$q}{$table}{$q}
                ADD COLUMN {$q}{$field}{$q}
                {$type} {$null} {$default}
            ";
        }
        // INDEXES
        foreach ($indexes as $index) {
            $f = $index->getFields();
            $t = $index->getType();
            $commands[] = "
                CREATE INDEX {$q}{$table}_{$rand()}{$q} ON {$q}{$table}{$q}
                USING {$t} ($enq($f))
            ";
        }
        // FOREIGN KEYS
        foreach ($foreignKeys as $foreignKey) {
            $pt = $foreignKey->getParentTable();
            $cf = $foreignKey->getChildFields();
            $pf = $foreignKey->getParentFields();
            $commands[] = "
                ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$table}_{$rand()}{$q}
                FOREIGN KEY ({$enq($cf)}) REFERENCES {$q}{$pt}{$q} ({$enq($pf)})
                ON DELETE CASCADE ON UPDATE CASCADE
            ";
        }
        $commands = array_map('trim', $commands);
        return $commands;
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
    private function getType(AbstractType $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) Type::string() => 'TEXT',
            ];
        }
        return $map[(string) $type];
    }

    /**
     * @param AbstractType $type
     *
     * @return string
     */
    private function getDefault(AbstractType $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) Type::string() => "''::TEXT",
            ];
        }
        return $map[(string) $type];
    }

        // $alters = [];
        // $_fields = [];
        // $map = [
        //     TableDefinition::TYPE_INT => 'INTEGER',
        //     TableDefinition::TYPE_BLOB => 'BYTEA',
        //     TableDefinition::TYPE_TEXT => 'TEXT',
        //     TableDefinition::TYPE_JSON => 'JSONB',
        //     TableDefinition::TYPE_FOREIGN_KEY => 'BIGINT',
        //     TableDefinition::TYPE_BOOL => 'BOOLEAN',
        //     TableDefinition::TYPE_FLOAT => 'REAL',
        //     TableDefinition::TYPE_DATE => 'DATE',
        //     TableDefinition::TYPE_DATETIME => 'TIMESTAMP(0) WITHOUT TIME ZONE',
        //     TableDefinition::TYPE_INT_ARRAY => 'INTEGER[]',
        //     TableDefinition::TYPE_TEXT_ARRAY => 'TEXT[]',
        // ];
        // $defaults = [
        //     TableDefinition::TYPE_INT => '0',
        //     TableDefinition::TYPE_FOREIGN_KEY => '0',
        //     TableDefinition::TYPE_BLOB => '\'\'::BYTEA',
        //     TableDefinition::TYPE_TEXT => ,
        //     TableDefinition::TYPE_JSON => '\'{}\'::JSONB',
        //     TableDefinition::TYPE_BOOL => '\'f\'::BOOLEAN',
        //     TableDefinition::TYPE_FLOAT => '0',
        //     TableDefinition::TYPE_DATE => 'CURRENT_DATE',
        //     TableDefinition::TYPE_DATETIME => 'CURRENT_TIMESTAMP',
        //     TableDefinition::TYPE_INT_ARRAY => '\'{}\'::INTEGER[]',
        //     TableDefinition::TYPE_TEXT_ARRAY => '\'{}\'::TEXT[]',
        // ];
    // /**
    //  * @param TableDefinition $definition
    //  *
    //  * @return Promise
    //  */
    // public function createAsync(TableDefinition $definition): Promise
    // {
    //     return \Amp\call(function ($definition) {
    //         yield $this->dropAsync($definition->getTable());
    //         $commands = $this->createCommands($definition);
    //         foreach ($commands as $command) {
    //             yield $this->execAsync($command);
    //         }
    //     }, $definition);
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param array           $values
    //  *
    //  * @return Promise
    //  */
    // public function insertAsync(TableDefinition $definition, array $values): Promise
    // {
    //     return \Amp\call(function ($definition, $values) {
    //         [$cmd, $args] = $this->insertCommand($definition, $values);
    //         return yield $this->valAsync($cmd, ...$args);
    //     }, $definition, $values);
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id
    //  * @param array           $values
    //  *
    //  * @return Promise
    //  */
    // public function updateAsync(TableDefinition $definition, int $id, array $values): Promise
    // {
    //     return \Amp\call(function ($definition, $id, $values) {
    //         if (!$values) {
    //             return false;
    //         }
    //         [$cmd, $args] = $this->updateCommand($definition, $id, $values);
    //         return yield $this->execAsync($cmd, ...$args);
    //     }, $definition, $id, $values);
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param array           $ids
    //  * @param mixed           $fields
    //  *
    //  * @return Promise
    //  */
    // public function getAsync(TableDefinition $definition, array $ids, $fields): Promise
    // {
    //     return \Amp\call(function ($definition, $ids, $fields) {
    //         [$cmd, $args, $pk] = $this->getCommand($definition, $ids, $fields);
    //         $all = yield $this->allAsync($cmd, ...$args);
    //         $collect = [];
    //         foreach ($all as $row) {
    //             $id = $row[$pk];
    //             unset($row[$pk]);
    //             foreach ($row as $k => &$v) {
    //                 $get = $fields[$k]['get'] ?? null;
    //                 $v = $get === null ? $v : $get($v);
    //             }
    //             unset($v);
    //             $collect[$id] = $row;
    //         }
    //         return $collect;
    //     }, $definition, $ids, $fields);
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id1
    //  * @param int             $id2
    //  *
    //  * @return Promise
    //  */
    // public function reidAsync(TableDefinition $definition, int $id1, int $id2): Promise
    // {
    //     return \Amp\call(function ($definition, $id1, $id2) {
    //         [$cmd, $args] = $this->reidCommand($definition, $id1, $id2);
    //         return yield $this->execAsync($cmd, ...$args);
    //     }, $definition, $id1, $id2);
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id1
    //  *
    //  * @return Promise
    //  */
    // public function deleteAsync(TableDefinition $definition, int $id): Promise
    // {
    //     return \Amp\call(function ($definition, $id) {
    //         [$cmd, $args] = $this->deleteCommand($definition, $id);
    //         return yield $this->execAsync($cmd, ...$args);
    //     }, $definition, $id);
    // }

    

    // /**
    //  * @param TableDefinition $definition
    //  * @param array           $values
    //  *
    //  * @return array
    //  */
    // private function insertCommand(TableDefinition $definition, array $values): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $pk = $definition->getPrimaryKey();
    //     $_columns = [];
    //     $_values = [];
    //     $args = [];
    //     if ($definition->isForeignKeyTable($this->getName())) {
    //         $values = [];
    //     }
    //     foreach ($values as $value) {
    //         $f = $q . $value['field'] . $q;
    //         $_columns[] = $f;
    //         $_values[] = str_replace('%s', $f, $value['set_pattern']);
    //         $args[] = $value['set'] ? $value['set']($value['value']) : $value['value'];
    //     }
    //     $_columns = implode(', ', $_columns);
    //     $_values = implode(', ', $_values);
    //     $insert = ($_columns && $_values) ? "({$_columns}) VALUES ({$_values})" : 'DEFAULT VALUES';
    //     $command = "INSERT INTO {$q}{$table}{$q} {$insert} RETURNING {$q}{$pk}{$q}";
    //     return [$command, $args];
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id
    //  * @param array           $values
    //  *
    //  * @return array
    //  */
    // private function updateCommand(TableDefinition $definition, int $id, array $values): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $pk = $definition->getPrimaryKey();
    //     $args = [];
    //     $update = [];
    //     foreach ($values as $value) {
    //         $f = $q . $value['field'] . $q;
    //         $update[] = $f . ' = ' . str_replace('%s', $f, $value['set_pattern']);
    //         $args[] = $value['set'] ? $value['set']($value['value']) : $value['value'];
    //     }
    //     $update = implode(', ', $update);
    //     $args[] = $id;
    //     $command = "UPDATE {$q}{$table}{$q} SET {$update} WHERE {$q}{$pk}{$q} = ?";
    //     return [$command, $args];
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id1
    //  * @param int             $id2
    //  *
    //  * @return array
    //  */
    // private function reidCommand(TableDefinition $definition, int $id1, int $id2): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $pk = $definition->getPrimaryKey();
    //     $args = [$id2, $id1];
    //     $command = "UPDATE {$q}{$table}{$q} SET {$q}{$pk}{$q} = ? WHERE {$q}{$pk}{$q} = ?";
    //     return [$command, $args];
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param int             $id
    //  *
    //  * @return array
    //  */
    // private function deleteCommand(TableDefinition $definition, int $id): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $pk = $definition->getPrimaryKey();
    //     $args = [$id];
    //     $command = "DELETE FROM {$q}{$table}{$q} WHERE {$q}{$pk}{$q} = ?";
    //     return [$command, $args];
    // }

    // /**
    //  * @param TableDefinition $definition
    //  * @param array           $ids
    //  * @param mixed           $fields
    //  *
    //  * @return array
    //  */
    // private function getCommand(TableDefinition $definition, array $ids, $fields): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $pk = $definition->getPrimaryKey();
    //     $ids = array_map('intval', $ids);
    //     $_ = implode(', ', array_fill(0, count($ids), '?'));
    //     $where = "{$q}{$pk}{$q} IN ({$_})";
    //     $_pk = '_pk_' . mt_rand();
    //     $_fields = ["{$q}{$pk}{$q} AS {$q}{$_pk}{$q}"];
    //     foreach ($fields as $alias => $field) {
    //         $f = str_replace('%s', $q . $field['field'] . $q, $field['get_pattern']);
    //         $_fields[] = $f . ' AS ' . $q . $alias . $q;
    //     }
    //     $_fields = implode(', ', $_fields);
    //     $command = "SELECT {$_fields} FROM {$q}{$table}{$q} WHERE {$where}";
    //     return [$command, $ids, $_pk];
    // }

    // /**
    //  * @param array $where
    //  *
    //  * @return array
    //  */
    // private function flattenWhere(array $where): array
    // {
    //     if (!$where) {
    //         return ['(TRUE)', []];
    //     }
    //     $q = $this->config['quote'];
    //     $collect = [];
    //     foreach ($where as $key => $value) {
    //         if (is_array($value) && count($value)) {
    //             $args = array_merge($args, $value);
    //             $_ = implode(', ', array_fill(0, count($value), '?'));
    //             $collect[] = "({$q}{$key}{$q} IN ({$_}))";
    //         } elseif (!is_array($value)) {
    //             $args[] = $value;
    //             $collect[] = "({$q}{$key}{$q} = ?)";
    //         }
    //     }
    //     return ['(' . implode(' AND ', $collect) . ')', $args];
    // }
}
