<?php

namespace Ejz;

use Generator;

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
            // '_fields' => null,
            'asc' => true,
            'rand' => false,
            'min' => null,
            'max' => null,
            'limit' => 1E9,
            'pk' => null,
            'config' => [],
        ];
        $params['config'] = ($params['config'] ?? []) + $this->config;
        [
            'fields' => $fields,
            // '_fields' => $_fields,
            'asc' => $asc,
            'rand' => $rand,
            'min' => $min,
            'max' => $max,
            'limit' => $limit,
            'pk' => $pk,
            'config' => $config,
        ] = $params;
        $config += $this->config;
        [
            'quote' => $quote,
            'iterator_chunk_size' => $iterator_chunk_size,
            'rand_iterator_intervals' => $rand_iterator_intervals,
        ] = $config;
        if ($pk === null) {
            $pk = $this->pk($table);
            if ($pk === null || count($pk) !== 1) {
                return;
            }
            // @todo: work with compound pk
            [$pk] = $pk;
        }
        $qpk = $quote . $pk . $quote;
        $fields = $fields ?? $this->fields($table);
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
        $collect = [];
        foreach ($fields as $field) {
            if (!$field instanceof DatabaseField) {
                $field = new DatabaseField($field);
            }
            $collect[$field->getAlias()] = $field;
        }
        $fields = $collect;
        $select = array_map(function ($field) use ($quote) {
            return $field->stringify($quote);
        }, $fields);
        $_pk = 'pk_' . md5($pk);
        $select[] = $qpk . ' AS ' . $quote . $_pk . $quote;
        $order = $qpk . ' ' . ($asc ? 'ASC' : 'DESC');
        $template = sprintf(
            'SELECT %s FROM %s %%s ORDER BY %s LIMIT %%s',
            implode(', ', $select),
            $quote . $table . $quote,
            $order
        );
        [$op1, $op2] = $asc ? ['>', '<='] : ['<', '>='];
        while ($limit > 0) {
            $where = [];
            if (($asc && $min !== null) || (!$asc && $max !== null)) {
                $first = $first ?? true;
                $where[] = '(' . $qpk . ' ' . $op1 . ($first ? '=' : '') . ' ?)';
                $args[] = $asc ? $min : $max;
            }
            if (($asc && $max !== null) || (!$asc && $min !== null)) {
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
                foreach ($row as $k => &$v) {
                    $v = $fields[$k]->get($v);
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
    //  *
    //  * @return array
    //  */
    // private function createCommands(TableDefinition $definition): array
    // {
    //     $q = $this->config['quote'];
    //     $table = $definition->getTable();
    //     $fields = $definition->getFields();
    //     if ($definition->isForeignKeyTable($this->getName())) {
    //         $fields = [];
    //     }
    //     $pk = $definition->getPrimaryKey();
    //     $commands = [];
    //     $alters = [];
    //     $_fields = [];
    //     $map = [
    //         TableDefinition::TYPE_INT => 'INTEGER',
    //         TableDefinition::TYPE_BLOB => 'BYTEA',
    //         TableDefinition::TYPE_TEXT => 'TEXT',
    //         TableDefinition::TYPE_JSON => 'JSONB',
    //         TableDefinition::TYPE_FOREIGN_KEY => 'BIGINT',
    //         TableDefinition::TYPE_BOOL => 'BOOLEAN',
    //         TableDefinition::TYPE_FLOAT => 'REAL',
    //         TableDefinition::TYPE_DATE => 'DATE',
    //         TableDefinition::TYPE_DATETIME => 'TIMESTAMP(0) WITHOUT TIME ZONE',
    //         TableDefinition::TYPE_INT_ARRAY => 'INTEGER[]',
    //         TableDefinition::TYPE_TEXT_ARRAY => 'TEXT[]',
    //     ];
    //     $defaults = [
    //         TableDefinition::TYPE_INT => '0',
    //         TableDefinition::TYPE_FOREIGN_KEY => '0',
    //         TableDefinition::TYPE_BLOB => '\'\'::BYTEA',
    //         TableDefinition::TYPE_TEXT => '\'\'::TEXT',
    //         TableDefinition::TYPE_JSON => '\'{}\'::JSONB',
    //         TableDefinition::TYPE_BOOL => '\'f\'::BOOLEAN',
    //         TableDefinition::TYPE_FLOAT => '0',
    //         TableDefinition::TYPE_DATE => 'CURRENT_DATE',
    //         TableDefinition::TYPE_DATETIME => 'CURRENT_TIMESTAMP',
    //         TableDefinition::TYPE_INT_ARRAY => '\'{}\'::INTEGER[]',
    //         TableDefinition::TYPE_TEXT_ARRAY => '\'{}\'::TEXT[]',
    //     ];
    //     $seq = $table . '_seq';
    //     $pk_start_with = $definition->getPrimaryKeyStartWith($this->getName());
    //     $pk_increment_by = $definition->getPrimaryKeyIncrementBy($this->getName());
    //     $commands[] = "DROP SEQUENCE IF EXISTS {$q}{$seq}{$q} CASCADE";
    //     $commands[] = "
    //         CREATE SEQUENCE {$q}{$seq}{$q}
    //         AS BIGINT
    //         START WITH {$pk_start_with}
    //         INCREMENT BY {$pk_increment_by}
    //         MINVALUE {$pk_start_with}
    //     ";
    //     $rand = mt_rand();
    //     $alters[] = "
    //         ALTER TABLE {$q}{$table}{$q}
    //         ADD CONSTRAINT {$q}{$table}_{$rand}{$q}
    //         PRIMARY KEY ({$q}{$pk}{$q})
    //     ";
    //     $_fields[] = "
    //         {$q}{$pk}{$q} BIGINT DEFAULT
    //         nextval('{$seq}'::regclass) NOT NULL
    //     ";
    //     $uniques = [];
    //     $indexes = [];
    //     foreach ($fields as $field => $meta) {
    //         [
    //             'is_nullable' => $is_nullable,
    //             'type' => $type,
    //             'default' => $default,
    //             'unique' => $unique,
    //             'index' => $index,
    //         ] = $meta;
    //         foreach ($unique as $_) {
    //             @ $uniques[$_][] = $field;
    //         }
    //         foreach ($index as $_) {
    //             @ $indexes[$_][] = $field;
    //         }
    //         $default = $default ?? ($is_nullable ? 'NULL' : ($defaults[$type] ?? null));
    //         $default = (string) $default;
    //         $_field = $q . $field . $q . ' ' . $map[$type];
    //         if ($default !== '') {
    //             $_field .= ' DEFAULT ' . $default;
    //         }
    //         $_field .= ' ' . ($is_nullable ? 'NULL' : 'NOT NULL');
    //         $_fields[] = $_field;
    //         if ($type === TableDefinition::TYPE_FOREIGN_KEY) {
    //             $c = $table . '_' . mt_rand();
    //             $alters[] = "
    //                 ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$c}{$q} FOREIGN KEY ({$field})
    //                 REFERENCES {$meta['references']} ON DELETE CASCADE ON UPDATE CASCADE
    //             ";
    //             @ $indexes['HASH:' . mt_rand()][] = $field;
    //         }
    //     }
    //     foreach ($uniques as $unique => $fs) {
    //         $fs = implode(', ', array_map(function ($f) use ($q) {
    //             return $q . $f . $q;
    //         }, $fs));
    //         $c = $table . '_' . mt_rand();
    //         $alters[] = "ALTER TABLE {$q}{$table}{$q} ADD CONSTRAINT {$q}{$c}{$q} UNIQUE ({$fs})";
    //     }
    //     foreach ($indexes as $index => $fs) {
    //         $index = explode(':', $index);
    //         $type = count($index) > 1 ? $index[0] : 'BTREE';
    //         $fs = implode(', ', array_map(function ($f) use ($q) {
    //             return $q . $f . $q;
    //         }, $fs));
    //         $c = $table . '_' . mt_rand();
    //         $alters[] = "CREATE INDEX {$q}{$c}{$q} ON {$q}{$table}{$q} USING {$type} ({$fs})";
    //     }
    //     $_fields = implode(', ', $_fields);
    //     $commands[] = "CREATE TABLE {$q}{$table}{$q} ({$_fields})";
    //     // print_r($commands);
    //     // print_r($alters);
    //     return array_merge($commands, $alters);
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
