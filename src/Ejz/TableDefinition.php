<?php

namespace Ejz;

use RuntimeException;

class TableDefinition
{
    const TYPE_INT = 'INT';
    const TYPE_FLOAT = 'FLOAT';
    const TYPE_BLOB = 'BLOB';
    const TYPE_TEXT = 'TEXT';
    const TYPE_JSON = 'JSON';
    const TYPE_BOOL = 'BOOL';
    const TYPE_DATE = 'DATE';
    const TYPE_DATETIME = 'DATETIME';
    const TYPE_INT_ARRAY = 'INT_ARRAY';
    const TYPE_TEXT_ARRAY = 'TEXT_ARRAY';
    const TYPE_FOREIGN_KEY = 'FOREIGN_KEY';

    const INVALID_FIELD_ERROR = 'INVALID_FIELD_ERROR: %s';

    /** @var string */
    private $table;

    /** @var array */
    private $definition;

    /** @var array */
    private $shards;

    /** @var array */
    private $allShards;

    /**
     * @param string $table
     * @param array  $definition
     */
    public function __construct(string $table, array $definition, array $shards)
    {
        $this->table = $table;
        $this->definition = $definition;
        $this->shards = $shards;
        $this->shards = $this->getAllShards();
        $this->allShards = $this->shards;
        $this->setDefaults();
        $this->test();
    }

    /**
     *
     */
    private function test()
    {
        if (false) {
            throw new RuntimeException();
        }
    }

    /**
     *
     */
    private function setDefaults()
    {
        $fields = $this->definition['fields'] ?? [];
        $collect = [];
        foreach ($fields as $field => $meta) {
            if (!is_array($meta)) {
                $meta = ['type' => $meta];
            }
            $meta['type'] = $meta['type'] ?? self::TYPE_TEXT;
            $meta['is_nullable'] = !empty($meta['is_nullable']);
            $meta['unique'] = (array) ($meta['unique'] ?? []);
            $meta['index'] = (array) ($meta['index'] ?? []);
            $meta['default'] = $meta['default'] ?? null;
            $meta['get'] = $meta['get'] ?? null;
            $meta['get_pattern'] = $meta['get_pattern'] ?? '%s';
            $meta['set'] = $meta['set'] ?? null;
            $meta['set_pattern'] = $meta['set_pattern'] ?? '?';
            unset($get, $set, $get_pattern, $set_pattern);
            if ($meta['type'] === self::TYPE_JSON) {
                $get = function ($_) {
                    return json_decode($_, true);
                };
                $set = function ($_) {
                    return json_encode($_);
                };
            }
            if ($meta['type'] === self::TYPE_BLOB) {
                $get = function ($_) {
                    return base64_decode($_);
                };
                $set = function ($_) {
                    return base64_encode($_);
                };
                $get_pattern = 'encode((%s)::BYTEA, \'base64\')';
                $set_pattern = 'decode((?)::TEXT, \'base64\')';
            }
            if ($get ?? null) {
                if ($g = $meta['get']) {
                    $meta['get'] = function ($_) use ($g, $get) {
                        return $g($get($_));
                    };
                } else {
                    $meta['get'] = $get;
                }
            }
            if ($set ?? null) {
                if ($s = $meta['set']) {
                    $meta['set'] = function ($_) use ($s, $set) {
                        return $set($s($_));
                    };
                } else {
                    $meta['set'] = $set;
                }
            }
            if ($get_pattern ?? null) {
                if ($g = $meta['get_pattern']) {
                    $meta['get_pattern'] = str_replace('%s', '(' . $g . ')', $get_pattern);
                } else {
                    $meta['get_pattern'] = $get_pattern;
                }
            }
            if ($set_pattern ?? null) {
                if ($s = $meta['set_pattern']) {
                    $meta['set_pattern'] = str_replace('?', '(' . $set_pattern . ')', $s);
                } else {
                    $meta['set_pattern'] = $set_pattern;
                }
            }
            $collect[$field] = $meta;
        }
        $this->definition['fields'] = $collect;
    }

    /**
     * @return string
     */
    public function getTable(): string
    {
        return $this->table;
    }

    /**
     * @return string
     */
    public function getPrimaryKey(): string
    {
        return $this->getTable() . '_id';
    }

    /**
     * @return array
     */
    public function getFields(): array
    {
        return $this->getDatabaseFields();
    }

    /**
     * @return array
     */
    public function getDatabaseFields(): array
    {
        return $this->definition['fields'];
    }

    /**
     * @return array
     */
    public function getBitmapFields(): array
    {
        $fields = $this->definition['fields'];
        return $fields;
    }

    /**
     * @return bool
     */
    public function hasBitmap(): bool
    {
        return !empty($this->definition['bitmap']);
    }

    /**
     * @param array $values
     *
     * @return int
     */
    public function getScore(array $values): int
    {
        $get_score = $this->definition['get_score'] ?? null;
        return $get_score !== null ? $get_score($values) : 0;
    }

    /**
     * @param ?array $values
     *
     * @return array
     */
    public function normalizeValues(array $values): array
    {
        $collect = [];
        $fields = $this->getFields();
        foreach ($values as $key => $value) {
            $modifiers = $this->getModifiers();
            [$field, $append] = [false, []];
            foreach ($modifiers as $regex => $callback) {
                if (preg_match($regex, $key, $match)) {
                    [$field, $append, $value] = $callback($match, $value);
                    break;
                }
            }
            $field = $field ?: $key;
            if (!isset($fields[$field])) {
                throw new RuntimeException(sprintf(self::INVALID_FIELD_ERROR, $field));
            }
            $value = ['value' => $value, 'field' => $field];
            $value['get'] = $fields[$field]['get'];
            $value['get_pattern'] = $fields[$field]['get_pattern'];
            $value['set'] = $fields[$field]['set'];
            $value['set_pattern'] = $fields[$field]['set_pattern'];
            if ($get = $append['get'] ?? null) {
                if ($g = $value['get']) {
                    $value['get'] = function ($_) use ($g, $get) {
                        return $get($g($_));
                    };
                } else {
                    $value['get'] = $get;
                }
            }
            if ($set = $append['set'] ?? null) {
                if ($s = $value['set']) {
                    $value['set'] = function ($_) use ($s, $set) {
                        return $s($set($_));
                    };
                } else {
                    $value['set'] = $set;
                }
            }
            if ($get_pattern = $append['get_pattern'] ?? null) {
                if ($g = $value['get_pattern']) {
                    $value['get_pattern'] = str_replace('%s', '(' . $g . ')', $get_pattern);
                } else {
                    $value['get_pattern'] = $get_pattern;
                }
            }
            if ($set_pattern = $append['set_pattern'] ?? null) {
                if ($s = $value['set_pattern']) {
                    $value['set_pattern'] = str_replace('?', '(' . $set_pattern . ')', $s);
                } else {
                    $value['set_pattern'] = $set_pattern;
                }
            }
            $collect[$append['alias'] ?? $key] = $value;
        }
        return $collect;
    }

    /**
     * @return array
     */
    public function getAllShards(): array
    {
        if (is_array($this->allShards)) {
            return $this->allShards;
        }
        $get_all_shards = $this->definition['get_all_shards'] ?? null;
        if ($get_all_shards !== null) {
            return $get_all_shards($this->shards);
        }
        return $this->shards;
    }

    /**
     * @param int $id
     *
     * @return array
     */
    public function getReadShardsById(int $id): array
    {
        $get = $this->definition['get_read_shards_by_id'] ?? null;
        if ($get !== null) {
            return $get($id, $this->shards);
        }
        $get = $this->definition['get_shards_by_id'] ?? null;
        if ($get !== null) {
            return $get($id, $this->shards);
        }
        return $this->shards;
    }

    /**
     * @param int $id
     *
     * @return array
     */
    public function getWriteShardsById(int $id): array
    {
        $get = $this->definition['get_write_shards_by_id'] ?? null;
        if ($get !== null) {
            return $get($id, $this->shards);
        }
        $get = $this->definition['get_shards_by_id'] ?? null;
        if ($get !== null) {
            return $get($id, $this->shards);
        }
        return $this->shards;
    }

    /**
     * @param array $values
     *
     * @return array
     */
    public function getReadShardsByValues(array $values): array
    {
        $get = $this->definition['get_read_shards_by_values'] ?? null;
        if ($get !== null) {
            return $get($values, $this->shards);
        }
        $get = $this->definition['get_shards_by_values'] ?? null;
        if ($get !== null) {
            return $get($values, $this->shards);
        }
        return $this->shards;
    }

    /**
     * @param array $values
     *
     * @return array
     */
    public function getWriteShardsByValues(array $values): array
    {
        $get = $this->definition['get_write_shards_by_values'] ?? null;
        if ($get !== null) {
            return $get($values, $this->shards);
        }
        $get = $this->definition['get_shards_by_values'] ?? null;
        if ($get !== null) {
            return $get($values, $this->shards);
        }
        return $this->shards;
    }

    /**
     * @return bool
     */
    public function isCacheable(): bool
    {
        return !empty($this->definition['is_cacheable']);
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public function isForeignKeyTable(string $name): bool
    {
        $_ = $this->definition['is_foreign_key_table'] ?? null;
        return $_ === null ? false : $_($name, $this->shards);
    }

    /**
     * @param string $name
     *
     * @return int
     */
    public function getPrimaryKeyStartWith(string $name): int
    {
        $v = $this->definition['primary_key_start_with'] ?? 1;
        return (int) (is_callable($v) ? $v($name, $this->shards) : $v);
    }

    /**
     * @param string $shard
     *
     * @return int
     */
    public function getPrimaryKeyIncrementBy(string $name): int
    {
        $v = $this->definition['primary_key_increment_by'] ?? 1;
        return (int) (is_callable($v) ? $v($name, $this->shards) : $v);
    }

    /**
     * @return array
     */
    private function getModifiers(): array
    {
        $modifiers = $this->definition['modifiers'] ?? [];
        return $modifiers + $this->getDefaultModifiers();
    }

    /**
     * @return array
     */
    private function getDefaultModifiers(): array
    {
        return [
            '~^(.*)\[\]$~' => function ($match, $value) {
                $append = [
                    'set_pattern' => '%s || ?',
                ];
                $value = is_array($value) ? $value : [$value];
                return [$match[1], $append, $value];
            },
            '~^(.*)\{\}$~' => function ($match, $value) {
                $append = [
                    'set_pattern' => '%s || ?',
                ];
                return [$match[1], $append, $value];
            },
        ];
    }
}
