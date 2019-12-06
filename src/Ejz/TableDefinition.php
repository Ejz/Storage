<?php

namespace Ejz;

use RuntimeException;

class TableDefinition
{
    const TYPE_INT = 'int';
    const TYPE_BLOB = 'blob';
    const TYPE_TEXT = 'text';
    const TYPE_JSON = 'json';

    const INVALID_FIELD_ERROR = 'INVALID_FIELD_ERROR: %s';

    /** @var string */
    private $table;

    /** @var array */
    private $definition;

    /**
     * @param string $table
     * @param array  $definition
     */
    public function __construct(string $table, array $definition)
    {
        $this->table = $table;
        $this->definition = $definition;
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
        foreach ($fields as $field => &$meta) {
            $meta['type'] = $meta['type'] ?? self::TYPE_TEXT;
            $meta['is_nullable'] = !empty($meta['is_nullable']);
            $meta['unique'] = (array) ($meta['unique'] ?? []);
            // $meta['tags'] = (array) ($meta['tags'] ?? []);
            $meta['default'] = $meta['default'] ?? null;
            $meta['get'] = $meta['get'] ?? null;
            $meta['get_pattern'] = $meta['get_pattern'] ?? '%s';
            $meta['set'] = $meta['set'] ?? null;
            $meta['set_pattern'] = $meta['set_pattern'] ?? '?';
            if ($meta['type'] === self::TYPE_JSON) {
                $get = function ($_) {
                    return json_decode($_, true);
                };
                $set = function ($_) {
                    return json_encode($_);
                };
                if ($g = $meta['get']) {
                    $meta['get'] = function ($_) use ($g, $get) {
                        return $g($get($_));
                    };
                }
                if ($s = $meta['set']) {
                    $meta['set'] = function ($_) use ($s, $set) {
                        return $set($s($_));
                    };
                }
                $meta['get'] = $meta['get'] ?? $get;
                $meta['set'] = $meta['set'] ?? $set;
            }
        }
        unset($meta);
        $this->definition['fields'] = $fields;
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
        return $this->definition['fields'];
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
                    [$field, $append] = $callback($match);
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
                }
                $value['get'] = $value['get'] ?? $get;
            }
            if ($get = $append['get_pattern'] ?? null) {
                if ($g = $value['get_pattern']) {
                    $value['get_pattern'] = str_replace('%s', $get, $g);
                }
                $value['get_pattern'] = $value['get_pattern'] ?? $get;
            }
            if ($set = $append['set'] ?? null) {
                if ($s = $value['set']) {
                    $value['set'] = function ($_) use ($s, $set) {
                        return $s($set($_));
                    };
                }
                $value['set'] = $value['set'] ?? $set;
            }
            if ($set = $append['set_pattern'] ?? null) {
                if ($s = $value['set_pattern']) {
                    $value['set_pattern'] = str_replace('?', $set, $s);
                }
                $value['set_pattern'] = $value['set_pattern'] ?? $set;
            }
            $collect[$append['alias'] ?? $key] = $value;
        }
        return $collect;
        // $values = $collect;
        // foreach ($this->normalizeFields() as $key => $value) {
        //     if (array_key_exists('default', $value) && !array_key_exists($key, $values)) {
        //         if (is_callable($value['default'])) {
        //             $values[$key]['value'] = $value['default']($values);
        //         } else {
        //             $values[$key]['value'] = $value['default'];
        //         }
        //     }
        // }
        // return $values;
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return array
     */
    public function getShards(?int $id, array $values = []): array
    {
        $shards = $this->definition['shards'];
        if ($this->definition['get_shards'] ?? null) {
            return $this->definition['get_shards']($id, $values, $shards);
        }
        return $shards;
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return string
     */
    public function getShard(?int $id, array $values = []): string
    {
        $shards = $this->getShards($id, $values);
        return $shards[array_rand($shards)];
    }

    /**
     * @param string $shard
     *
     * @return int
     */
    public function getPrimaryKeyStartWith(string $shard): int
    {
        $shards = $this->definition['shards'];
        $val = $this->definition['primary_key_start_with'] ?? 1;
        return (int) (is_callable($val) ? $val($shard, $shards) : $val);
    }

    /**
     * @param string $shard
     *
     * @return int
     */
    public function getPrimaryKeyIncrementBy(string $shard): int
    {
        $shards = $this->definition['shards'];
        $val = $this->definition['primary_key_increment_by'] ?? 1;
        return (int) (is_callable($val) ? $val($shard, $shards) : $val);
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
            '~^(.*)\{\}$~' => function ($match) {
                $append = [];
                return [$match[1], $append];
            },
        ];
    }
}
