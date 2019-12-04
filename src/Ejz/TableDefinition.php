<?php

namespace Ejz;

use RuntimeException;

class TableDefinition
{
    const TYPE_INT = 'int';
    const TYPE_BLOB = 'blob';
    const TYPE_TEXT = 'text';
    const TYPE_JSON = 'json';

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
        $this->normalizeFields();
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
    private function normalizeFields()
    {
        $fields = $this->definition['fields'];
        foreach ($fields as $field => &$meta) {
            $meta['type'] = $meta['type'] ?? self::TYPE_TEXT;
            $meta['get_pattern'] = $meta['get_pattern'] ?? '%s';
            $meta['set_pattern'] = $meta['set_pattern'] ?? '?';
            $meta['is_nullable'] = !empty($meta['is_nullable']);
            $meta['database_default'] = $meta['database_default'] ?? null;
            $meta['unique'] = (array) ($meta['unique'] ?? []);
            if ($meta['type'] === self::TYPE_JSON) {
                $meta['set'] = $meta['set'] ?? function ($_) { return json_encode($_); };
                $meta['get'] = $meta['get'] ?? function ($_) { return json_decode($_, true); };
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
     * @param mixed $filter (optional)
     *
     * @return array
     */
    public function getFields($filter = null): array
    {
        $fields = $this->definition['fields'];
        $fields = $filter !== null ? array_filter($fields, function ($value, $key) use ($filter) {
            if (is_array($filter)) {
                return in_array($value['field'] ?? $key, $filter);
            }
            return in_array($filter, (array) ($value['filter'] ?? []));
        }, ARRAY_FILTER_USE_BOTH) : $fields;
        return $fields;
    }

    /**
     * @param array $values
     *
     * @return array
     */
    public function setDefaultValues(array $values): array
    {
        $fields = $this->getFields();
        foreach ($fields as $key => $value) {
            if (array_key_exists('default', $value) && !array_key_exists($key, $values)) {
                if (is_callable($value['default'])) {
                    $values[$key] = $value['default']($values);
                } else {
                    $values[$key] = $value['default'];
                }
            }
        }
        return $values;
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
        return $this->definition['get_shards']($id, $values, $shards);
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
     * @param mixed $filter
     *
     * @return array
     */
    public function getFieldsForIterate($filter): array
    {
        $fields = $this->getFields($filter);
        $ret = [];
        foreach ($fields as $key => $value) {
            $ret[$key] = [
                'pattern' => $value['get_pattern'] ?? '%s',
                'field' => $value['field'] ?? $key,
            ];
        }
        return $ret;
    }
}
