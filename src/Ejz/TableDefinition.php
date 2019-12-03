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
     * @return array
     */
    public function getFields(): array
    {
        return $this->definition['fields'];
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
        $shards = $this->definition['names'];
        return $this->definition['get_shards']($id, $values, $shards);
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return int
     */
    public function getShard(?int $id, array $values = []): int
    {
        $shards = $this->getShards($id, $values);
        return $shards[array_rand($shards)];
    }

    /**
     * @return int
     */
    public function getPrimaryKeyStartWith(): int
    {
        return (int) ($this->definition['primary_key_start_with'] ?? 1);
    }

    /**
     * @return int
     */
    public function getPrimaryKeyIncrementBy(): int
    {
        return (int) ($this->definition['primary_key_increment_by'] ?? 1);
    }
}
