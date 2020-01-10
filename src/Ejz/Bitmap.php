<?php

namespace Ejz;

use Throwable;

class Bitmap
{
    /** @var RedisClient */
    protected $client;

    /**
     * @param RedisClient $client
     */
    public function __construct(RedisClient $client)
    {
        $this->client = $client;
    }

    /**
     * @param string $cmd
     * @param array  ...$args
     *
     * @return mixed
     */
    public function execute(string $cmd, ...$args)
    {
        $cmd = preg_split('~\s+~', trim($cmd));
        $cmd = array_map(function ($elem) use (&$args) {
            return $elem === '?' ? array_shift($args) : $elem;
        }, $cmd);
        $c = array_shift($cmd);
        return $this->client->$c(...$cmd);
    }

    /**
     * @param string $method
     * @param array  $args
     *
     * @return mixed
     */
    public function __call(string $method, array $args)
    {
        return $this->client->$method(...$args);
    }

    /**
     * @param TableDefinition $definition
     * @param int             $id
     * @param array           $values
     */
    public function upsert(TableDefinition $definition, int $id, array $values)
    {
        $table = $definition->getTable();
        $fields = $definition->getBitmapFields();
        $values = array_map(function ($value) {
            return $value['value'];
        }, $values);
        $_values = [[], []];
        foreach ($values as $field => $value) {
            $meta = $fields[$field] ?? null;
            if ($meta !== null) {
                $getter = $meta['getter'] ?? null;
                $_values[0][] = '? ?';
                $_values[1][] = $field;
                $_values[1][] = $getter === null ? $value : $getter($values);
            }
        }
        $_values[0] = ($_values[0] ? ' VALUES ' : '') . implode(' ', $_values[0]);
        $this->execute('ADD ? ?' . $_values[0], $table, $id, ...$_values[1]);
    }

    /**
     * @param string $table
     */
    public function DROP(string $table)
    {
        try {
            $this->client->DROP($table);
        } catch (Throwable $e) {
        }
    }

    /**
     * @param TableDefinition $definition
     */
    public function create(TableDefinition $definition)
    {
        $table = $definition->getTable();
        $this->drop($table);
        $fields = $definition->getBitmapFields();
        $_fields = [[], []];
        foreach ($fields as $field => $meta) {
            if ($meta['type'] === TableDefinition::TYPE_FOREIGN_KEY) {
                $_fields[0][] = '? FOREIGNKEY ?';
                $_fields[1][] = $field;
                $_fields[1][] = $meta['references'];
            } elseif ($meta['type'] === TableDefinition::TYPE_BOOL) {
                $_fields[0][] = '? BOOLEAN';
                $_fields[1][] = $field;
            }
        }
        $_fields[0] = ($_fields[0] ? ' FIELDS ' : '') . implode(' ', $_fields[0]);
        $this->execute('CREATE ?' . $_fields[0], $table, ...$_fields[1]);
    }
}
