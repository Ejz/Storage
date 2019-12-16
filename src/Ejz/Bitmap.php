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
     * @param  TableDefinition $definition
     * @param  int             $id
     * @param  array           $values
     */
    public function upsert(TableDefinition $definition, int $id, array $values)
    {
        $table = $definition->getTable();
        $_values = ['', []];
        $this->execute('ADD ? ?' . $_values[0], $table, $id, ...$_values[1]);
    }

    /**
     * @param string $table
     */
    public function drop(string $table)
    {
        try {
            $this->execute('DROP ?', $table);
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
        $_fields = ['', []];
        $this->execute('CREATE ?' . $_fields[0], $table, ...$_fields[1]);
    }
}
