<?php

namespace Ejz;

use Throwable;
use Amp\Iterator;

class Bitmap
{
    /** @var RedisClient */
    protected $client;

    /** @var array */
    protected $config;

    /**
     * @param RedisClient $client
     * @param array       $config (optional)
     */
    public function __construct(RedisClient $client, array $config = [])
    {
        $this->client = $client;
        $this->config = $config + [
            'iterator_chunk_size' => 10,
        ];
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
     * @param Repository $repository
     */
    public function CREATE(Repository $repository)
    {
        $table = $repository->getTable();
        $args = [$table];
        $fields = $repository->getBitmapFields();
        $args = array_merge($args, $fields ? ['FIELDS'] : []);
        foreach ($fields as $name => $field) {
            $args[] = $name;
            $type = $field->getType();
            $args[] = $this->getFieldTypeString($type);
            if ($type->is(Type::bitmapForeignKey())) {
                $args[] = $type->getParentTable();
            }
        }
        $this->client->CREATE(...$args);
    }

    /**
     * @param string $table
     * @param int    $id
     * @param array  $fields
     *
     * @return bool
     */
    public function ADD(string $table, int $id, array $fields): bool
    {
        $args = [$table, $id];
        $args = array_merge($args, $fields ? ['VALUES'] : []);
        foreach ($fields as $field) {
            $args[] = $field->getName();
            $args[] = $field->exportValue();
        }
        return $this->client->ADD(...$args);
    }

    /**
     * @return Iterator
     */
    public function SEARCH(string $table, string $query): Iterator
    {
        $result = $this->client->SEARCH($table, $query, 'WITHCURSOR');
        $size = $result[0] ?? 0;
        $cursor = $result[1] ?? null;
        $iterator_chunk_size = $this->config['iterator_chunk_size'];
        $emit = function ($emit) use ($cursor, $size, $iterator_chunk_size) {
            if ($cursor === null) {
                return;
            }
            while ($size > 0) {
                $ids = $this->client->CURSOR($cursor, 'LIMIT', $iterator_chunk_size);
                $size -= $iterator_chunk_size;
                $iterator = $this->repository->get($ids);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
            }
        };
        $iterator = new Producer($emit);
        $iterator->setSize($size);
        return $iterator;
    }

    /**
     * @param AbstractType $type
     *
     * @return string
     */
    private function getFieldTypeString(AbstractType $type): string
    {
        static $map;
        if ($map === null) {
            $map = [
                (string) Type::bitmapBool() => 'BOOLEAN',
                (string) Type::bitmapForeignKey() => 'FOREIGNKEY',
            ];
        }
        return $map[(string) $type];
    }
}
