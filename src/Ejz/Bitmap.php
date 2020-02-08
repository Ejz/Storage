<?php

namespace Ejz;

use Throwable;
use Amp\Iterator;
use Amp\Promise;
use Amp\Success;
use Ejz\Type\AbstractType;
use RuntimeException;

class Bitmap
{
    /** @var string */
    protected $name;

    /** @var RedisClient */
    protected $client;

    /** @var string */
    private const METHOD_NOT_FOUND = 'METHOD_NOT_FOUND: %s';

    /**
     * @param RedisClient $client
     */
    public function __construct(string $name, RedisClient $client)
    {
        $this->name = $name;
        $this->client = $client;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->getName();
    }

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function drop(string $index): Promise
    {
        try {
            $this->client->DROP($index);
        } catch (Throwable $e) {
        }
        return new Success();
    }

    /**
     * @param Repository $repository
     *
     * @return Promise
     */
    public function create(Repository $repository): Promise
    {
        $index = $repository->getBitmapIndex();
        $fields = $repository->getBitmapFields();
        $args = ['FIELDS', '_shard', 'STRING'];
        foreach ($fields as $name => $field) {
            $args[] = $name;
            $type = $field->getType();
            $args[] = $this->getFieldTypeString($type);
            if ($type->is(Type::bitmapForeignKey())) {
                $args[] = $type->getParentTable();
            }
        }
        $this->client->CREATE($index, ...$args);
        return new Success();
    }

    /**
     * @param Repository $repository
     *
     * @return Promise
     */
    public function createNew($index, $fields): Promise
    {
        $args = ['FIELDS', '_shard', 'STRING'];
        foreach ($fields as $name => $field) {
            $args[] = $name;
            $type = $field->getType();
            $args[] = $this->getFieldTypeString($type);
            if ($type->is(Type::bitmapForeignKey())) {
                $args[] = $type->getParentTable();
            }
        }
        $this->client->CREATE($index, ...$args);
        return new Success();
    }

    /**
     * @param string $index
     * @param int    $id
     * @param array  $fields
     * @param mixed  $pool
     *
     * @return Promise
     */
    public function add(string $index, int $id, array $fields, $pool): Promise
    {
        $name = $pool->random()->getName();
        $args = ['VALUES', '_shard', $name];
        foreach ($fields as $field) {
            $args[] = $field->getName();
            $args[] = $field->exportValue();
        }
        $ret = $this->client->ADD($index, $id, ...$args);
        return new Success($id);
    }

    /**
     * @return Promise
     */
    public function list(): Promise
    {
        return new Success($this->client->LIST());
    }

    /**
     * @param string       $index
     * @param string|array $query
     *
     * @return Emitter
     */
    public function search(Repository $repository, $query): Emitter
    {
        $emitter = new Emitter();
        if (is_string($query)) {
            $index = $repository->getBitmapIndex();
            $result = $this->client->SEARCH($index, $query, 'WITHCURSOR');
            [$size, $cursor] = [$result[0] ?? 0, $result[1] ?? null];
            $left = $size;
            $ids = [];
        } else {
            [
                'size' => $size,
                'cursor' => $cursor,
                'ids' => $ids,
                'left' => $left,
            ] = $query;
            $left += count($ids);
        }
        $emitter->setSize($size);
        $emitter->setContext(compact('cursor', 'ids', 'left'));
        $coroutine = \Amp\call(function ($left, $cursor, $repository, $ids, $emitter) {
            while ($left > 0) {
                $ids = $ids ?: $this->client->CURSOR($cursor, 'LIMIT', 10);
                $left -= count($ids);
                $_ids = array_flip($ids);
                $emitter->setContext($left, 'left');
                $emitter->setContext($_ids, 'ids');
                $iterator = $repository->get($ids);
                $iterator = Emitter::getIteratorWithIdsOrder($iterator, $ids);
                while (($value = yield $iterator->pull()) !== null) {
                    yield $emitter->push($value);
                    $_ids[$value[0]] = null;
                    $emitter->setContext($_ids, 'ids');
                }
                $ids = [];
            }
        }, $left, $cursor, $repository, $ids, $emitter);
        $coroutine->onResolve(function ($e) use ($emitter) {
            $emitter->finish();
        });
        return $emitter;
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

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        if (substr($name, -4) === 'Sync') {
            $name = substr($name, 0, -4);
            return Promise\wait($this->$name(...$arguments));
        }
        throw new RuntimeException(sprintf(self::METHOD_NOT_FOUND, $name));
    }
}
