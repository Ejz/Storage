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

    /** @var array */
    protected $searchIteratorStates;

    /** @var string */
    private const METHOD_NOT_FOUND = 'METHOD_NOT_FOUND: %s';

    /**
     * @param RedisClient $client
     */
    public function __construct(string $name, RedisClient $client)
    {
        $this->name = $name;
        $this->client = $client;
        $this->searchIteratorStates = [];
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
        $args = $fields ? ['FIELDS'] : [];
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
     *
     * @return Promise
     */
    public function add(string $index, int $id, array $fields): Promise
    {
        $args = $fields ? ['VALUES'] : [];
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
     * @return Iterator
     */
    public function search(Repository $repository, $query): Iterator
    {
        if (is_string($query)) {
            $index = $repository->getBitmapIndex();
            $result = $this->client->SEARCH($index, $query, 'WITHCURSOR');
            [$size, $cursor] = [$result[0] ?? 0, $result[1] ?? null];
            $left = $size;
            $ids = null;
        } else {
            $state = $query[$this->name] + [
                'pointer' => 0,
                'ids' => [],
                'left' => null,
            ];
            [
                'size' => $size,
                'cursor' => $cursor,
                'pointer' => $pointer,
                'ids' => $ids,
                'left' => $left,
            ] = $state;
            $left = $left ?? $size;
            if ($pointer > 0) {
                $ids = array_slice($ids, $pointer);
            }
            $left += count($ids);
        }
        $me = new Producer();
        $emit = function ($emit) use ($left, $cursor, $repository, $ids) {
            while ($left > 0) {
                $ids = $ids ?? $this->client->CURSOR($cursor, 'LIMIT', 10);
                $left -= count($ids);
                $state = ['left' => $left, 'pointer' => 0, 'ids' => $ids];
                $this->setSearchIteratorState($cursor, $state);
                $iterator = $repository->get($ids);
                $iterator = Producer::getIteratorWithIdsOrder($iterator, $ids);
                while (yield $iterator->advance()) {
                    $this->moveSearchIteratorStatePointer($cursor);
                    yield $emit($iterator->getCurrent());
                }
                $ids = null;
            }
            $this->setSearchIteratorState($cursor, ['left' => 0]);
        };
        $me->setIterator($emit);
        $me->setSize($size);
        $me->setCursor($cursor);
        $me->setBitmap($this);
        return $me;
    }

    public function getSearchIteratorState($cursor)
    {
        return $this->searchIteratorStates[$cursor];
    }

    public function setSearchIteratorState($cursor, $state)
    {
        $this->searchIteratorStates[$cursor] = $state;
    }

    public function moveSearchIteratorStatePointer($cursor)
    {
        $this->searchIteratorStates[$cursor]['pointer']++;
    }

    /**
     * @param Repository $repository
     * @param int        $size
     * @param string     $cursor
     * @param array      $ids
     *
     * @return Iterator
     */
    public function restoreSearch(
        Repository $repository,
        int $size,
        int $left,
        string $cursor,
        array $ids
    ): Iterator {
        $client = $this->client;
        $emit = function ($emit) use ($repository, $left, $cursor, $ids, $client) {
            while ($left > 0) {
                $ids = $ids ?? $client->CURSOR($cursor, 'LIMIT', 10);
                $left -= count($ids);
                $iterator = $repository->get($ids);
                $iterator = Producer::getIteratorWithIdsOrder($iterator, $ids);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
                unset($ids);
            }
        };
        $iterator = new Producer($emit, true);
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
