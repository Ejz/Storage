<?php

namespace Ejz;

use Throwable;
// use Amp\Iterator;
use Amp\Promise;
use Ejz\Type\AbstractType;
use RuntimeException;

class Bitmap implements NameInterface, BitmapInterface
{
    use NameTrait;
    use SyncTrait;

    /** @var RedisClient */
    protected $client;

    /** @var array */
    protected $config;

    /** @var string */
    public const ID_FIELD = 'id';

    /**
     * @param string      $name
     * @param RedisClient $client
     */
    public function __construct(string $name, RedisClient $client, array $config = [])
    {
        $this->setName($name);
        $this->client = $client;
        $this->config = $config + [
            'iterator_chunk_size' => 30,
        ];
    }

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function drop(string $index): Promise
    {
        return \Amp\call(function ($index) {
            if (!yield $this->indexExists($index)) {
                return false;
            }
            $this->client->DROP($index);
            return true;
        }, $index);
    }

    /**
     * @return Promise
     */
    public function indexes(): Promise
    {
        return \Amp\call(function () {
            return $this->client->LIST();
        });
    }

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function indexExists(string $index): Promise
    {
        return \Amp\call(function ($index) {
            return in_array($index, yield $this->indexes());
        }, $index);
    }

    /**
     * @param string $index
     * @param array  $fields (optional)
     *
     * @return Promise
     */
    public function create(string $index, array $fields = []): Promise
    {
        return \Amp\call(function ($index, $fields) {
            $args = ['FIELDS'];
            foreach ($fields as $field) {
                $args[] = $field->getName();
                $type = $field->getType();
                $args[] = $this->getFieldTypeString($type);
                if ($type->is(Type::bitmapForeignKey())) {
                    $args[] = $type->getParentTable();
                }
                if ($type->is(Type::bitmapArray())) {
                    $args[] = 'SEPARATOR';
                    $args[] = $type->getSeparator();
                }
            }
            $args = count($args) > 1 ? $args : [];
            $this->client->CREATE($index, ...$args);
        }, $index, $fields);
    }  

    /**
     * @param string $index
     * @param int    $id
     * @param array  $fields (optional)
     *
     * @return Promise
     */
    public function add(string $index, int $id, array $fields = []): Promise
    {
        return \Amp\call(function ($index, $id, $fields) {
            $args = ['VALUES'];
            foreach ($fields as $field) {
                $value = $field->exportValue();
                if ($value === null) {
                    continue;
                }
                $args[] = $field->getName();
                $args[] = $value;
            }
            $args = count($args) > 1 ? $args : [];
            $this->client->ADD($index, $id, ...$args);
            return $id;
        }, $index, $id, $fields);
    }

    /**
     * @param string $index
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function search(string $index, array $params = []): Iterator
    {
        $iterator = new Iterator();
        $emit = function ($emit) use ($index, $params, $iterator) {
            $params += [
                'query' => '*',
                'cursor' => [],
                'sortby' => self::ID_FIELD,
                'asc' => true,
                'fks' => [],
            ];
            [
                'query' => $query,
                'cursor' => $cursor,
                'sortby' => $sortby,
                'asc' => $asc,
                'fks' => $fks,
            ] = $params;
            $fks = (array) $fks;
            $args = [
                $index,
                $query,
                'SORTBY',
                $sortby,
                $asc ? 'ASC' : 'DESC',
                'LIMIT',
                0,
                1000,
            ];
            foreach ($fks as $fk) {
                $args[] = 'APPENDFK';
                $args[] = $fk;
            }
            $ids = $this->client->SEARCH(...$args);
            $size = array_shift($ids);
            // var_dump($size);
            $iterator->setContext($size, 'size');
            while (($value = array_shift($ids)) !== null) {
                $value = $fks ? $value : [$value];
                $id = array_shift($value);
                yield $emit([$id, $value]);
            }
        };
        $iterator->setIterator($emit);
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
                (string) Type::bitmapDate() => 'DATE',
                (string) Type::bitmapDateTime() => 'DATETIME',
                (string) Type::bitmapString() => 'STRING',
                (string) Type::bitmapInt() => 'INTEGER',
                (string) Type::bitmapArray() => 'ARRAY',
                (string) Type::bitmapForeignKey() => 'FOREIGNKEY',
            ];
        }
        return $map[(string) $type];
    }
}
