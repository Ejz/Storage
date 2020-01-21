<?php

namespace Ejz;

use Throwable;
use Amp\Iterator;

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
        if (!$repository->hasBitmap()) {
            return;
        }
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
        $ret = $this->client->ADD(...$args);
        return $ret;
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
