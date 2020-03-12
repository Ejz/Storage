<?php

namespace Ejz;

use Countable;

class WhereCondition implements Countable
{
    /** @var array */
    private $conditions;

    /**
     * @param ?FieldPool $fieldPool  (optional)
     * @param array      $conditions (optional)
     */
    public function __construct(array $conditions = [])
    {
        if (count($conditions) === count(array_filter(array_keys($conditions), 'is_string'))) {
            $this->conditions = array_map(null, array_keys($conditions), array_values($conditions));
        } else {
            $this->conditions = array_values($conditions);
        }
    }

    /**
     * @param string  $field
     * @param mixed   $value
     * @param ?string $operation (optional)
     * @param ?string $pattern   (optional)
     */
    public function append(string $field, $value, ?string $operation = null, ?string $pattern = null)
    {
        $this->conditions[] = [$field, $value, $operation, $pattern];
    }

    /**
     * @return array
     */
    public function stringify(): array
    {
        $where = [];
        $args = [];
        $map = ['=' => 'IN', '!=' => 'NOT IN'];
        foreach ($this->conditions as $condition) {
            [$field, $value] = $condition;
            $operation = $condition[2] ?? '=';
            $pattern = $condition[3] ?? '$field $operation $value';
            $mapped = $map[$operation] ?? null;
            $value = is_array($value) ? array_values($value) : [$value];
            $count = count($value);
            if ($mapped !== null) {
                if (!$count) {
                    $where[] = $operation === '=' ? '(FALSE)' : '(TRUE)';
                    continue;
                }
                $pattern = preg_replace_callback('~\\$value~i', function ($match) use ($count) {
                    return '(' . implode(', ', array_fill(0, $count, '$value')) . ')';
                }, $pattern);
            }
            $pos = 0;
            $operation = $mapped ?? $operation;
            $callback = function ($match) use (&$args, &$pos, $value, $field, $operation) {
                $what = $match[1];
                if ($what === 'operation') {
                    $args[] = $operation;
                    return '%';
                }
                if ($what === 'field') {
                    $args[] = $field;
                    return '#';
                }
                $args[] = $value[$pos++] ?? null;
                return '?';
            };
            $pattern = preg_replace_callback('~\\$(field|operation|value)~i', $callback, $pattern);
            $where[] = '(' . $pattern . ')';
        }
        $where = $where ?: ['(FALSE)'];
        return ['WHERE ' . implode(' AND ', $where), $args];
    }

    /**
     * @return string
     */
    public function key(): string
    {
        $keys = [];
        $count = count($this->conditions);
        if ($count > 1) {
            $keys = array_map(function ($condition) {
                return (new self([$condition]))->key();
            }, $this->conditions);
            sort($keys, SORT_STRING);
        } else {
            $keys = [md5(serialize($this->stringify()))];
        }
        return md5(serialize($keys));
    }

    /**
     * @return int
     */
    public function count(): int
    {
        return count($this->conditions);
    }
}