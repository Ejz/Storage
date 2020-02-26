<?php

namespace Ejz;

use Countable;

class WhereCondition implements Countable
{
    /** @var array */
    private $conditions;

    /**
     * @param array $conditions (optional)
     */
    public function __construct(array $conditions = [])
    {
        if (count($conditions) === count(array_filter(array_keys($conditions), 'is_string'))) {
            $conditions = array_map(null, array_keys($conditions), array_values($conditions));
        }
        $this->conditions = $conditions;
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
     * @param string $quote (optional)
     *
     * @return array
     */
    public function stringify(string $quote = ''): array
    {
        $where = [];
        $args = [];
        $map = ['=' => 'IN', '!=' => 'NOT IN'];
        foreach ($this->conditions as $condition) {
            [$field, $value] = $condition;
            $operation = $condition[2] ?? '=';
            $pattern = $condition[3] ?? '%s';
            $field = sprintf($pattern, $quote . $field . $quote);
            if (is_array($value) && count($value) > 0 && isset($map[$operation])) {
                $where[] = sprintf(
                    '(%s %s (%s))',
                    $field,
                    $map[$operation],
                    implode(', ', array_fill(0, count($value), '?'))
                );
                array_push($args, ...$value);
            } elseif (!is_array($value)) {
                $where[] = sprintf('(%s %s ?)', $field, $operation);
                $args[] = $value;
            } else {
                $where[] = '(FALSE)';
            }
        }
        $where = $where ?: ['(FALSE)'];
        return ['WHERE ' . implode(' AND ', $where), $args];
    }

    /**
     * @return int
     */
    public function count(): int
    {
        return count($this->conditions);
    }
}