<?php

namespace Ejz;

class Condition
{

    public function __construct($conditions)
    {
        $collect = [];
        foreach ($conditions as $field => $value) {
            $operation = is_array($value) ? 'IN' : '=';
            $collect[] = compact('field', 'value', 'operation');
        }
        $this->conditions = $collect;
        $this->pushed = [];
    }

    public function push($field, $operation, $value)
    {
        $this->pushed[] = compact('field', 'value', 'operation');
    }

    public function stringify($quote)
    {
        $result = [];
        $arguments = [];
        foreach (array_merge($this->conditions, $this->pushed) as $condition) {
            [
                'field' => $field,
                'value' => $value,
                'operation' => $operation,
            ] = $condition;
            if (is_array($value)) {
                if ($value) {
                    $_ = implode(', ', array_fill(0, count($value), '?'));
                    $result[] = sprintf('(%s %s (' . $_ . '))', $quote . $field . $quote, $operation);
                    $arguments = array_merge($arguments, $value);
                } else {
                    $result[] = '(FALSE)';
                }
            } else {
                $result[] = sprintf('(%s %s ?)', $quote . $field . $quote, $operation);
                $arguments[] = $value;
            }
        }
        return [implode(' AND ', $result), $arguments];
    }

    /**
     */
    public function reset()
    {
        $this->pushed = [];
    }
}