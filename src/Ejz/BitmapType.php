<?php

namespace Ejz;

class BitmapType
{
    /**
     * @return AbstractType
     */
    public static function BOOLEAN(): AbstractType
    {
        return new class(__FUNCTION__) extends AbstractType {
            public function setValue($value)
            {
                return $value === null ? null : (bool) $value;
            }
        };
    }

    /**
     * @return AbstractType
     */
    public static function STRING(): AbstractType
    {
        return new class(__FUNCTION__) extends AbstractType {
            public function setValue($value)
            {
                return $value === null ? null : (string) $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function FULLTEXT(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                return $value === null ? null : (string) $value;
            }

            public function getCreateOptions(): array
            {
                $options = $this->options;
                $query = [];
                $args = [];
                $prefixSearch = $options['prefixSearch'] ?? null;
                if ($prefixSearch !== null) {
                    $query[] = 'PREFIXSEARCH';
                }
                $noStopWords = $options['noStopWords'] ?? null;
                if ($noStopWords !== null) {
                    $query[] = 'NOSTOPWORDS';
                }
                return [implode(' ', $query), $args];
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function INTEGER(array $options = []): AbstractType
    {
        $options['min'] = $options['min'] ?? null;
        $options['max'] = $options['max'] ?? null;
        if ($options['min'] !== null) {
            $options['min'] = (int) $options['min'];
        }
        if ($options['max'] !== null) {
            $options['max'] = (int) $options['max'];
        }
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return null;
                }
                $value = (int) $value;
                $min = $this->options['min'];
                $max = $this->options['max'];
                if ($min !== null && $value < $min) {
                    return null;
                }
                if ($max !== null && $max < $value) {
                    return null;
                }
                return $value;
            }

            public function getCreateOptions(): array
            {
                $options = $this->options;
                $query = [];
                $args = [];
                $min = $options['min'];
                if ($min !== null) {
                    $query[] = 'MIN ?';
                    $args[] = $min;
                }
                $max = $options['max'];
                if ($max !== null) {
                    $query[] = 'MAX ?';
                    $args[] = $max;
                }
                return [implode(' ', $query), $args];
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function DATE(array $options = []): AbstractType
    {
        $options['min'] = $options['min'] ?? null;
        $options['max'] = $options['max'] ?? null;
        if ($options['min'] !== null) {
            $options['min'] = date('Y-m-d', (int) strtotime($options['min']));
        }
        if ($options['max'] !== null) {
            $options['max'] = date('Y-m-d', (int) strtotime($options['max']));
        }
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return null;
                }
                $value = date('Y-m-d', (int) strtotime($value));
                $min = $this->options['min'];
                $max = $this->options['max'];
                if ($min !== null && $value < $min) {
                    return null;
                }
                if ($max !== null && $max < $value) {
                    return null;
                }
                return $value;
            }

            public function getCreateOptions(): array
            {
                $options = $this->options;
                $query = [];
                $args = [];
                $min = $options['min'];
                if ($min !== null) {
                    $query[] = 'MIN ?';
                    $args[] = $min;
                }
                $max = $options['max'];
                if ($max !== null) {
                    $query[] = 'MAX ?';
                    $args[] = $max;
                }
                return [implode(' ', $query), $args];
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function DATETIME(array $options = []): AbstractType
    {
        $options['min'] = $options['min'] ?? null;
        $options['max'] = $options['max'] ?? null;
        if ($options['min'] !== null) {
            $options['min'] = date('Y-m-d H:i:s', (int) strtotime($options['min']));
        }
        if ($options['max'] !== null) {
            $options['max'] = date('Y-m-d H:i:s', (int) strtotime($options['max']));
        }
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return null;
                }
                $value = date('Y-m-d H:i:s', (int) strtotime($value));
                $min = $this->options['min'];
                $max = $this->options['max'];
                if ($min !== null && $value < $min) {
                    return null;
                }
                if ($max !== null && $max < $value) {
                    return null;
                }
                return $value;
            }

            public function getCreateOptions(): array
            {
                $options = $this->options;
                $query = [];
                $args = [];
                $min = $options['min'];
                if ($min !== null) {
                    $query[] = 'MIN ?';
                    $args[] = $min;
                }
                $max = $options['max'];
                if ($max !== null) {
                    $query[] = 'MAX ?';
                    $args[] = $max;
                }
                return [implode(' ', $query), $args];
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function FOREIGNKEY(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value instanceof AbstractBean) {
                    return $value;
                }
                if ($value === null) {
                    return null;
                }
                $value = (int) $value;
                return $value > 0 ? $value : null;
            }

            public function exportValue($value)
            {
                return $value instanceof AbstractBean ? $value->id : $value;
            }

            public function getCreateOptions(): array
            {
                $options = $this->options;
                $query = [];
                $args = [];
                $references = $options['references'] ?? null;
                if ($references !== null) {
                    $query[] = 'REFERENCES #';
                    $args[] = $references;
                }
                return [implode(' ', $query), $args];
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function ARRAY(array $options = []): AbstractType
    {
        $options['separator'] = $options['separator'] ?? '|';
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                return $value === null ? null : (array) $value;
            }

            public function exportValue($value)
            {
                $separator = $this->options['separator'];
                return $value === null ? null : implode($separator, $value);
            }

            public function getCreateOptions(): array
            {
                $options = $this->options;
                $query = [];
                $args = [];
                $query[] = 'SEPARATOR ?';
                $args[] = $options['separator'];
                return [implode(' ', $query), $args];
            }
        };
    }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return AbstractType
     */
    public static function __callStatic(string $name, array $arguments): AbstractType
    {
        $name = strtoupper($name);
        $map = [
            'INT' => 'INTEGER',
            'BOOL' => 'BOOLEAN',
            'FK' => 'FOREIGNKEY',
        ];
        $map = $map[$name];
        return self::$map(...$arguments);
    }
}
