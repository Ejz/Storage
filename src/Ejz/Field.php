<?php

namespace Ejz;

class Field
{
    public function __construct(string $name, ?AbstractType $type = null, ?string $alias = null)
    {
        $this->name = $name;
        $this->type = $type ?? Type::string();
        $this->alias = $alias ?? $name;
        // $this->getterStringHandlers = [];
        // $this->getterValueHandlers = [];
        // $this->setterStringHandlers = [];
        // $this->setterValueHandlers = [];
    }

    public function hasValue()
    {
        return $this->hasValue ?? false;
    }

    public function getValue()
    {
        return $this->value ?? null;
    }

    public function setValue($value)
    {
        $this->value = $this->type->cast($value);
        $this->hasValue = true;
    }

    public function unsetValue()
    {
        $this->value = null;
        $this->hasValue = false;
    }

    public function getAlias()
    {
        return $this->alias;
    }



    public function getType()
    {
        return $this->type;
    }

    public function setterString()
    {
        $setterString = $this->type->setterString();
        $setterStrings = $this->setterStrings ?? [];
        $onion = array_reduce($setterStrings, function ($next, $current) use ($quote) {
            return str_replace('?', $next, $current);
        }, $setterString);
        return $onion;
    }

    public function setterHandler()
    {
        $setterHandler = $this->type->setterHandler();
        $setterHandlers = $this->setterHandlers ?? [];
        $onion = array_reduce($setterHandlers, function ($next, $current) use ($quote) {
            return function () use ($next, $current, $quote) {
                return $current($next, $quote);
            };
        }, $setterHandler);
        return $onion();
    }

    public function registerGetterString($string)
    {

    }

    public function registerSetterString($string)
    {
        
    }

    public function registerGetterHandler($handler)
    {
        
    }

    public function registerSetterHandler($handler)
    {
        
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
     * @param string $quote
     *
     * @return string
     */
    public function getSelectString(string $quote): string
    {
        $handler = $this->type->getSelectStringHandler();
        $middlewares = $this->getSelectStringMiddlewares();
        $onion = array_reduce($middlewares, function ($string, $current) {
            // return $current($request, $handler);
        }, $handler);
        $f1 = str_replace($onion(), '%s', $quote . $this->name . $quote);
        $f2 = $quote . $this->alias . $quote;
        return $f1 . ' AS ' . $f2;
    }

    /**
     * @return array
     */
    private function getSelectStringMiddlewares(): array
    {
        return [];
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function getSelectValue($value)
    {
        $handler = $this->type->getSelectValueHandler();
        $middlewares = $this->getSelectValueMiddlewares();
        $onion = array_reduce($middlewares, function ($string, $current) {
            // return $current($request, $handler);
        }, $handler);
        return $onion($value);
    }

    /**
     * @return array
     */
    private function getSelectValueMiddlewares(): array
    {
        return [];
    }
}
