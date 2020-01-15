<?php

namespace Ejz;

class Field
{
    /** @var string */
    private $name;

    /** @var AbstractType */
    private $type;

    /** @var string */
    private $alias;

    /** @var bool */
    private $hasValue;

    /** @var mixed */
    private $value;

    /**
     * @param string        $name
     * @param ?AbstractType $type  (optional)
     * @param ?string       $alias (optional)
     */
    public function __construct(string $name, ?AbstractType $type = null, ?string $alias = null)
    {
        $this->name = $name;
        $this->type = $type ?? Type::default(true);
        $this->alias = $alias ?? $this->name;
        $this->unsetValue();
    }

    /**
     * @param string $quote
     *
     * @return string
     */
    public function getSelectString(string $quote): string
    {
        $selectString = $this->type->getSelectString();
        $f1 = str_replace($selectString, '%s', $quote . $this->name . $quote);
        $f2 = $quote . $this->alias . $quote;
        return $f1 . ' AS ' . $f2;
    }

    /**
     * @param mixed $value
     */
    public function importValue($value)
    {
        $value = $this->type->import($value);
        $this->setValue($value);
    }

    /**
     * @return bool
     */
    public function hasValue(): bool
    {
        return $this->hasValue;
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @return string
     */
    public function getAlias(): string
    {
        return $this->alias;
    }

    /**
     */
    public function unsetValue()
    {
        $this->value = null;
        $this->hasValue = false;
    }

    /**
     * @param mixed $value
     */
    public function setValue($value)
    {
        $value = $this->type->set($value);
        $this->value = $value;
        $this->hasValue = true;
    }

    /**
     * @return AbstractType
     */
    public function getType(): AbstractType
    {
        return $this->type;
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

    // $f1 = str_replace('%s', '%s', $quote . $this->name . $quote);
    // $f2 = $quote . $this->alias . $quote;
    // return $f1 . ' AS ' . $f2;
    // return '%s';
    // $handler = $this->type->getSelectStringHandler();
    // $middlewares = $this->getSelectStringMiddlewares();
    // $onion = array_reduce($middlewares, function ($string, $current) {
        // return $current($request, $handler);
    // }, $handler);

    // /**
    //  * @return array
    //  */
    // private function getSelectStringMiddlewares(): array
    // {
    //     return [];
    // }

    // /**
    //  * @param string $quote
    //  *
    //  * @return string
    //  */
    // public function getInsertString(string $quote): string
    // {
    //     $handler = $this->type->getInsertStringHandler();
    //     $middlewares = $this->getInsertStringMiddlewares();
    //     $onion = array_reduce($middlewares, function ($string, $current) {
    //         // return $current($request, $handler);
    //     }, $handler);
    //     return $onion();
    // }

    // /**
    //  * @return array
    //  */
    // private function getInsertStringMiddlewares(): array
    // {
    //     return [];
    // }

    // /**
    //  * @param string $quote
    //  *
    //  * @return string
    //  */
    // public function getUpdateString(string $quote): string
    // {
    //     $handler = $this->type->getUpdateStringHandler();
    //     $middlewares = $this->getUpdateStringMiddlewares();
    //     $onion = array_reduce($middlewares, function ($string, $current) {
    //         // return $current($request, $handler);
    //     }, $handler);
    //     return str_replace($onion(), '%s', $quote . $this->name . $quote);
    // }

    // /**
    //  * @return array
    //  */
    // private function getUpdateStringMiddlewares(): array
    // {
    //     return [];
    // }




    // public function setterString()
    // {
    //     $setterString = $this->type->setterString();
    //     $setterStrings = $this->setterStrings ?? [];
    //     $onion = array_reduce($setterStrings, function ($next, $current) use ($quote) {
    //         return str_replace('?', $next, $current);
    //     }, $setterString);
    //     return $onion;
    // }

    // public function setterHandler()
    // {
    //     $setterHandler = $this->type->setterHandler();
    //     $setterHandlers = $this->setterHandlers ?? [];
    //     $onion = array_reduce($setterHandlers, function ($next, $current) use ($quote) {
    //         return function () use ($next, $current, $quote) {
    //             return $current($next, $quote);
    //         };
    //     }, $setterHandler);
    //     return $onion();
    // }

    // public function registerGetterString($string)
    // {

    // }

    // public function registerSetterString($string)
    // {
        
    // }

    // public function registerGetterHandler($handler)
    // {
        
    // }

    // public function registerSetterHandler($handler)
    // {
        
    // }

    

    

    // /**
    //  * @param mixed $value
    //  *
    //  * @return mixed
    //  */
    // public function getSelectValue($value)
    // {
    //     $handler = $this->type->getSelectValueHandler();
    //     $middlewares = $this->getSelectValueMiddlewares();
    //     $onion = array_reduce($middlewares, function ($string, $current) {
    //         // return $current($request, $handler);
    //     }, $handler);
    //     return $onion($value);
    // }

    // /**
    //  * @return array
    //  */
    // private function getSelectValueMiddlewares(): array
    // {
    //     return [];
    // }// if ()
        // $this->value = $this->type === null ? $value : 
        // $this->hasValue = true;

    
}
