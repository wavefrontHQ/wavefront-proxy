grammar Condition;

import DSLexer;

@header {
  package parser.predicate;
}

program
  : evalExpression EOF
  ;

evalExpression
  : '(' evalExpression ')'
  | evalExpression op=('&'|'|'|'^'|'<<<'|'<<'|'>>>'|'>>') evalExpression
  | complement='~' evalExpression
  | evalExpression op=('*'|'/'|'%') evalExpression
  | evalExpression op=('-'|'+') evalExpression
  | evalExpression comparisonOperator evalExpression
  | evalExpression op=('or'|'and'|'OR'|'AND') evalExpression
  | not=('not'|'NOT') evalExpression
  | placeholder multiModifier=('any'|'ANY'|'all'|'ALL') stringComparisonOp stringExpression
  | stringExpression stringComparisonOp stringExpression
  | stringExpression in='in' '(' stringExpression (',' stringExpression)*')'
  | iff
  | parse
  | time
  | length
  | strHashCode
  | strIsEmpty
  | strIsNotEmpty
  | strIsBlank
  | strIsNotBlank
  | random
  | number
  | '$' propertyAccessor
  ;

iff
  : 'if' '(' evalExpression ',' evalExpression ',' evalExpression ')'
  ;

parse
  : 'parse' '(' stringExpression ',' evalExpression ')'
  ;

time
  : 'time' '(' stringExpression (',' stringExpression)? ')'
  ;

length
  : 'length' '(' stringExpression ')'
  ;

strHashCode
  : 'hashCode' '(' stringExpression ')'
  ;

strIsEmpty
  : 'isEmpty' '(' stringExpression ')'
  ;

strIsNotEmpty
  : 'isNotEmpty' '(' stringExpression ')'
  ;

strIsBlank
  : 'isBlank' '(' stringExpression ')'
  ;

strIsNotBlank
  : 'isNotBlank' '(' stringExpression ')'
  ;

random
  : 'random' '(' ')'
  ;

propertyAccessor
  : 'value'
  | 'timestamp'
  | 'duration'
  ;

stringExpression
  : '(' stringExpression ')'
  | stringExpression concat='+' stringExpression
  | stringExpression '.' stringFunc
  | strIff
  | string
  ;

stringFunc
  : substring
  | strLeft
  | strRight
  | replace
  | replaceAll
  | toLowerCase
  | toUpperCase
  ;

strIff
  : 'if' '(' evalExpression ',' stringExpression ',' stringExpression ')'
  ;

substring
  : 'substring' '(' evalExpression (',' evalExpression)? ')'
  ;

strLeft
  : 'left' '(' evalExpression ')'
  ;

strRight
  : 'right' '(' evalExpression ')'
  ;

replace
  : 'replace' '(' stringExpression ',' stringExpression ')'
  ;

replaceAll
  : 'replaceAll' '(' stringExpression ',' stringExpression ')'
  ;

toLowerCase
  : 'toLowerCase' '(' ')'
  ;

toUpperCase
  : 'toUpperCase' '(' ')'
  ;

string
  : Quoted
  | placeholder
  ;

placeholder
  : '{{' (Letters | Identifier) '}}'
  ;

stringComparisonOp
  : '='
  | 'equals'
  | 'equalsIgnoreCase'
  | 'startsWith'
  | 'startsWithIgnoreCase'
  | 'contains'
  | 'containsIgnoreCase'
  | 'endsWith'
  | 'endsWithIgnoreCase'
  | 'matches'
  | 'matchesIgnoreCase'
  | 'regexMatch'
  | 'regexMatchIgnoreCase'
  ;

comparisonOperator
  : '=' | '>' | '<' | '<' '=' | '>' '=' | '!' '='
  ;

number
  : MinusSign? Number (siSuffix)?
  | PlusSign? Number (siSuffix)?
  ;

siSuffix
  : 'Y' // 10^24
  | 'Z' // 10^21
  | 'E' // 10^18
  | 'P' // 10^15
  | 'T' // 10^12
  | 'G' // 10^9
  | 'M' // 10^6
  | 'k' // 10^3
  | 'h' // 10^2
  | 'da' // 10^1
  | 'd' // 10^-1
  | 'c' // 10^-2
  | 'm' // 10^-3
  | 'µ' // 10^-6
  | 'n' // 10^-9
  | 'p' // 10^-12
  | 'f' // 10^-15
  | 'a' // 10^-18
  | 'z' // 10^-21
  | 'y' // 10^-24
  ;
