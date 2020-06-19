lexer grammar DSLexer;

EQ
  : '='
  ;

MinusSign
  : '-'
  ;

PlusSign
  : '+'
  ;

UID
 : BLOCK BLOCK '-' BLOCK '-' BLOCK '-' BLOCK '-' BLOCK BLOCK BLOCK
 ;

fragment BLOCK
 : [A-Za-z0-9] [A-Za-z0-9] [A-Za-z0-9] [A-Za-z0-9]
 ;

// negative numbers are not accounted for here since we need to
// handle for instance 5 - 6 (and not consume the minus sign into the number making it just two numbers).
Number
  : Digit+ ('.' Digit+)? (('e' | 'E') (MinusSign | PlusSign)? Digit+)?
  | '.' Digit+ (('e' | 'E') (MinusSign | PlusSign)? Digit+)?
  ;

Identifier
  : Letter+ (Letter | Digit | '_')*
  ;

Letters
  : Letter+ Digit*
  ;

Quoted
  : '"' ( '\\"' | . )*? '"'
  | '\'' ( '\\\'' | . )*? '\''
  ;

Literal
  : Letter (Letter
  | Digit
  | '.'
  | '-'
  | '_'
  | '~')*
  ;

fragment
Letter
  : 'a'..'z'
  | 'A'..'Z'
  ;

fragment
Digit
  : '0'..'9'
  ;

fragment
Hex
  : 'a'..'f'
  | 'A'..'F'
  | Digit
  ;

WS     : [ \t\r\n]+                  -> channel(HIDDEN) ;
COMMENT:            '/*' .*? '*/'    -> channel(HIDDEN);
LINE_COMMENT:       '//' ~[\r\n]*    -> channel(HIDDEN);