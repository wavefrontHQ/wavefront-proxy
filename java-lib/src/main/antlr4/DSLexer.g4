lexer grammar DSLexer;

EQ
  : '='
  ;

NEQ
  : '!='
  ;

IpV4Address
  :  Octet '.' Octet '.' Octet '.' Octet
  ;

MinusSign
  : '-'
  ;

PlusSign
  : '+'
  ;


IpV6Address
  : ('::')? ((Segment ':') | (Segment '::'))+ (Segment | (Segment '::'))
  | '::'
  | '::' Segment ('::')?
  | ('::')? Segment '::'
  | ('::')? ((Segment '::')
             | Segment ':')+ IpV4Address
  | '::' IpV4Address
  ;

// negative numbers are not accounted for here since we need to
// handle for instance 5 - 6 (and not consume the minus sign into the number making it just two numbers).
Number
  : Digit+ ('.' Digit+)? (('e' | 'E') (MinusSign | PlusSign)? Digit+)?
  | '.' Digit+ (('e' | 'E') (MinusSign | PlusSign)? Digit+)?
  ;

Letters
  : Letter+ Digit*
  ;

Quoted
  : '"' ( '\\"' | . )*? '"'
  | '\'' ( '\\\'' | . )*? '\''
  ;

Literal
  : '~'? Letter (Letter
  | Digit
  | '.'
  | '-'
  | '_'
  | '|'
  | '~'
  | '{'
  | '}'
  | SLASH
  | STAR)+
  ;

// Special token that we do allow for tag values.
RelaxedLiteral
  : (Letter | Digit) (Letter
  | Digit
  | '.'
  | '-'
  | '_'
  | '|'
  | '~'
  | '{'
  | '}')+
  ;

BinType
  : '!M'
  | '!H'
  | '!D'
  ;

Weight
  : '#' Number
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

fragment
Segment
  : Hex Hex Hex Hex
  | Hex Hex Hex
  | Hex Hex
  | Hex
  ;

fragment
Octet
  : ('1'..'9') (('0'..'9') ('0'..'9')?)?
  | '0'
  ;

STAR   : '*' ;
SLASH  : '/' ;
WS     : [ \t\r\n]+ -> channel(HIDDEN) ;