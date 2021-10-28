grammar Graphflow;

oC_Cypher : SP? oC_Statement ( SP? ';' )? SP? EOF ;

oC_Statement : oC_Query ;

oC_Query : oC_RegularQuery
         | gF_bplusTreeNodeIndexQuery ;

gF_bplusTreeNodeIndexQuery : gF_bPlusTreeNodeIndexPattern (SP oC_Where)?;

gF_bPlusTreeNodeIndexPattern : CREATE SP INDEX SP gF_indexName SP oC_NodePattern
                           SP ON SP oC_PropertyOrLabelsExpression;

oC_RegularQuery : oC_SingleQuery  ( SP? oC_Union )* ;

oC_Union : UNION SP? oC_SingleQuery
         | UNION SP ALL SP? oC_SingleQuery ;

oC_SingleQuery : oC_SinglePartQuery
               | oC_MultiPartQuery
               ;

oC_SinglePartQuery : ( ( oC_ReadingClause SP? )? oC_Return );

oC_MultiPartQuery : ( ( oC_ReadingClause SP? )? oC_With SP? )+ oC_SinglePartQuery ;

oC_ReadingClause : oC_Match ;

oC_With : WITH SP oC_ReturnBody ( SP? oC_Where )?;

oC_Return : RETURN SP oC_ReturnBody ;

oC_ReturnBody : oC_ReturnItems ( SP oC_Order)? ( SP oC_Skip )? ( SP oC_Limit )? ;

oC_ReturnItems : STAR
               | ( oC_ReturnItem ( SP? ',' SP? oC_ReturnItem )* )
               ;

oC_ReturnItem : oC_Expression
              | ( oC_Expression SP AS SP gF_Variable )
              ;

oC_Skip : L_SKIP SP oC_IntegerLiteral ;

oC_Limit : LIMIT SP oC_IntegerLiteral ;

oC_Order : ORDER SP BY SP oC_SortItem ( SP? COMMA SP? oC_SortItem )* ;

oC_SortItem : oC_Expression ( SP? ( ASCENDING | ASC | DESCENDING | DESC ) )? ;

oC_Match : MATCH SP oC_Pattern (SP oC_Where)? ;

oC_Pattern : oC_RelationshipPattern ( SP? COMMA SP? oC_RelationshipPattern )* ;

oC_Where : WHERE SP oC_Expression ;

oC_Expression : oC_OrExpression ;

oC_OrExpression : oC_AndExpression
                | oC_OrExpression SP OR SP oC_OrExpression ;

oC_AndExpression : oC_NotExpression
                | oC_AndExpression SP AND SP oC_AndExpression ;

oC_NotExpression : ( NOT SP? )? oC_ComparisonExpression ;

oC_ComparisonExpression : oC_AddOrSubtractExpression ( SP? gF_Comparison SP? oC_AddOrSubtractExpression )? ;

oC_AddOrSubtractExpression : oC_MultiplyDivideModuloExpression
                           | oC_AddOrSubtractExpression SP? PLUS SP? oC_AddOrSubtractExpression
                           | oC_AddOrSubtractExpression SP? oC_Dash SP? oC_AddOrSubtractExpression ;

oC_MultiplyDivideModuloExpression : oC_PowerOfExpression
                                  | oC_MultiplyDivideModuloExpression  SP? STAR SP? oC_MultiplyDivideModuloExpression
                                  | oC_MultiplyDivideModuloExpression  SP? FORWARD_SLASH SP? oC_MultiplyDivideModuloExpression
                                  | oC_MultiplyDivideModuloExpression  SP? MODULO SP? oC_MultiplyDivideModuloExpression ;

oC_PowerOfExpression : gF_UnaryNegationExpression ( SP? EXPONENT SP? gF_UnaryNegationExpression )? ;

gF_UnaryNegationExpression : (oC_Dash SP?)? oC_PropertyOrLabelsExpression  ;

gF_Comparison : EQUAL_TO      | NOT_EQUAL_TO          |
                LESS_THAN     | LESS_THAN_OR_EQUAL    |
                GREATER_THAN  | GREATER_THAN_OR_EQUAL |
                STARTS_WITH   | ENDS_WITH             |
                CONTAINS ;

oC_PropertyOrLabelsExpression : oC_Atom ( SP? oC_PropertyLookup )? ;

oC_Atom : oC_Literal
        | COUNT SP? '(' SP? '*' SP? ')'
        | oC_ParenthesizedExpression
        | oC_FunctionInvocation
        | gF_Variable
        ;

oC_PropertyLookup : '.' SP? ( gF_Variable ) ;

oC_RelationshipPattern : oC_NodePattern SP? oC_Dash oC_RelationshipDetail oC_RightArrowHead
                         oC_NodePattern ;
oC_NodePattern : OPEN_ROUND_BRACKET SP? gF_Variable (oC_NodeType)? SP? CLOSE_ROUND_BRACKET ;

oC_NodeType : SP? COLON SP? gF_Variable ;

oC_RelationshipDetail : OPEN_SQUARE_BRACKET gF_Variable? COLON oC_RelationshipLabel
                        CLOSE_SQUARE_BRACKET oC_Dash;

oC_RelationshipLabel : gF_Variable ;

gF_Variable : ( Characters | UNDERSCORE ) ( Digits | Characters | UNDERSCORE )*;

groupByClause : GROUP SP BY SP oC_PropertyOrLabelsExpression ;

sortByClause : SORT SP BY SP oC_PropertyOrLabelsExpression ;

gF_indexName : gF_Variable ;

oC_ParenthesizedExpression :  '(' SP? oC_Expression SP? ')' ;

oC_FunctionInvocation : oC_FunctionName SP? '(' SP? oC_Expression SP?  ')' ;

oC_FunctionName : MIN
                | MAX
                | SUM
                ;


oC_Literal : oC_NumberLiteral
            | oC_BooleanLiteral
            | StringLiteral
            ;

oC_NumberLiteral : ( oC_IntegerLiteral | oC_DoubleLiteral ) ;

oC_IntegerLiteral : Digits ;

oC_DoubleLiteral : Digits DOT Digits ;

oC_BooleanLiteral : TRUE
                    | FALSE
                    ;
TRUE : T R U E ;

FALSE : F A L S E ;

StringLiteral : QuotedString ;

gf_keyword : MATCH
             | CREATE
             | ADJ
             | LIST
             | WHERE
             | TRUE
             | FALSE
             | GROUP
             | SORT
             | BY
             | INDEX
             | ON
             | RETURN
             | WITH
             | STARTS
             | ENDS
             | CONTAINS
             | AND
             | OR
             | L_SKIP
             | LIMIT
             | ORDER
             | ASCENDING
             | ASC
             | DESCENDING
             | DESC
             ;

SP : ( WHITESPACE )+ ;

WHITESPACE : SPACE
              | TAB
              | LF
              | VT
              | FF
              | CR
              | FS
              | GS
              | RS
              | US
              | ' '
              | '᠎'
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | ' '
              | '　'
              | ' '
              | ' '
              | ' '
              | Comment
              ;

/*********** Lexer rules ***********/

fragment EscapedChar : TAB | CR | LF | BACKSPACE | FF | '\\' ( '"' | '\'' | '\\' ) ;
QuotedString : DOUBLE_QUOTE ( EscapedChar | ~( '"' ) )* DOUBLE_QUOTE
             | SINGLE_QUOTE ( EscapedChar | ~( '\'' ) )* SINGLE_QUOTE ;

Comment : '/*' .*? '*/'
        | '//' ~( '\n' | '\r' )*  '\r'? ( '\n' | EOF ) ;

MATCH : M A T C H ;
CREATE : C R E A T E ;
ADJ : A D J ;
LIST : L I S T ;
WHERE : W H E R E ;
GROUP : G R O U P ;
SORT : S O R T ;
BY : B Y ;
INDEX : I N D E X ;
ON : O N ;
RETURN : R E T U R N ;
WITH : W I T H ;
COUNT : C O U N T ;
MIN : M I N ;
MAX : M A X ;
SUM : S U M ;
STARTS : S T A R T S ;
ENDS : E N D S ;
CONTAINS : C O N T A I N S ;
OR : O R ;
AND : A N D ;
NOT : N O T ;
AS : A S ;
L_SKIP : S K I P ;
LIMIT : L I M I T ;
ORDER : O R D E R ;
ASCENDING : A S C E N D I N G ;
ASC : A S C ;
DESCENDING : D E S C E N D I N G ;
DESC : D E S C ;
UNION : U N I O N ;
ALL : A L L ;


fragment SPACE : [ ] ;
fragment TAB : [\t] ;
fragment CR : [\r] ;
fragment LF : [\n] ;
fragment FF : [\f] ;
fragment BACKSPACE : [\b] ;
fragment VT : [\u000B] ;
fragment FS : [\u001C] ;
fragment GS : [\u001D] ;
fragment RS : [\u001E] ;
fragment US : [\u001F] ;

oC_RightArrowHead : '>' ;

STAR : '*' ;
PLUS : '+' ;
MODULO : '%' ;
EXPONENT : '^' ;
oC_Dash : '-' ;
UNDERSCORE : '_' ;
FORWARD_SLASH : '/' ;
BACKWARD_SLASH : '\\' ;
SEMICOLON: ';' ;
COMMA: ',' ;
COLON : ':' ;
DOT : '.' ;
SINGLE_QUOTE: '\'' ;
DOUBLE_QUOTE: '"' ;
OPEN_ROUND_BRACKET : '(' ;
CLOSE_ROUND_BRACKET : ')' ;
OPEN_SQUARE_BRACKET : '[' ;
CLOSE_SQUARE_BRACKET : ']' ;
EQUAL_TO : '=' ;
NOT_EQUAL_TO : '<>' ;
LESS_THAN : '<' ;
LESS_THAN_OR_EQUAL : '<=' ;
GREATER_THAN : '>' ;
GREATER_THAN_OR_EQUAL : '>=' ;
STARTS_WITH : STARTS SP WITH ;
ENDS_WITH : ENDS SP WITH ;


fragment A : ('a'|'A') ;
fragment B : ('b'|'B') ;
fragment C : ('c'|'C') ;
fragment D : ('d'|'D') ;
fragment E : ('e'|'E') ;
fragment F : ('f'|'F') ;
fragment G : ('g'|'G') ;
fragment H : ('h'|'H') ;
fragment I : ('i'|'I') ;
fragment J : ('j'|'J') ;
fragment K : ('k'|'K') ;
fragment L : ('l'|'L') ;
fragment M : ('m'|'M') ;
fragment N : ('n'|'N') ;
fragment O : ('o'|'O') ;
fragment P : ('p'|'P') ;
fragment Q : ('q'|'Q') ;
fragment R : ('r'|'R') ;
fragment S : ('s'|'S') ;
fragment T : ('t'|'T') ;
fragment U : ('u'|'U') ;
fragment V : ('v'|'V') ;
fragment W : ('w'|'W') ;
fragment X : ('x'|'X') ;
fragment Y : ('y'|'Y') ;
fragment Z : ('z'|'Z') ;

fragment Character : A | B | C | D | E | F | G | H | I | J | K | L | M |
                     N | O | P | Q | R | S | T | U | V | W | X | Y | Z ;
Characters : ( Character )+ ;

fragment Digit : '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' ;
Digits : ( Digit )+ ;
