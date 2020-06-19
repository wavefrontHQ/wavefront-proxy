// Generated from /Users/vvorontsov/GitHub/wavefront-proxy/proxy/src/main/antlr4/DSLexer.g4 by ANTLR 4.8
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DSLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		EQ=1, MinusSign=2, PlusSign=3, UID=4, Number=5, Identifier=6, Letters=7, 
		Quoted=8, Literal=9, WS=10, COMMENT=11, LINE_COMMENT=12;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"EQ", "MinusSign", "PlusSign", "UID", "BLOCK", "Number", "Identifier", 
			"Letters", "Quoted", "Literal", "Letter", "Digit", "Hex", "WS", "COMMENT", 
			"LINE_COMMENT"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'='", "'-'", "'+'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "EQ", "MinusSign", "PlusSign", "UID", "Number", "Identifier", "Letters", 
			"Quoted", "Literal", "WS", "COMMENT", "LINE_COMMENT"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public DSLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "DSLexer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\16\u00c7\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\3\2\3"+
		"\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\3\6\3\6\3\6\3\6\3\6\3\7\6\7=\n\7\r\7\16\7>\3\7\3\7\6\7C\n\7\r\7\16\7"+
		"D\5\7G\n\7\3\7\3\7\3\7\5\7L\n\7\3\7\6\7O\n\7\r\7\16\7P\5\7S\n\7\3\7\3"+
		"\7\6\7W\n\7\r\7\16\7X\3\7\3\7\3\7\5\7^\n\7\3\7\6\7a\n\7\r\7\16\7b\5\7"+
		"e\n\7\5\7g\n\7\3\b\6\bj\n\b\r\b\16\bk\3\b\3\b\3\b\7\bq\n\b\f\b\16\bt\13"+
		"\b\3\t\6\tw\n\t\r\t\16\tx\3\t\7\t|\n\t\f\t\16\t\177\13\t\3\n\3\n\3\n\3"+
		"\n\7\n\u0085\n\n\f\n\16\n\u0088\13\n\3\n\3\n\3\n\3\n\3\n\7\n\u008f\n\n"+
		"\f\n\16\n\u0092\13\n\3\n\5\n\u0095\n\n\3\13\3\13\3\13\3\13\7\13\u009b"+
		"\n\13\f\13\16\13\u009e\13\13\3\f\3\f\3\r\3\r\3\16\3\16\5\16\u00a6\n\16"+
		"\3\17\6\17\u00a9\n\17\r\17\16\17\u00aa\3\17\3\17\3\20\3\20\3\20\3\20\7"+
		"\20\u00b3\n\20\f\20\16\20\u00b6\13\20\3\20\3\20\3\20\3\20\3\20\3\21\3"+
		"\21\3\21\3\21\7\21\u00c1\n\21\f\21\16\21\u00c4\13\21\3\21\3\21\5\u0086"+
		"\u0090\u00b4\2\22\3\3\5\4\7\5\t\6\13\2\r\7\17\b\21\t\23\n\25\13\27\2\31"+
		"\2\33\2\35\f\37\r!\16\3\2\t\5\2\62;C\\c|\4\2GGgg\5\2/\60aa\u0080\u0080"+
		"\4\2C\\c|\4\2CHch\5\2\13\f\17\17\"\"\4\2\f\f\17\17\2\u00e1\2\3\3\2\2\2"+
		"\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2"+
		"\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\3"+
		"#\3\2\2\2\5%\3\2\2\2\7\'\3\2\2\2\t)\3\2\2\2\13\66\3\2\2\2\rf\3\2\2\2\17"+
		"i\3\2\2\2\21v\3\2\2\2\23\u0094\3\2\2\2\25\u0096\3\2\2\2\27\u009f\3\2\2"+
		"\2\31\u00a1\3\2\2\2\33\u00a5\3\2\2\2\35\u00a8\3\2\2\2\37\u00ae\3\2\2\2"+
		"!\u00bc\3\2\2\2#$\7?\2\2$\4\3\2\2\2%&\7/\2\2&\6\3\2\2\2\'(\7-\2\2(\b\3"+
		"\2\2\2)*\5\13\6\2*+\5\13\6\2+,\7/\2\2,-\5\13\6\2-.\7/\2\2./\5\13\6\2/"+
		"\60\7/\2\2\60\61\5\13\6\2\61\62\7/\2\2\62\63\5\13\6\2\63\64\5\13\6\2\64"+
		"\65\5\13\6\2\65\n\3\2\2\2\66\67\t\2\2\2\678\t\2\2\289\t\2\2\29:\t\2\2"+
		"\2:\f\3\2\2\2;=\5\31\r\2<;\3\2\2\2=>\3\2\2\2><\3\2\2\2>?\3\2\2\2?F\3\2"+
		"\2\2@B\7\60\2\2AC\5\31\r\2BA\3\2\2\2CD\3\2\2\2DB\3\2\2\2DE\3\2\2\2EG\3"+
		"\2\2\2F@\3\2\2\2FG\3\2\2\2GR\3\2\2\2HK\t\3\2\2IL\5\5\3\2JL\5\7\4\2KI\3"+
		"\2\2\2KJ\3\2\2\2KL\3\2\2\2LN\3\2\2\2MO\5\31\r\2NM\3\2\2\2OP\3\2\2\2PN"+
		"\3\2\2\2PQ\3\2\2\2QS\3\2\2\2RH\3\2\2\2RS\3\2\2\2Sg\3\2\2\2TV\7\60\2\2"+
		"UW\5\31\r\2VU\3\2\2\2WX\3\2\2\2XV\3\2\2\2XY\3\2\2\2Yd\3\2\2\2Z]\t\3\2"+
		"\2[^\5\5\3\2\\^\5\7\4\2][\3\2\2\2]\\\3\2\2\2]^\3\2\2\2^`\3\2\2\2_a\5\31"+
		"\r\2`_\3\2\2\2ab\3\2\2\2b`\3\2\2\2bc\3\2\2\2ce\3\2\2\2dZ\3\2\2\2de\3\2"+
		"\2\2eg\3\2\2\2f<\3\2\2\2fT\3\2\2\2g\16\3\2\2\2hj\5\27\f\2ih\3\2\2\2jk"+
		"\3\2\2\2ki\3\2\2\2kl\3\2\2\2lr\3\2\2\2mq\5\27\f\2nq\5\31\r\2oq\7a\2\2"+
		"pm\3\2\2\2pn\3\2\2\2po\3\2\2\2qt\3\2\2\2rp\3\2\2\2rs\3\2\2\2s\20\3\2\2"+
		"\2tr\3\2\2\2uw\5\27\f\2vu\3\2\2\2wx\3\2\2\2xv\3\2\2\2xy\3\2\2\2y}\3\2"+
		"\2\2z|\5\31\r\2{z\3\2\2\2|\177\3\2\2\2}{\3\2\2\2}~\3\2\2\2~\22\3\2\2\2"+
		"\177}\3\2\2\2\u0080\u0086\7$\2\2\u0081\u0082\7^\2\2\u0082\u0085\7$\2\2"+
		"\u0083\u0085\13\2\2\2\u0084\u0081\3\2\2\2\u0084\u0083\3\2\2\2\u0085\u0088"+
		"\3\2\2\2\u0086\u0087\3\2\2\2\u0086\u0084\3\2\2\2\u0087\u0089\3\2\2\2\u0088"+
		"\u0086\3\2\2\2\u0089\u0095\7$\2\2\u008a\u0090\7)\2\2\u008b\u008c\7^\2"+
		"\2\u008c\u008f\7)\2\2\u008d\u008f\13\2\2\2\u008e\u008b\3\2\2\2\u008e\u008d"+
		"\3\2\2\2\u008f\u0092\3\2\2\2\u0090\u0091\3\2\2\2\u0090\u008e\3\2\2\2\u0091"+
		"\u0093\3\2\2\2\u0092\u0090\3\2\2\2\u0093\u0095\7)\2\2\u0094\u0080\3\2"+
		"\2\2\u0094\u008a\3\2\2\2\u0095\24\3\2\2\2\u0096\u009c\5\27\f\2\u0097\u009b"+
		"\5\27\f\2\u0098\u009b\5\31\r\2\u0099\u009b\t\4\2\2\u009a\u0097\3\2\2\2"+
		"\u009a\u0098\3\2\2\2\u009a\u0099\3\2\2\2\u009b\u009e\3\2\2\2\u009c\u009a"+
		"\3\2\2\2\u009c\u009d\3\2\2\2\u009d\26\3\2\2\2\u009e\u009c\3\2\2\2\u009f"+
		"\u00a0\t\5\2\2\u00a0\30\3\2\2\2\u00a1\u00a2\4\62;\2\u00a2\32\3\2\2\2\u00a3"+
		"\u00a6\t\6\2\2\u00a4\u00a6\5\31\r\2\u00a5\u00a3\3\2\2\2\u00a5\u00a4\3"+
		"\2\2\2\u00a6\34\3\2\2\2\u00a7\u00a9\t\7\2\2\u00a8\u00a7\3\2\2\2\u00a9"+
		"\u00aa\3\2\2\2\u00aa\u00a8\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab\u00ac\3\2"+
		"\2\2\u00ac\u00ad\b\17\2\2\u00ad\36\3\2\2\2\u00ae\u00af\7\61\2\2\u00af"+
		"\u00b0\7,\2\2\u00b0\u00b4\3\2\2\2\u00b1\u00b3\13\2\2\2\u00b2\u00b1\3\2"+
		"\2\2\u00b3\u00b6\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b4\u00b2\3\2\2\2\u00b5"+
		"\u00b7\3\2\2\2\u00b6\u00b4\3\2\2\2\u00b7\u00b8\7,\2\2\u00b8\u00b9\7\61"+
		"\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bb\b\20\2\2\u00bb \3\2\2\2\u00bc\u00bd"+
		"\7\61\2\2\u00bd\u00be\7\61\2\2\u00be\u00c2\3\2\2\2\u00bf\u00c1\n\b\2\2"+
		"\u00c0\u00bf\3\2\2\2\u00c1\u00c4\3\2\2\2\u00c2\u00c0\3\2\2\2\u00c2\u00c3"+
		"\3\2\2\2\u00c3\u00c5\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c5\u00c6\b\21\2\2"+
		"\u00c6\"\3\2\2\2\36\2>DFKPRX]bdfkprx}\u0084\u0086\u008e\u0090\u0094\u009a"+
		"\u009c\u00a5\u00aa\u00b4\u00c2\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}