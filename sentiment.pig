RAW = LOAD '$data' USING PigStorage('|') AS (fname:chararray, text:chararray);
DOCUMENTS = FOREACH RAW GENERATE fname, TOKENIZE(REPLACE(LOWER(text), '[^a-zA-Z]+', ' ')) as word;
WORDS_IN_DOC = FOREACH DOCUMENTS {
	d_word = DISTINCT word;
	GENERATE d_word as words;
}
ONLY_WORDS = FOREACH WORDS_IN_DOC GENERATE flatten(words) as word;
WORDS = GROUP ONLY_WORDS BY word;
COUNTS = FOREACH WORDS GENERATE group, COUNT(ONLY_WORDS) as count;
SORTED = ORDER COUNTS BY count ASC;

RAW_GROUP = GROUP RAW ALL;
RAW_COUNT = FOREACH RAW_GROUP GENERATE COUNT(RAW) as docs;

IDF = FOREACH SORTED GENERATE group as word, LOG((double) RAW_COUNT.docs/(double)count) as idf;

DOC_WORD = FOREACH DOCUMENTS GENERATE fname, flatten(word) as word;
WC_GROUPED_BY_DOC = GROUP DOC_WORD BY (fname, word);
TF = FOREACH WC_GROUPED_BY_DOC GENERATE flatten(group), COUNT(DOC_WORD) as tf;

PRE_TFIDF = JOIN TF by word, IDF by word;

TFIDF = FOREACH PRE_TFIDF GENERATE fname, TF::group::word as word, (double)tf*idf as weight;

SENTIMENT_RAW = LOAD '$sentiment' using PigStorage() AS (pos:chararray, id:chararray, pos_score:double, neg_score:double, synset_term:chararray, gloss:chararray);

SENTIMENT_ALL = FOREACH SENTIMENT_RAW GENERATE flatten(TOKENIZE(synset_term)) as synset_term, pos_score, neg_score;

SENTIMENT_DATA = FOREACH SENTIMENT_ALL GENERATE REGEX_EXTRACT(synset_term, '([^#])+#([0-9]+)', 1) as synset, pos_score, neg_score;

SENTIMENT = JOIN TFIDF BY word, SENTIMENT_DATA BY synset;

SENTIMENT_GROUPED = GROUP SENTIMENT BY (fname, word, weight);

-- Drop the word here, because we don't actually need it any more.
SENTIMENT_AVG = FOREACH SENTIMENT_GROUPED GENERATE group.fname as fname, group.weight*AVG(SENTIMENT.pos_score) as positive, group.weight*AVG(SENTIMENT.neg_score) as negative;

DOCS_GROUPED = GROUP SENTIMENT_AVG BY fname;

TOTAL_SENTIMENT = FOREACH DOCS_GROUPED GENERATE group as fname, SUM(SENTIMENT_AVG.positive) as positive, SUM(SENTIMENT_AVG.negative) as negative;

STORE IDF INTO '$output/idf';
STORE TF INTO '$output/tf';
STORE TFIDF INTO '$output/tfidf';
STORE TOTAL_SENTIMENT INTO '$output/sentiment';
