-- Assumes data format is <docname>\t<doctext>
RAW = LOAD '$data' USING PigStorage('|') AS (fname:chararray, text:chararray);
DOCUMENTS = FOREACH RAW GENERATE fname, TOKENIZE(REPLACE(LOWER(text), '[^a-zA-Z]+', ' ')) as word;
WORDS_IN_DOC = FOREACH DOCUMENTS {
	d_word = DISTINCT word;
	GENERATE d_word as words;
}

-- Flatten everything so we have <document>, <word> pairs
ONLY_WORDS = FOREACH WORDS_IN_DOC GENERATE flatten(words) as word;

-- Re-group so we can COUNT
WORDS = GROUP ONLY_WORDS BY word;
COUNTS = FOREACH WORDS GENERATE group, COUNT(ONLY_WORDS) as count;

-- Determine the actual length of the document in case we want to adjust TF for document length
DOC_LEN_RAW = FOREACH DOCUMENTS GENERATE fname, flatten(word);
RAW_GROUP = GROUP RAW ALL;
RAW_COUNT = FOREACH RAW_GROUP GENERATE COUNT(RAW) as docs;
RAW_DOC_GROUP = GROUP DOC_LEN_RAW BY fname;
DOC_LENGTHS = FOREACH RAW_DOC_GROUP GENERATE group as fname, COUNT(DOC_LEN_RAW) as length:long;

-- DUMP DOC_LENGTHS;


-- If a word occurs in more than 95% of documents, drop it.
INTERESTING_COUNTS = FILTER COUNTS BY count < .95*(double)RAW_COUNT.docs;
SORTED = ORDER INTERESTING_COUNTS BY count ASC;


IDF = FOREACH SORTED GENERATE group as word, LOG((double) RAW_COUNT.docs/(double)count) as idf;

DOCS_JND = JOIN DOCUMENTS by fname, DOC_LENGTHS by fname;

DOC_WORD = FOREACH DOCS_JND GENERATE DOCUMENTS::fname as fname, length, flatten(word) as word;
WC_GROUPED_BY_DOC = GROUP DOC_WORD BY (fname, length, word);
TF = FOREACH WC_GROUPED_BY_DOC GENERATE flatten(group), COUNT(DOC_WORD) as tf, (double)COUNT(DOC_WORD)/group.length as la_tf;

PRE_TFIDF = COGROUP TF by word, IDF by word;

PRE_PRE_TFIDF = FOREACH PRE_TFIDF GENERATE flatten(TF), flatten(IDF);

-- Generate both normal and length-adjusted weight.
TFIDF = FOREACH PRE_PRE_TFIDF GENERATE fname, TF::group::word as word, (double)tf*idf as weight, la_tf*idf as la_weight;

SENTIMENT_RAW = LOAD '$sentiment' using PigStorage() AS (pos:chararray, id:chararray, pos_score:double, neg_score:double, synset_term:chararray, gloss:chararray);

SENTIMENT_ALL = FOREACH SENTIMENT_RAW GENERATE flatten(TOKENIZE(synset_term)) as synset_term, pos_score, neg_score;

SENTIMENT_DATA = FOREACH SENTIMENT_ALL GENERATE REGEX_EXTRACT(synset_term, '([^#])+#([0-9]+)', 1) as synset, pos_score, neg_score;

SENTIMENT = COGROUP TFIDF BY word, SENTIMENT_DATA BY synset;

SENTIMENT_TMP = FOREACH SENTIMENT GENERATE flatten(TFIDF), flatten(SENTIMENT_DATA);

SENTIMENT_GROUPED = GROUP SENTIMENT_TMP BY (fname, word, weight, la_weight);

-- Drop the word here, because we don't actually need it any more.
SENTIMENT_AVG = FOREACH SENTIMENT_GROUPED GENERATE group.fname as fname:chararray, group.weight*AVG(SENTIMENT_TMP.pos_score) as positive:double, group.weight*AVG(SENTIMENT_TMP.neg_score) as negative:double, group.la_weight*AVG(SENTIMENT_TMP.pos_score) as la_positive:double, group.la_weight*AVG(SENTIMENT_TMP.neg_score) as la_negative:double;

DOCS_GROUPED = GROUP SENTIMENT_AVG BY fname;

--
-- Everything from here down is specific to the data set and document naming convention I used, replace it with your own logic.
--

TOTAL_SENTIMENT = FOREACH DOCS_GROUPED GENERATE group as fname, REGEX_EXTRACT_ALL(group, '([0-9]+)-([0-9]+)-([0-9]+)-(.+).txt') as details:(year:chararray, month:chararray, day:chararray, type:chararray), SUM(SENTIMENT_AVG.positive) as pos:double, SUM(SENTIMENT_AVG.negative) as neg:double, SUM(SENTIMENT_AVG.la_positive) as la_pos:double, SUM(SENTIMENT_AVG.la_negative) as la_neg:double;

-- STORE IDF INTO '$output/idf';
-- STORE TF INTO '$output/tf';
-- STORE TFIDF INTO '$output/tfidf';
-- STORE TOTAL_SENTIMENT INTO '$output/sentiment';

B = FOREACH TOTAL_SENTIMENT GENERATE REPLACE(REPLACE(details.type, '10[qk]', '10q/k'), '.*earnings-transcript.*', 'earnings-statement') as type, fname, details.year as year, details.month as month, details.day as day, pos as pos_y_coord, neg as neg_y_coord, la_pos, la_neg;
STORE B into '$output/sentiment-final';

