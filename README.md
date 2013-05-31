pig-sentiment-analysis
======================

Download the latest SentiWordNet data from http://sentiwordnet.isti.cnr.it/

To build the backend data, run as:
pig -p sentiment=SentiWordNet.txt -p input=corpus/ -p output=out/ sentiment.pig

The web site can be served out of www, but the data set needs to have the headers added before it can be supplied.
