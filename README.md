# rye

Experimental implementation with Apache Spark of a subset of functionality from http://tesserae.caset.buffalo.edu/

### Some notes

- The stemmer is public domain and grabbed from a common Python lib
- Stop words are an arbitrary list from the Internet 

### Logical ToDos

- Get a better, multilingual stemmer
- Better stop word list
- Add filtering of "stop bigrams"
	+ Real Tesserae has a scoring engine
	+ Bigrams common to a genre/language should probable get scored 0 and filtered out for performance

### Plans

- Reimplement parts of the code using the Spark DataFrames/SQL API
	+ Hoping to squeeze a little free performance out of the Catalyst optimizer
	+ Not using a Python lambda in the "big filter" (i.e., the distance filter after the Cartesian join) might help perf
