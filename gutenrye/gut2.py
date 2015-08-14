from pyspark import SparkContext
from porter2 import stem
import urllib2
import re

sc = SparkContext()

#text_urls = ['https://www.gutenberg.org/cache/epub/77/pg77.txt', 'http://www.gutenberg.org/cache/epub/2701/pg2701.txt']
text_urls = ['https://dl.dropboxusercontent.com/u/105876471/tth.txt', 'https://dl.dropboxusercontent.com/u/105876471/coc.txt']
text1_url = text_urls[0]
text2_url = text_urls[1]

# Load from web:
def wgetAndTokenize(url):
  response = urllib2.urlopen(url)
  data = response.read()
  return re.split('\s+', data);

text1_tokens = wgetAndTokenize(text1_url)
text2_tokens = wgetAndTokenize(text2_url)

# make RDD with list of words along with their position in the original text (so we can find context later)
text1_tokensRDD = sc.parallelize(text1_tokens).zipWithIndex()
text2_tokensRDD = sc.parallelize(text2_tokens).zipWithIndex()
#print text1_tokensRDD.take(5)

# define a list of stop words (chosen fairly arbitrarily)
stop_words = ['a', 'i', 'an', 'as', 'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 'afterwards', 'again', 'against', 'aint', 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', 'arent', 'around', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief', 'but', 'by', 'cmon', 'cs', 'came', 'can', 'cant', 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'co', 'com', 'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', 'couldnt', 'course', 'currently', 'definitely', 'described', 'despite', 'did', 'didnt', 'different', 'do', 'does', 'doesnt', 'doing', 'dont', 'done', 'down', 'downwards', 'during', 'each', 'edu', 'eg', 'eight', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 'et', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'far', 'few', 'fifth', 'first', 'five', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'four', 'from', 'further', 'furthermore', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings', 'had', 'hadnt', 'happens', 'hardly', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hes', 'hello', 'help', 'hence', 'her', 'here', 'heres', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'id', 'ill', 'im', 'ive', 'ie', 'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated', 'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', 'isnt', 'it', 'itd', 'itll', 'its', 'its', 'itself', 'just', 'keep', 'keeps', 'kept', 'know', 'known', 'knows', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets', 'like', 'liked', 'likely', 'little', 'look', 'looking', 'looks', 'ltd', 'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 'might', 'more', 'moreover', 'most', 'mostly', 'much', 'must', 'my', 'myself', 'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'no', 'nobody', 'non', 'none', 'noone', 'nor', 'normally', 'not', 'nothing', 'novel', 'now', 'nowhere', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'particular', 'particularly', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably', 'probably', 'provides', 'que', 'quite', 'qv', 'rather', 'rd', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'relatively', 'respectively', 'right', 'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'she', 'should', 'shouldnt', 'since', 'six', 'so', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup', 'sure', 'ts', 'take', 'taken', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', 'thats', 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'theres', 'thereafter', 'thereby', 'therefore', 'therein', 'theres', 'thereupon', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'think', 'third', 'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'twice', 'two', 'un', 'under', 'unfortunately', 'unless', 'unlikely', 'until', 'unto', 'up', 'upon', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 'value', 'various', 'very', 'via', 'viz', 'vs', 'want', 'wants', 'was', 'wasnt', 'way', 'we', 'wed', 'well', 'were', 'weve', 'welcome', 'well', 'went', 'were', 'werent', 'what', 'whats', 'whatever', 'when', 'whence', 'whenever', 'where', 'wheres', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whos', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'willing', 'wish', 'with', 'within', 'without', 'wont', 'wonder', 'would', 'wouldnt', 'yes', 'yet', 'you', 'youd', 'youll', 'youre', 'youve', 'your', 'yours', 'yourself', 'yourselves', 'zero']
 
# get rid of sequences of non-word chars, keep remaining strings with something in them, and not in stop list:
text1_tokensRDD = text1_tokensRDD.map(lambda p:(re.sub('\W+', '', p[0]).lower(), p[1])).filter(lambda p:len(p[0])>0 and not p[0] in stop_words) 
#print text1_tokensRDD.take(5)
text2_tokensRDD = text2_tokensRDD.map(lambda p:(re.sub('\W+', '', p[0]).lower(), p[1])).filter(lambda p:len(p[0])>0 and not p[0] in stop_words) 

# stem the words using imported stem function (chosen arbitrarily)
text1_stemmedRDD = text1_tokensRDD.map(lambda p:(stem(p[0]), p[1])) 
#print text1_stemmedRDD.take(5)
text2_stemmedRDD = text2_tokensRDD.map(lambda p:(stem(p[0]), p[1])) 

# for each word, get the list of loci:
text1_concRDD = text1_stemmedRDD.groupByKey()
#print text1_concRDD.take(5)
text2_concRDD = text2_stemmedRDD.groupByKey()

# find every pair of words (brute force)
text1_bigrams = text1_concRDD.cartesian(text1_concRDD)
#print text1_bigrams.first()
text2_bigrams = text2_concRDD.cartesian(text2_concRDD)

# eliminate transposed pairs, and dupes -- keep ("a","b"); not ("b", "a") or ("a", "a") etc
text1_bigrams = text1_bigrams.filter(lambda p:p[0][0] < p[1][0])
#print text1_bigrams.first()
text2_bigrams = text2_bigrams.filter(lambda p:p[0][0] < p[1][0])

# toss all pairs which never occurs within "distance" of each other:
distance = 7
def findBigramsWithin(pair):
  p,q = pair
  return ((p[0],q[0]), [(loc1, loc2) for loc1 in p[1] for loc2 in q[1] if abs(loc1-loc2)<distance])

text1_bigram_loci = text1_bigrams.map(findBigramsWithin).filter(lambda p:len(p[1])>0)
#print text1_bigram_loci.take(10)
text2_bigram_loci = text2_bigrams.map(findBigramsWithin).filter(lambda p:len(p[1])>0)

# "match" bigram+loci from text1 with same bigram (and other loci) from text2 (keeping only those that occur in both)
joined = text1_bigram_loci.join(text2_bigram_loci)

# make it run and print a report
for bigram in joined.collect():
  print "\n"+str(bigram[0])
  print "\ttext 1 loci"
  for locus in bigram[1][0]:
    lo,hi = min(locus[0],locus[1]),max(locus[0],locus[1])
    print "\t\t" + str(locus) + ": " + " ".join(text1_tokens[lo-4:hi+4])
  print "\ttext 2 loci"
  for locus in bigram[1][1]:
    lo,hi = min(locus[0],locus[1]),max(locus[0],locus[1])
    print "\t\t" + str(locus) + ": " + " ".join(text2_tokens[lo-4:hi+4])
