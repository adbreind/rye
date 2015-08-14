from pyspark import SparkContext
from pyspark.sql import HiveContext
from porter2 import stem
import urllib2
import re

sc = SparkContext()
sqlContext = HiveContext(sc)

stop_words = ['a', 'i', 'an', 'as', 'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 'afterwards', 'again', 'against', 'aint', 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', 'arent', 'around', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief', 'but', 'by', 'cmon', 'cs', 'came', 'can', 'cant', 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'co', 'com', 'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', 'couldnt', 'course', 'currently', 'definitely', 'described', 'despite', 'did', 'didnt', 'different', 'do', 'does', 'doesnt', 'doing', 'dont', 'done', 'down', 'downwards', 'during', 'each', 'edu', 'eg', 'eight', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 'et', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'far', 'few', 'fifth', 'first', 'five', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'four', 'from', 'further', 'furthermore', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings', 'had', 'hadnt', 'happens', 'hardly', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hes', 'hello', 'help', 'hence', 'her', 'here', 'heres', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'id', 'ill', 'im', 'ive', 'ie', 'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated', 'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', 'isnt', 'it', 'itd', 'itll', 'its', 'its', 'itself', 'just', 'keep', 'keeps', 'kept', 'know', 'known', 'knows', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets', 'like', 'liked', 'likely', 'little', 'look', 'looking', 'looks', 'ltd', 'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 'might', 'more', 'moreover', 'most', 'mostly', 'much', 'must', 'my', 'myself', 'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'no', 'nobody', 'non', 'none', 'noone', 'nor', 'normally', 'not', 'nothing', 'novel', 'now', 'nowhere', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'particular', 'particularly', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably', 'probably', 'provides', 'que', 'quite', 'qv', 'rather', 'rd', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'relatively', 'respectively', 'right', 'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'she', 'should', 'shouldnt', 'since', 'six', 'so', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup', 'sure', 'ts', 'take', 'taken', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', 'thats', 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'theres', 'thereafter', 'thereby', 'therefore', 'therein', 'theres', 'thereupon', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'think', 'third', 'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'twice', 'two', 'un', 'under', 'unfortunately', 'unless', 'unlikely', 'until', 'unto', 'up', 'upon', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 'value', 'various', 'very', 'via', 'viz', 'vs', 'want', 'wants', 'was', 'wasnt', 'way', 'we', 'wed', 'well', 'were', 'weve', 'welcome', 'well', 'went', 'were', 'werent', 'what', 'whats', 'whatever', 'when', 'whence', 'whenever', 'where', 'wheres', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whos', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'willing', 'wish', 'with', 'within', 'without', 'wont', 'wonder', 'would', 'wouldnt', 'yes', 'yet', 'you', 'youd', 'youll', 'youre', 'youve', 'your', 'yours', 'yourself', 'yourselves', 'zero']

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

# get rid of sequences of non-word chars, keep remaining strings with something in them, and not in stop list:
text1_tokensRDD = text1_tokensRDD.map(lambda p:(re.sub('\W+', '', p[0]).lower(), p[1])).filter(lambda p:len(p[0])>0 and not p[0] in stop_words) 
print text1_tokensRDD.take(5)
text2_tokensRDD = text2_tokensRDD.map(lambda p:(re.sub('\W+', '', p[0]).lower(), p[1])).filter(lambda p:len(p[0])>0 and not p[0] in stop_words) 

# stem the words using imported stem function (chosen arbitrarily)
text1_stemmedRDD = text1_tokensRDD.map(lambda p:(stem(p[0]), p[1])) 
print text1_stemmedRDD.take(5)
text2_stemmedRDD = text2_tokensRDD.map(lambda p:(stem(p[0]), p[1])) 

t1raw = text1_stemmedRDD.toDF(['entry', 'locus'])
t1raw.show()

t2raw = text2_stemmedRDD.toDF(['entry', 'locus'])

t1raw.registerTempTable("t1raw")
t2raw.registerTempTable("t2raw")

bg1 = sqlContext.sql("select a.entry a1, b.entry b1, a.locus, b.locus from t1raw a cross join t1raw b where a.entry < b.entry and a.locus - b.locus < 7 and b.locus - a.locus < 7")
bg1.show(4)

bg2 = sqlContext.sql("select a.entry a2, b.entry b2, a.locus, b.locus from t2raw a cross join t2raw b where a.entry < b.entry and a.locus - b.locus < 7 and b.locus - a.locus < 7")

bg2.show(4)

bg = bg1.join(bg2, ((bg1.a1 == bg2.a2) & (bg1.b1 == bg2.b2)))
bg.show(100)

