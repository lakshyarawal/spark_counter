from pyspark import SparkContext, SparkConf

# define stop words
stop_words = set(['the','and','to','of','a','in','i','that','he','was','his','it','her','with','for','you','had','as','she','not','but','be','my','at','is'])

# define function to clean text
def clean_text(text):
    import re
    # remove all punctuation and convert to lowercase
    text = re.sub(r'[^\w\s]','',text).lower()
    # split text into words and remove stop words
    words = [w for w in text.split() if w not in stop_words]
    return words

# create SparkContext object
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

# read in text files as RDDs
# define a list of file paths
path = "./books/"
rdd1 = sc.textFile(path+ "pg145.txt")
rdd2 = sc.textFile(path+ "pg394.txt")
rdd3 = sc.textFile(path+ "pg1513.txt")
rdd4 = sc.textFile(path+ "pg2160.txt")
rdd5 = sc.textFile(path+ "pg2641.txt")
rdd6 = sc.textFile(path+ "pg2701.txt")
rdd7 = sc.textFile(path+ "pg4085.txt")
rdd8 = sc.textFile(path+ "pg6593.txt")
rdd9 = sc.textFile(path+ "pg37106.txt")
rdd10 = sc.textFile(path+ "pg67979.txt")

union_rdd = rdd1.union(rdd2).union(rdd3).union(rdd4).union(rdd5).union(rdd6).union(rdd7).union(rdd8).union(rdd9).union(rdd10)


# combine text files using union
combined_text_rdd = sc.union(union_rdd)

# clean text and count words
word_count_rdd = union_rdd.flatMap(lambda line: clean_text(line)).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)


# sort by count in descending order
sorted_word_count_rdd = word_count_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))

# output result to text file
sorted_word_count_rdd.saveAsTextFile("./outputs/finalOne.txt")

# stop SparkContext
sc.stop()