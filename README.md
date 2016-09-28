Text Clustering program in Scala, used for the "sentiment radar" project of Information Retrieval course. K-Means algorithm is used for clustering.

idAssign.scala transform a text file(one line per file) into a new file with key-text pairs

parser.scala use the output of idAssign.scala and remove non-alphabetic and non-digit characters in each line of text. It also performs stemming on the text.

tfidf.scala takes the output of parser.scala, extract tf-idf features and perform k-Means clustering using Apache Spark MLlib. 

sampleTweets.scala takes a random sample of the tweets
