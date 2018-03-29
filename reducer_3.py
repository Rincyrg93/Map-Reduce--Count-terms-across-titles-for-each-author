#Rincy Raju George- Big Data- Reducer code to compute how many times every term occurs across titles, for each author using Python Hadoop Streaming
#!/usr/bin/env python 
import sys
import os
import re

if __name__== "__main__":
	
	current_author=None
	dictFiles={}
	count=0
	total=0
	for line in sys.stdin:
		
		author, word= line.strip().split("\t")
		w,fcount=word.split(":")
		fcount=int(fcount)
		if author== current_author:
			total+=fcount
			dictFiles[w]=total
	
		else:
			if current_author is not None:
				sys.stdout.write("{0}\t{1}\n".format(current_author,dictFiles))		
			current_author=author
			dictFiles={}
			total=fcount
			dictFiles[w]=total
						
	if current_author:
		sys.stdout.write("{0}\t{1}\n".format(current_author,dictFiles))

			
