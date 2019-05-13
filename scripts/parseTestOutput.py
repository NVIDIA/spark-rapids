### Script for generating test reports for OpcodeSuite tests (sql/core/src/test/scala/org/.../sql/OpcodeSuite.scala)
### "build/mvn -q -Dtest=none -DwildcardSuites=org.apache.spark.sql.OpcodeSuite test > test_results.txt" to execute tests
### "python scripts/parseTestOutput.py test_results.txt" to generate test report

import os,sys


class Test():
	def __init__(self,testName,passing=True,fallback=False):
		self.testName = testName
		self.passing = passing
		self.fallback = fallback
	def set_passing(self,result):
		self.passing = result
	def set_fallback(self,result):
		self.fallback = result


def getStart(thefile):
	starttoken = "OpcodeSuite:"
	for line in thefile:
		if line.find(starttoken)==-1:
			continue
		else:
			return
	sys.exit("Couldn't find the start of OpcodeSuite results; tests were likely not executed due to a fatal error\n")


def getNextTest(thefile):
	endtoken = "Run completed in"
	testex_token = "EXECUTING TEST:"
	for line in thefile:
		if line.find(endtoken) != -1:
			return -1
		if line.find(testex_token)==-1:
			continue
		else:
			newTest = Test(line.split(':')[1].strip())
			return newTest
	return -1


def getTestResults(thefile,curr_test):
	testend_token="TEST: *** END ***"
	fallback_token = "UDF compilation failure:"
	hardfail_token = "*** FAILED ***"
	for line in thefile:
		if line.find(testend_token)==-1:
			pass
		else:
			return curr_test
		if line.find(fallback_token)==-1:
			pass
		else:
			curr_test.set_fallback(True)
		if line.find(hardfail_token)==-1:
			pass
		else:
			curr_test.set_passing(False)
			return curr_test


def generateAndSaveReport(testList):
	test_count=len(testList)
	test_fail=0; test_pass=0; test_fallback=0
	test_fallback_pass=0; test_fallback_fail=0
	for mytest in testList:
		if mytest.fallback==True:
			test_fallback+=1
			if mytest.passing==True:
				test_pass+=1
				test_fallback_pass+=1
			else:
				test_fail+=1
				test_fallback_pass+=1
		else:
			if mytest.passing==True:
				test_pass+=1
			else:
				test_fail+=1
	
	with open("test_report.txt",'w+') as myout:
		myout.write("*** Total number of tests: {}\n".format(test_count))
		myout.write("*** Total number of tests passing: {}\n".format(test_pass))
		myout.write("*** Total number of tests failing: {}\n".format(test_fail))
		myout.write("*** Total number of tests falling back to JVM execution: {}\n".format(test_fallback))
		myout.write("*** Total number of tests falling back AND passing: {}\n".format(test_fallback_pass))
		myout.write("*** Total number of tests falling back AND failing: {}\n".format(test_fallback_fail))
		myout.write("\n\n\n\n\n\n")
		myout.write("INDIVIDUAL TEST INFORMATION BELOW:\n\n")
		for mytest in testList:
			myout.write("TEST NAME: {}\n".format(mytest.testName))
			myout.write("TEST PASSING?  {}\n".format(mytest.passing))
			myout.write("TEST FALLING BACK?  {}\n".format(mytest.fallback))
			myout.write("\n\n")
				
def main():
	try:
		open(sys.argv[1],'r')
	except IOError:
		print("Cannot read file: {}\n".format(sys.argv[1]))

	with open(sys.argv[1],'r') as myfile:
		testList=[]
		getStart(myfile)
		while(1):
			curr_test=getNextTest(myfile)
			if curr_test==-1:
				break
			testList.append(getTestResults(myfile,curr_test))
	generateAndSaveReport(testList)


if __name__=="__main__":
	main()

